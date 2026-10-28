use std::str::FromStr as _;

use smb_domain::protocol::{
    ACE, ACL, AccessAce, AccessMask, AceFlags, AceValue, AclRevision, SID, SecurityDescriptor,
    SecurityDescriptorControl,
};

use super::{explicit_dacl, merge_dacl};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(super) fn ace(sid: &str, inherited: bool) -> Result<ACE> {
    Ok(ACE {
        ace_flags: AceFlags::new().with_inherited(inherited),
        value: AceValue::AccessAllowed(AccessAce {
            access_mask: AccessMask::new().with_generic_read(true),
            sid: SID::from_str(sid)?,
        }),
    })
}

pub(super) fn descriptor(aces: Vec<ACE>, protected: bool) -> SecurityDescriptor {
    SecurityDescriptor {
        sbz1: 0,
        control: SecurityDescriptorControl::new()
            .with_self_relative(true)
            .with_dacl_present(true)
            .with_dacl_protected(protected),
        owner_sid: None,
        group_sid: None,
        sacl: None,
        dacl: Some(ACL {
            acl_revision: AclRevision::Nt4,
            ace: aces,
        }),
    }
}

fn sids(descriptor: &SecurityDescriptor) -> Vec<(String, bool)> {
    descriptor
        .dacl
        .iter()
        .flat_map(|dacl| dacl.ace.iter())
        .map(|ace| {
            let sid = match &ace.value {
                AceValue::AccessAllowed(value) => value.sid.to_string(),
                other => format!("{other:?}"),
            };
            (sid, ace.ace_flags.inherited())
        })
        .collect()
}

#[test]
fn observation_keeps_only_explicit_aces_and_the_protection_bit() -> Result {
    let observed = explicit_dacl(descriptor(
        vec![ace("S-1-1-0", true)?, ace("S-1-5-32-545", false)?],
        true,
    ));
    assert_eq!(sids(&observed), vec![("S-1-5-32-545".to_owned(), false)]);
    assert!(observed.control.dacl_protected());
    assert!(observed.control.dacl_present());
    Ok(())
}

#[test]
fn unchanged_target_costs_no_set_info() -> Result {
    let source = descriptor(Vec::new(), false);
    let target = descriptor(vec![ace("S-1-1-0", true)?], false);
    assert!(merge_dacl(&source, target).is_none());
    Ok(())
}

#[test]
fn merge_puts_source_explicit_aces_before_target_inherited_ones() -> Result {
    let source = descriptor(vec![ace("S-1-5-32-545", false)?], false);
    let target = descriptor(
        vec![ace("S-1-5-32-544", false)?, ace("S-1-1-0", true)?],
        false,
    );
    let merged = merge_dacl(&source, target).ok_or("merge expected")?;
    assert_eq!(
        sids(&merged),
        vec![
            ("S-1-5-32-545".to_owned(), false),
            ("S-1-1-0".to_owned(), true),
        ]
    );
    assert!(!merged.control.dacl_protected());
    assert!(merged.control.dacl_present());
    Ok(())
}

#[test]
fn protected_source_stops_inheritance_on_the_target() -> Result {
    let source = descriptor(vec![ace("S-1-5-32-545", false)?], true);
    let target = descriptor(
        vec![ace("S-1-5-32-544", false)?, ace("S-1-1-0", true)?],
        false,
    );
    let merged = merge_dacl(&source, target).ok_or("merge expected")?;
    assert_eq!(sids(&merged), vec![("S-1-5-32-545".to_owned(), false)]);
    assert!(merged.control.dacl_protected());
    Ok(())
}

#[test]
fn null_dacl_source_applies_nothing() -> Result {
    let mut source = descriptor(Vec::new(), false);
    source.dacl = None;
    source.control = source.control.with_dacl_present(false);
    let target = descriptor(vec![ace("S-1-5-32-544", false)?], true);
    assert!(merge_dacl(&source, target).is_none());
    Ok(())
}

#[test]
fn stale_explicit_target_aces_are_cleared_when_source_has_none() -> Result {
    let source = descriptor(Vec::new(), false);
    let target = descriptor(
        vec![ace("S-1-5-32-544", false)?, ace("S-1-1-0", true)?],
        false,
    );
    let merged = merge_dacl(&source, target).ok_or("merge expected")?;
    assert_eq!(sids(&merged), vec![("S-1-1-0".to_owned(), true)]);
    Ok(())
}

#[test]
fn protection_bit_change_alone_triggers_an_update() -> Result {
    let source = descriptor(Vec::new(), true);
    let target = descriptor(vec![ace("S-1-1-0", true)?], false);
    let merged = merge_dacl(&source, target).ok_or("merge expected")?;
    assert!(merged.control.dacl_protected());
    assert!(merged.control.dacl_present());
    assert!(
        sids(&merged).is_empty(),
        "protected source without ACEs is deny-all"
    );
    Ok(())
}

#[test]
fn protected_empty_fallback_clears_inherited_and_null_target_dacls() -> Result {
    use crate::model::acl::safe_deny_all_acl;
    use crate::model::{AclEncoding, StoragePath};
    use binrw::{BinRead as _, BinWrite as _};
    use std::io::Cursor;

    let fallback = safe_deny_all_acl(AclEncoding::WindowsSecurityDescriptor)?;
    let source = super::decode_acl(&StoragePath::new("file")?, &fallback)?;
    let mut null_target = descriptor(Vec::new(), true);
    null_target.dacl = None;
    null_target.control = null_target.control.with_dacl_present(false);
    for target in [descriptor(vec![ace("S-1-1-0", true)?], true), null_target] {
        let merged = merge_dacl(&source, target).ok_or("deny-all requires SET_INFO")?;
        assert_empty_protected_dacl(&merged);
        let mut encoded = Cursor::new(Vec::new());
        merged.write_le(&mut encoded)?;
        let actual = SecurityDescriptor::read_le(&mut Cursor::new(encoded.into_inner()))?;
        assert_empty_protected_dacl(&actual);
    }
    Ok(())
}

fn assert_empty_protected_dacl(descriptor: &SecurityDescriptor) {
    assert!(descriptor.control.dacl_present());
    assert!(descriptor.control.dacl_protected());
    assert!(
        descriptor
            .dacl
            .as_ref()
            .is_some_and(|dacl| dacl.ace.is_empty()),
        "empty deny-all DACL must remain present, not NULL"
    );
}
