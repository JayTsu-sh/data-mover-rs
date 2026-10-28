//! Native Windows ACL operations. Storage roles use capability-opened handles; the public
//! path helpers are an explicit ambient-path seam for inspection and inheritance control.

use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::os::windows::fs::OpenOptionsExt as _;
use std::os::windows::io::AsRawHandle as _;
use std::path::Path;
use std::ptr;

use windows_sys::Wdk::System::SystemServices::RtlValidRelativeSecurityDescriptor;
use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    GetSecurityInfo, SE_FILE_OBJECT, SetSecurityInfo,
};
use windows_sys::Win32::Security::{
    ACE_HEADER, ACL, ACL_REVISION_DS, AddAce, DACL_SECURITY_INFORMATION,
    GROUP_SECURITY_INFORMATION, GetAce, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
    GetSecurityDescriptorLength, INHERITED_ACE, InitializeAcl, OWNER_SECURITY_INFORMATION,
    PROTECTED_DACL_SECURITY_INFORMATION, PSID, SE_DACL_PROTECTED,
    UNPROTECTED_DACL_SECURITY_INFORMATION,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_BACKUP_SEMANTICS, READ_CONTROL, WRITE_DAC,
};

mod inspection;
pub use inspection::{
    AceInfo, AceType, Permission, PermissionEntry, PermissionMapping, SecurityInfo,
    get_security_info,
};

/// Reads a self-relative security descriptor, including owner, group and discretionary ACL.
/// Owner/group are descriptive: applying these bytes changes only the discretionary ACL.
pub fn get_acl_bytes(path: &Path) -> io::Result<Vec<u8>> {
    read_handle(&open_path(path, false)?)
}

/// Applies explicit source ACEs and its inheritance protection to a file or directory.
/// Inherited ACEs come from the destination tree. A missing/NULL source DACL is a no-op;
/// an empty DACL remains a real empty ACL, never a NULL (everyone-allowed) ACL.
pub fn set_acl_bytes(path: &Path, bytes: &[u8]) -> io::Result<()> {
    apply_handle(&open_path(path, true)?, bytes)
}

/// Copies explicit permissions and inheritance protection, retaining destination ownership.
pub fn copy_acl(source: &Path, target: &Path) -> io::Result<()> {
    set_acl_bytes(target, &get_acl_bytes(source)?)
}

/// Enables or disables inheritance without discarding the object's existing ACL.
pub fn set_inheritance_protect(path: &Path, protected: bool) -> io::Result<()> {
    let file = open_path(path, true)?;
    let descriptor = Descriptor::read(&file)?;
    let (_, _, dacl) = descriptor.components();
    set_dacl(&file, dacl, protected)
}

pub(super) fn open_path(path: &Path, writable: bool) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .access_mode(READ_CONTROL | if writable { WRITE_DAC } else { 0 })
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

/// Opens metadata access relative to the same directory capability as the storage role.
pub(super) fn open_at(root: &cap_std::fs::Dir, path: &Path, writable: bool) -> io::Result<File> {
    use cap_std::fs::OpenOptionsExt as _;
    let mut options = cap_std::fs::OpenOptions::new();
    options.access_mode(READ_CONTROL | if writable { WRITE_DAC } else { 0 });
    options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    root.open_with(path, &options)
        .map(cap_std::fs::File::into_std)
}

pub(super) fn read_handle(file: &File) -> io::Result<Vec<u8>> {
    Descriptor::read(file).map(|descriptor| descriptor.bytes())
}

/// The copied ACL contract carries source explicit ACEs; inherited ACEs belong to the
/// destination tree. Keep the ambient inspection API's complete descriptor separate.
pub(super) fn read_copy_handle(file: &File) -> io::Result<Vec<u8>> {
    explicit_descriptor(read_handle(file)?)
}

fn explicit_descriptor(mut bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    let source = ValidatedDescriptor::new(&bytes)?;
    let (dacl, _) = source.dacl()?;
    if dacl.is_null() {
        return Ok(bytes);
    }
    let explicit = explicit_acl(dacl)?;
    let offset =
        u32::from_le_bytes(bytes[16..20].try_into().map_err(|_| invalid_descriptor())?) as usize;
    let size = u16::from_le_bytes(
        bytes
            .get(offset + 2..offset + 4)
            .ok_or_else(invalid_descriptor)?
            .try_into()
            .map_err(|_| invalid_descriptor())?,
    ) as usize;
    let target = bytes
        .get_mut(offset..offset + size)
        .ok_or_else(invalid_descriptor)?;
    for (target, byte) in target
        .iter_mut()
        .zip(explicit.iter().flat_map(|word| word.to_ne_bytes()))
    {
        *target = byte;
    }
    Ok(bytes)
}

pub(super) fn apply_handle(file: &File, bytes: &[u8]) -> io::Result<()> {
    let source = ValidatedDescriptor::new(bytes)?;
    let (dacl, protected) = source.dacl()?;
    if dacl.is_null() {
        return Ok(());
    }
    // Passing only explicit ACEs with UNPROTECTED lets Windows regenerate inherited ACEs
    // from the target's parent. A protected ACL must contain only the explicit source ACEs.
    let mut explicit = explicit_acl(dacl)?;
    set_dacl(file, explicit.as_mut_ptr().cast(), protected)
}

struct Descriptor {
    ptr: *mut c_void,
    owner: PSID,
    group: PSID,
    dacl: *mut ACL,
}

impl Descriptor {
    fn read(file: &File) -> io::Result<Self> {
        let mut value = Self {
            ptr: ptr::null_mut(),
            owner: ptr::null_mut(),
            group: ptr::null_mut(),
            dacl: ptr::null_mut(),
        };
        // SAFETY: outputs live through the call; successful API output owns a LocalFree allocation.
        #[expect(
            unsafe_code,
            reason = "GetSecurityInfo returns an owned native descriptor"
        )]
        let status = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION,
                &mut value.owner,
                &mut value.group,
                &mut value.dacl,
                ptr::null_mut(),
                &mut value.ptr,
            )
        };
        check_status(status)?;
        if value.ptr.is_null() {
            return Err(io::Error::other(
                "Windows returned a missing security descriptor",
            ));
        }
        Ok(value)
    }
    fn components(&self) -> (PSID, PSID, *mut ACL) {
        (self.owner, self.group, self.dacl)
    }
    fn protected(&self) -> io::Result<bool> {
        descriptor_protected(self.ptr)
    }
    fn bytes(&self) -> Vec<u8> {
        // SAFETY: GetSecurityInfo provided a valid, live self-relative descriptor.
        #[expect(
            unsafe_code,
            reason = "copying the valid native descriptor while its owner lives"
        )]
        unsafe {
            std::slice::from_raw_parts(
                self.ptr.cast::<u8>(),
                GetSecurityDescriptorLength(self.ptr) as usize,
            )
            .to_vec()
        }
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        #[expect(
            unsafe_code,
            reason = "matching GetSecurityInfo allocation with LocalFree"
        )]
        unsafe {
            LocalFree(self.ptr);
        }
    }
}

/// Native descriptor APIs require aligned memory and cannot themselves infer a Rust slice's
/// bounds. Validate its complete self-relative framing with the length-aware NT API first.
struct ValidatedDescriptor(Vec<usize>);
impl ValidatedDescriptor {
    fn new(bytes: &[u8]) -> io::Result<Self> {
        let length = u32::try_from(bytes.len()).map_err(|_| invalid_descriptor())?;
        if bytes.len() < 20 {
            return Err(invalid_descriptor());
        }
        let mut words = vec![0usize; bytes.len().div_ceil(std::mem::size_of::<usize>())];
        #[expect(
            unsafe_code,
            reason = "aligned allocation has enough bytes; bounded native validator precedes all accessors"
        )]
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), words.as_mut_ptr().cast::<u8>(), bytes.len());
            if !RtlValidRelativeSecurityDescriptor(words.as_mut_ptr().cast(), length, 0) {
                return Err(invalid_descriptor());
            }
        }
        Ok(Self(words))
    }
    fn dacl(&self) -> io::Result<(*mut ACL, bool)> {
        let descriptor = self.0.as_ptr().cast_mut().cast();
        let (mut present, mut defaulted, mut dacl) = (0, 0, ptr::null_mut());
        #[expect(
            unsafe_code,
            reason = "descriptor is length-validated and remains alive"
        )]
        if unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok((
            if present != 0 { dacl } else { ptr::null_mut() },
            descriptor_protected(descriptor)?,
        ))
    }
}

fn invalid_descriptor() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid self-relative Windows security descriptor",
    )
}

fn descriptor_protected(descriptor: *mut c_void) -> io::Result<bool> {
    let (mut control, mut revision) = (0, 0);
    #[expect(
        unsafe_code,
        reason = "internal callers supply owned or length-validated descriptors"
    )]
    if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(control & SE_DACL_PROTECTED != 0)
}

fn set_dacl(file: &File, dacl: *const ACL, protected: bool) -> io::Result<()> {
    let flags = DACL_SECURITY_INFORMATION
        | if protected {
            PROTECTED_DACL_SECURITY_INFORMATION
        } else {
            UNPROTECTED_DACL_SECURITY_INFORMATION
        };
    #[expect(
        unsafe_code,
        reason = "live file handle and validated ACL remain alive during the native call"
    )]
    let status = unsafe {
        SetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            flags,
            ptr::null_mut(),
            ptr::null_mut(),
            dacl,
            ptr::null(),
        )
    };
    check_status(status)
}

fn check_status(status: u32) -> io::Result<()> {
    if status == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
}

fn explicit_acl(dacl: *mut ACL) -> io::Result<Vec<u32>> {
    // SAFETY: only called with an ACL inside a length-validated descriptor. The output capacity
    // uses the source's actual ACL size, sufficient for every retained ACE plus the ACL header.
    #[expect(
        unsafe_code,
        reason = "Win32 ACL calls operate within validated, owned buffers"
    )]
    unsafe {
        let size = u32::from((*dacl).AclSize);
        let mut words = vec![0u32; (size as usize).div_ceil(4)];
        let target = words.as_mut_ptr().cast::<ACL>();
        if InitializeAcl(target, size, ACL_REVISION_DS) == 0 {
            return Err(io::Error::last_os_error());
        }
        for index in 0..u32::from((*dacl).AceCount) {
            let mut ace = ptr::null_mut();
            if GetAce(dacl, index, &mut ace) == 0 {
                return Err(io::Error::last_os_error());
            }
            let header = &*ace.cast::<ACE_HEADER>();
            if header.AceFlags & INHERITED_ACE as u8 == 0
                && AddAce(
                    target,
                    ACL_REVISION_DS,
                    u32::MAX,
                    ace,
                    u32::from(header.AceSize),
                ) == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(words)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_truncated_and_out_of_bounds_descriptors() {
        for bytes in [vec![], vec![0; 19], vec![0; 20], {
            let mut bytes = vec![0; 20];
            bytes[0] = 1;
            bytes[2..4].copy_from_slice(&0x8004u16.to_le_bytes());
            bytes[16..20].copy_from_slice(&4096u32.to_le_bytes());
            bytes
        }] {
            assert_eq!(
                ValidatedDescriptor::new(&bytes)
                    .err()
                    .map(|error| error.kind()),
                Some(io::ErrorKind::InvalidInput)
            );
        }
    }

    #[test]
    fn inheritance_toggle_preserves_existing_access_entries() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("file");
        std::fs::write(&path, b"value")?;
        let before = get_security_info(&path)?;
        set_inheritance_protect(&path, true)?;
        let protected = get_security_info(&path)?;
        assert!(!protected.inheritance_enabled);
        assert_eq!(protected.aces.len(), before.aces.len());
        set_inheritance_protect(&path, false)?;
        assert!(get_security_info(&path)?.inheritance_enabled);
        assert_eq!(std::fs::read(path)?, b"value");
        Ok(())
    }

    #[test]
    fn an_empty_acl_stays_non_null() -> io::Result<()> {
        let acl = crate::model::acl::safe_deny_all_acl(
            crate::model::AclEncoding::WindowsSecurityDescriptor,
        )
        .map_err(io::Error::other)?;
        let bytes = acl.access().ok_or_else(invalid_descriptor)?;
        let descriptor = ValidatedDescriptor::new(bytes)?;
        let (dacl, protected) = descriptor.dacl()?;
        assert!(protected);
        assert!(!dacl.is_null());
        let result = explicit_acl(dacl)?;
        assert!(!result.is_empty());
        Ok(())
    }

    #[test]
    fn conservative_acl_is_applied_as_a_protected_empty_dacl() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("file");
        std::fs::write(&path, b"content")?;
        let file = open_path(&path, true)?;
        let original = read_handle(&file)?;
        let acl = crate::model::acl::safe_deny_all_acl(
            crate::model::AclEncoding::WindowsSecurityDescriptor,
        )
        .map_err(io::Error::other)?;
        apply_handle(&file, acl.access().ok_or_else(invalid_descriptor)?)?;
        let observed = Descriptor::read(&file);
        // Restore before asserting, so temp-directory cleanup retains its original access.
        let restored = apply_handle(&file, &original);
        let observed = observed?;
        restored?;
        let (_, _, dacl) = observed.components();
        assert!(!dacl.is_null());
        assert!(observed.protected()?);
        // SAFETY: GetSecurityInfo returned a live descriptor containing the ACL pointer.
        #[expect(
            unsafe_code,
            reason = "reading the ACL header in a live native descriptor"
        )]
        let count = unsafe { (*dacl).AceCount };
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    fn copied_descriptor_omits_inherited_aces_but_inspection_bytes_are_complete() -> io::Result<()>
    {
        // One inherited allow ACE for Everyone (S-1-1-0), no explicit ACEs.
        let mut full = vec![0; 48];
        full[0] = 1;
        full[2..4].copy_from_slice(&0x8004u16.to_le_bytes());
        full[16..20].copy_from_slice(&20u32.to_le_bytes());
        full[20] = 2;
        full[22..24].copy_from_slice(&28u16.to_le_bytes());
        full[24..26].copy_from_slice(&1u16.to_le_bytes());
        full[29] = INHERITED_ACE as u8;
        full[30..32].copy_from_slice(&20u16.to_le_bytes());
        full[32..36].copy_from_slice(&READ_CONTROL.to_le_bytes());
        full[36] = 1; // SID revision
        full[37] = 1; // one subauthority
        full[43] = 1; // World SID authority
        ValidatedDescriptor::new(&full)?;
        let copied = explicit_descriptor(full.clone())?;
        ValidatedDescriptor::new(&copied)?;
        assert_eq!(u16::from_le_bytes([full[24], full[25]]), 1);
        assert_eq!(u16::from_le_bytes([copied[24], copied[25]]), 0);
        assert_eq!(copied[2..4], full[2..4]);
        assert_ne!(&copied[16..20], &[0; 4]); // empty ACL, not NULL
        Ok(())
    }
}
