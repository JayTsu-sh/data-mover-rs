use std::error::Error;
use std::path::Path;

use super::*;

type TestResult = Result<(), Box<dyn Error>>;

fn text(derived: Derived) -> Result<String, EndpointError> {
    derived.map(|identity| identity.stable_id().to_owned())
}

#[test]
fn nfs_options_versions_and_the_default_port_do_not_change_the_endpoint() -> TestResult {
    let expected = "nfs://10.128.61.200/ontap_lisaauto_nfs";
    for url in [
        "nfs://10.128.61.200/ontap_lisaauto_nfs?version=4.1&noresvport=true&uid=0&gid=0",
        "nfs://10.128.61.200:2049/ontap_lisaauto_nfs?version=3",
        "nfs://10.128.61.200:0/ontap_lisaauto_nfs/?uid=1000&gid=1000&mountport=635",
        "nfs://10.128.61.200/ontap_lisaauto_nfs?nfsport=2049",
        "nfs://10.128.61.200/ontap_lisaauto_nfs:/",
    ] {
        assert_eq!(text(nfs(url))?, expected, "{url}");
    }
    Ok(())
}

#[test]
fn nfs_export_colon_prefix_equals_the_joined_path_and_is_normalized() -> TestResult {
    let expected = "nfs://h/vol/a/b";
    for url in [
        "nfs://h/vol:/a/b",
        "nfs://h/vol:a/b/",
        "nfs://h/vol/a/b",
        "nfs://H/vol//a/./b",
        "nfs://h/vol/a/x/../b?version=4.1",
    ] {
        assert_eq!(text(nfs(url))?, expected, "{url}");
    }
    Ok(())
}

#[test]
fn nfs_ports_hosts_and_ipv6() -> TestResult {
    assert_eq!(text(nfs("nfs://h:2050/vol"))?, "nfs://h:2050/vol");
    // `nfsport=` wins over the authority port, as in nfs-rs.
    assert_eq!(
        text(nfs("nfs://h:2050/vol?nfsport=3049"))?,
        "nfs://h:3049/vol"
    );
    assert_eq!(
        text(nfs("nfs://[FE80::1]:2049/vol"))?,
        "nfs://[fe80::1]/vol"
    );
    assert_eq!(
        text(nfs("nfs://[fe80::1]:111/vol"))?,
        "nfs://[fe80::1]:111/vol"
    );
    // No resolution: an IP and a name for the same server stay different.
    assert_ne!(
        text(nfs("nfs://10.0.0.1/vol"))?,
        text(nfs("nfs://nas01/vol"))?
    );
    assert_eq!(text(nfs("nfs://h"))?, "nfs://h");
    Ok(())
}

#[test]
fn nfs_prefix_may_not_climb_out_of_the_export_but_the_export_resolves_like_nfs_rs() -> TestResult {
    // The backend mounts `/vol` and looks the prefix up inside it: `..` there is an error, never
    // the export `/x`.
    for url in [
        "nfs://h/vol:../x",
        "nfs://h/vol:..",
        "nfs://h/vol:a/../../x",
    ] {
        assert!(nfs(url).is_err(), "{url}");
    }
    // The export is URL text that nfs-rs resolves itself (`url` clamps `..` at the root).
    assert_eq!(text(nfs("nfs://h/a/../x"))?, "nfs://h/x");
    assert_eq!(text(nfs("nfs://h/%2e%2e/x"))?, "nfs://h/x");
    Ok(())
}

#[test]
fn nfs_names_are_what_the_server_sees() -> TestResult {
    // nfs-rs sends the export as URL text, so `a b` reaches the server as `a%20b`, while the
    // prefix is looked up literally: two different directories, two different endpoints.
    assert_eq!(text(nfs("nfs://h/vol/a b"))?, "nfs://h/vol/a%2520b");
    assert_eq!(text(nfs("nfs://h/vol:a b"))?, "nfs://h/vol/a%20b");
    // `#` starts a fragment in the export and is dropped by nfs-rs; in the prefix it is a name.
    assert_eq!(text(nfs("nfs://h/vol#x"))?, "nfs://h/vol");
    assert_eq!(text(nfs("nfs://h/vol:a#b"))?, "nfs://h/vol/a%23b");
    Ok(())
}

#[test]
fn nfs_port_edge_cases_follow_nfs_rs() -> TestResult {
    // The first `nfsport=` wins and its value is percent-decoded.
    assert_eq!(
        text(nfs("nfs://h/v?nfsport=3049&nfsport=4049"))?,
        "nfs://h:3049/v"
    );
    assert_eq!(text(nfs("nfs://h/v?nfsport=%33049"))?, "nfs://h:3049/v");
    // `nfsport=0` means the portmapper, whatever the authority says.
    assert_eq!(text(nfs("nfs://h:3049/v?nfsport=0"))?, "nfs://h/v");
    // With `#` in the export nfs-rs never sees the query, so `nfsport=` does not apply.
    assert_eq!(
        text(nfs("nfs://h:3049/vol#x?nfsport=4049"))?,
        "nfs://h:3049/vol"
    );
    // An empty port is no port, as in the `url` crate.
    assert_eq!(text(nfs("nfs://H:/v"))?, "nfs://h/v");
    Ok(())
}

#[test]
fn nfs_rejects_bad_ports_and_other_schemes() {
    for url in [
        "nfs://h:port/vol",
        "nfs://h/vol?nfsport=x",
        "smb://h/vol",
        "nfs:///vol",
        "nfs://user@h/vol",
    ] {
        assert!(nfs(url).is_err(), "{url}");
    }
}

#[test]
fn smb_lowercases_server_and_share_but_keeps_root_case() -> TestResult {
    let expected = "smb://nas01/data/Projects/Q3";
    for (server, share, root) in [
        ("NAS01", "Data", Some("Projects/Q3")),
        ("nas01:445", "data", Some("\\Projects\\Q3\\")),
        ("nas01", "/DATA/", Some("/Projects//./Q3")),
    ] {
        assert_eq!(
            text(smb(server, share, root))?,
            expected,
            "{server} {share}"
        );
    }
    for root in [None, Some(""), Some("/"), Some("\\")] {
        assert_eq!(text(smb("nas01", "data", root))?, "smb://nas01/data");
    }
    assert_eq!(
        text(smb("nas01:1445", "data", None))?,
        "smb://nas01:1445/data"
    );
    Ok(())
}

#[test]
fn ipv6_hosts_are_canonical_and_never_split_at_a_colon() -> TestResult {
    // smb-rs reads an unbracketed IPv6 server as an address with no port.
    assert_eq!(
        text(smb("2001:DB8::1", "s", None))?,
        "smb://[2001:db8::1]/s"
    );
    assert_ne!(
        text(smb("2001:db8::1:445", "s", None))?,
        text(smb("2001:db8::1", "s", None))?
    );
    assert_eq!(text(smb("fe80::abcd", "s", None))?, "smb://[fe80::abcd]/s");
    assert_eq!(text(smb("[fe80::1]:445", "s", None))?, "smb://[fe80::1]/s");
    assert_eq!(text(nfs("nfs://[0:0::1]/v"))?, text(nfs("nfs://[::1]/v"))?);
    for bad in ["[::1]junk", "[::1", "[zz::1]", "zz::1"] {
        assert!(smb(bad, "s", None).is_err(), "{bad}");
    }
    Ok(())
}

#[test]
fn smb_encodes_raw_segments_and_rejects_bad_input() -> TestResult {
    assert_eq!(
        text(smb("h", "s", Some("a b/100%/中文?#")))?,
        "smb://h/s/a%20b/100%25/%E4%B8%AD%E6%96%87%3F%23"
    );
    assert!(smb("h", "", None).is_err());
    assert!(smb("h", "a/b", None).is_err());
    assert!(smb("h", "s", Some("../x")).is_err());
    assert!(smb("user@h", "s", None).is_err());
    Ok(())
}

#[test]
fn s3_scheme_default_ports_and_profile_do_not_change_the_endpoint() -> TestResult {
    let expected = "s3://10.131.9.11:9000/data-mover-test/resume-base/resume-1";
    for endpoint in ["http://10.131.9.11:9000", "https://10.131.9.11:9000/"] {
        assert_eq!(
            text(s3(
                endpoint,
                "Data-Mover-Test",
                Some("resume-base/resume-1/")
            ))?,
            expected,
            "{endpoint}"
        );
    }
    for endpoint in [
        "http://s3.example.com",
        "http://s3.example.com:80",
        "https://S3.Example.com:443",
        "https://s3.example.com:80",
    ] {
        assert_eq!(
            text(s3(endpoint, "b", None))?,
            "s3://s3.example.com/b",
            "{endpoint}"
        );
    }
    Ok(())
}

#[test]
fn s3_prefix_is_literal_except_for_the_one_trailing_slash() -> TestResult {
    assert_eq!(text(s3("http://h", "b", Some("a/b/")))?, "s3://h/b/a/b");
    // `parse_s3_url` can produce each of these; they are distinct key spaces.
    let spaces = ["a/", "a//", "/a/", "a//b/", "a/b/"];
    for (index, first) in spaces.iter().enumerate() {
        for second in &spaces[index + 1..] {
            assert_ne!(
                text(s3("http://h", "b", Some(first)))?,
                text(s3("http://h", "b", Some(second)))?,
                "{first} vs {second}"
            );
        }
    }
    assert_eq!(text(s3("http://h", "b", Some("")))?, "s3://h/b");
    // `…:9000//` stores the prefix `/`: keys under `/…`, not the bucket root.
    assert_eq!(text(s3("http://h", "b", Some("/")))?, "s3://h/b/");
    assert_ne!(
        text(s3("http://h", "b", Some("/")))?,
        text(s3("http://h", "b", None))?
    );
    Ok(())
}

#[test]
fn s3_rejects_bad_endpoints_and_buckets() {
    assert!(s3("s3://h", "b", None).is_err());
    assert!(s3("http://AK:SK@h", "b", None).is_err());
    assert!(s3("http://h", "", None).is_err());
    assert!(s3("http://h", "a/b", None).is_err());
    assert!(s3("http://h/x", "b", None).is_err());
}

#[test]
fn hdfs_keeps_the_port_and_distinguishes_nameservices() -> TestResult {
    assert_eq!(
        text(hdfs("hdfs://NameNode:8020", "/data//x/"))?,
        "hdfs://namenode:8020/data/x"
    );
    assert_eq!(
        text(hdfs("hdfs://nameservice1", "/"))?,
        "hdfs://nameservice1"
    );
    assert_ne!(
        text(hdfs("hdfs://nn:8020", "/"))?,
        text(hdfs("hdfs://nn", "/"))?
    );
    assert_eq!(
        text(hdfs("hdfs://[::1]:9000", "/a b"))?,
        "hdfs://[::1]:9000/a%20b"
    );
    assert!(hdfs("nn:8020", "/").is_err());
    // A NameService is a case-sensitive Hadoop configuration key; a NameNode host is DNS.
    assert_eq!(text(hdfs("hdfs://NS1", "/"))?, "hdfs://NS1");
    assert_ne!(
        text(hdfs("hdfs://NS1", "/"))?,
        text(hdfs("hdfs://ns1", "/"))?
    );
    assert!(hdfs("hdfs://nn:8020/../x", "/").is_err());
    assert!(hdfs("hdfs://ns1/x", "/").is_err());
    assert!(smb("fe80::1%2", "s", None).is_err());
    Ok(())
}

#[test]
fn local_is_a_host_free_file_url_of_the_canonical_path() -> TestResult {
    let root = tempfile::tempdir()?;
    let canonical = std::fs::canonicalize(root.path())?;
    let derived = text(local(&canonical))?;
    assert!(derived.starts_with("file:///"), "{derived}");
    assert!(!derived.ends_with('/'), "{derived}");
    assert!(local(Path::new("relative/root")).is_err());
    assert!(local(Path::new("/a/../b")).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn local_encodes_raw_names() -> TestResult {
    assert_eq!(
        text(local(Path::new("/data/a b/100%/中")))?,
        "file:///data/a%20b/100%25/%E4%B8%AD"
    );
    assert_eq!(text(local(Path::new("/")))?, "file:///");
    Ok(())
}

#[cfg(unix)]
#[test]
fn local_keeps_distinct_non_utf8_names_distinct() -> TestResult {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;
    let first = Path::new("/d").join(OsStr::from_bytes(b"\xff"));
    let second = Path::new("/d").join(OsStr::from_bytes(b"\xfe"));
    assert_eq!(text(local(&first))?, "file:///d/%FF");
    assert_ne!(text(local(&first))?, text(local(&second))?);
    Ok(())
}

#[cfg(windows)]
#[test]
fn local_writes_verbatim_windows_prefixes_like_their_plain_forms() -> TestResult {
    assert_eq!(text(local(Path::new(r"\\?\c:\Data")))?, "file:///C:/Data");
    assert_eq!(text(local(Path::new(r"C:\Data")))?, "file:///C:/Data");
    assert_eq!(
        text(local(Path::new(r"\\?\UNC\Server\Share\x")))?,
        text(local(Path::new(r"\\server\share\x")))?
    );
    Ok(())
}

#[test]
fn every_scheme_sets_the_backend_kind() -> TestResult {
    assert_eq!(nfs("nfs://h/v")?.kind(), BackendKind::Nfs);
    assert_eq!(smb("h", "s", None)?.kind(), BackendKind::Cifs);
    assert_eq!(s3("http://h", "b", None)?.kind(), BackendKind::S3);
    assert_eq!(hdfs("hdfs://h:1", "/")?.kind(), BackendKind::Hdfs);
    #[cfg(unix)]
    assert_eq!(local(Path::new("/x"))?.kind(), BackendKind::Local);
    Ok(())
}
