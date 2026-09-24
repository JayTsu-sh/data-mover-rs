//! Canonical endpoint identity (ADR-0006 "Three identities", item 1).
//!
//! Each backend's [`BackendIdentity`] is derived here from what the backend actually connects to:
//! protocol, address, share or bucket, and prefix — never credentials, protocol versions, TLS, or
//! compatibility profiles. Two configurations that reach the same data derive the same string;
//! the string feeds `EntryIdentityKey`, snapshots and the recovery binding, so two *different*
//! data sets must never derive the same one.
//!
//! Hosts: DNS names are lowercased, IPv6 addresses are written in their canonical bracketed form
//! (`[fe80::1]`), and nothing is resolved — an IP address and a hostname for the same server, or a
//! short name and its FQDN, are different endpoints. Paths name what the server sees, one segment
//! at a time, percent-encoded (controls, space, `%`, `?`, `#`, non-ASCII) so the string is
//! unambiguous. Path rules differ per scheme and are on each function: NFS, SMB, HDFS and Local
//! resolve `.`/`..` and drop empty segments; S3 keys are not paths and are taken literally.
//!
//! The functions are pure: callers pass the parts their own parsers produced. The NFS URL is the
//! exception — its splitting rules are simple and repeated here, because `storage` may not depend
//! on the legacy `nfs` module.

use std::borrow::Cow;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::net::Ipv6Addr;
use std::path::{Component, Path, Prefix};

use percent_encoding::{AsciiSet, CONTROLS, percent_encode};
use url::{Url, form_urlencoded};

use crate::model::{BackendIdentity, BackendKind};

/// Bytes escaped in every path segment.
const SEGMENT: &AsciiSet = &CONTROLS.add(b' ').add(b'%').add(b'?').add(b'#');

const NFS_DEFAULT_PORT: u16 = 2049;
const SMB_DEFAULT_PORT: u16 = 445;

/// Why an endpoint could not be derived. The reason is fixed text: it never echoes the input,
/// which may carry credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EndpointError(&'static str);

impl fmt::Display for EndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for EndpointError {}

type Derived = Result<BackendIdentity, EndpointError>;

/// `nfs://host[:port]/export[/prefix]` from a data-mover NFS URL
/// (`nfs://host[:port]/export[:prefix][?query]`).
///
/// The two halves are read the way the backend uses them. The export is URL text that nfs-rs
/// parses with the `url` crate and sends to the server as `Url::path()` (so `a b` reaches the
/// server as `a%20b`, `.`/`..` are resolved, `#…` is a fragment); the prefix is a literal path
/// looked up inside the mounted export, whose `..` may not climb out of it. Each name the server
/// sees becomes one encoded segment, so `export:prefix` and `export/prefix` are the same endpoint
/// for ordinary names. The port comes from the authority, or from the first `nfsport=` query value
/// (as in nfs-rs); 2049 and 0 (portmapper) are dropped. `version`, `uid`, `gid`, `mountport` and
/// every other option are excluded.
pub(crate) fn nfs(url: &str) -> Derived {
    let rest = url
        .strip_prefix("nfs://")
        .ok_or(EndpointError("an NFS URL must start with nfs://"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let (export, prefix) = path.split_once(':').unwrap_or((path, ""));
    let (host, mut port) = host_and_port(authority)?;
    // With `#` in the export, nfs-rs reads the rest — query included — as a URL fragment.
    let query = if export.contains('#') { "" } else { query };
    let nfsport = form_urlencoded::parse(query.as_bytes()).find(|(key, _)| key == "nfsport");
    if let Some((_, value)) = nfsport {
        port = Some(
            value
                .parse()
                .map_err(|_| EndpointError("the NFS nfsport value is not a port"))?,
        );
    }
    let port = port.filter(|port| *port != 0 && *port != NFS_DEFAULT_PORT);
    let mut segments = nfs_export_segments(export)?;
    segments.extend(
        normalize(prefix)?
            .iter()
            .map(|name| encode(name.as_bytes())),
    );
    derive(BackendKind::Nfs, "nfs", &host, port, &segments)
}

/// The server-side names of an NFS export, as nfs-rs derives them from the URL.
fn nfs_export_segments(export: &str) -> Result<Vec<String>, EndpointError> {
    let parsed = Url::parse(&format!("nfs://export/{export}"))
        .map_err(|_| EndpointError("the NFS export is not a URL path"))?;
    Ok(parsed
        .path()
        .split('/')
        .filter(|name| !name.is_empty())
        .map(|name| encode(name.as_bytes()))
        .collect())
}

/// `smb://server[:port]/share[/root]` from the CIFS config fields.
///
/// Server and share are lowercased (share names are case-insensitive); an unbracketed IPv6
/// server is an address without a port, as smb-rs reads it. The root keeps its case, `\` and `/`
/// are equivalent, and `None`, `""` and `"/"` all mean the share root. Port 445 is dropped.
/// User, password, signing and guest policy are excluded.
pub(crate) fn smb(server: &str, share: &str, root: Option<&str>) -> Derived {
    let (host, port) = host_and_port(server)?;
    let port = port.filter(|port| *port != SMB_DEFAULT_PORT);
    let share = share.trim_matches(['/', '\\']).to_lowercase();
    if share.is_empty() || share.contains(['/', '\\']) {
        return Err(EndpointError("an SMB share is one non-empty name"));
    }
    let root = root.unwrap_or_default().replace('\\', "/");
    let mut segments = vec![encode(share.as_bytes())];
    segments.extend(normalize(&root)?.iter().map(|name| encode(name.as_bytes())));
    derive(BackendKind::Cifs, "smb", &host, port, &segments)
}

/// `s3://host[:port]/bucket[/prefix]` from the connected S3 storage's endpoint
/// (`http(s)://host[:port]`), bucket and prefix.
///
/// http and https are the same endpoint (the key space belongs to the service, not the
/// transport), so ports 80 and 443 are dropped whatever the scheme; the compatibility profile is
/// excluded. The bucket is lowercased. The prefix — URL text as the backend stores it — is taken
/// **literally**: S3 keys are not paths, so `a//`, `a/` and `/a/` are three key spaces, and only
/// the single trailing `/` the backend appends is removed.
pub(crate) fn s3(http_endpoint: &str, bucket: &str, prefix: Option<&str>) -> Derived {
    let authority = http_endpoint
        .strip_prefix("https://")
        .or_else(|| http_endpoint.strip_prefix("http://"))
        .ok_or(EndpointError(
            "an S3 endpoint must start with http:// or https://",
        ))?;
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if authority.contains('/') {
        return Err(EndpointError("an S3 endpoint has no path"));
    }
    let (host, port) = host_and_port(authority)?;
    let port = port.filter(|port| *port != 80 && *port != 443);
    let bucket = bucket.to_ascii_lowercase();
    if bucket.is_empty() || bucket.contains('/') {
        return Err(EndpointError("an S3 bucket is one non-empty name"));
    }
    let mut value = format!("s3://{}/{bucket}", authority_text(&host, port));
    // Emptiness is judged on the prefix as stored: `"/"` (keys under `/…`) is not the bucket root.
    let prefix = prefix.unwrap_or_default();
    if !prefix.is_empty() {
        value.push('/');
        value.push_str(prefix.strip_suffix('/').unwrap_or(prefix));
    }
    identity(BackendKind::S3, value)
}

/// `hdfs://nameservice[/root]` or `hdfs://namenode:port[/root]` from the parsed HDFS location
/// (`hdfs://host:port` or `hdfs://service`) and its absolute, decoded root.
///
/// The port is always kept: a missing port is what marks a `NameService`, so dropping 8020 would
/// turn a direct `NameNode` into a service name. A `NameService` keeps its case (it is a Hadoop
/// configuration key, `dfs.ha.namenodes.<service>`); a `NameNode` host is lowercased. The user and
/// Kerberos principal are excluded; an HA `NameService` keeps its logical name, so failover does
/// not change the endpoint.
pub(crate) fn hdfs(endpoint: &str, root: &str) -> Derived {
    let authority = endpoint
        .strip_prefix("hdfs://")
        .ok_or(EndpointError("an HDFS endpoint must start with hdfs://"))?;
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if authority.contains('/') {
        return Err(EndpointError("an HDFS endpoint has no path"));
    }
    let (host, port) = host_and_port(authority)?;
    let host = match port {
        Some(_) => host,
        None if authority.contains(['[', ':']) => {
            return Err(EndpointError(
                "an HDFS NameService is a name, not an address",
            ));
        }
        None => authority.to_owned(),
    };
    let segments = normalize(root)?
        .iter()
        .map(|name| encode(name.as_bytes()))
        .collect::<Vec<_>>();
    derive(BackendKind::Hdfs, "hdfs", &host, port, &segments)
}

/// `file:///path` (or `file://server/share/path` for a Windows UNC root) from the canonical
/// (absolute, symlink-resolved) Local root.
///
/// No hostname for local disks: a new container that mounts the same volume at the same path is
/// the same endpoint, while a different mount path is a different one. Unix names are encoded
/// byte for byte, so distinct non-UTF-8 names stay distinct. Windows verbatim prefixes
/// (`\\?\C:`, `\\?\UNC\server\share`) are written like their plain forms (`C:`, `//server/share`),
/// so the identity does not depend on how the path was canonicalized. The caller canonicalizes.
pub(crate) fn local(canonical: &Path) -> Derived {
    if !canonical.is_absolute() {
        return Err(EndpointError("a Local root must be absolute and canonical"));
    }
    let mut authority = String::new();
    let mut segments = Vec::new();
    for component in canonical.components() {
        match component {
            Component::Prefix(prefix) => match prefix.kind() {
                Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => {
                    segments.push(format!("{}:", char::from(letter).to_ascii_uppercase()));
                }
                Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                    authority = server.to_string_lossy().to_lowercase();
                    segments.push(encode(share.to_string_lossy().to_lowercase().as_bytes()));
                }
                _ => return Err(EndpointError("a Local root has an unsupported prefix")),
            },
            Component::Normal(name) => segments.push(encode(&os_bytes(name))),
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                return Err(EndpointError("a Local root must be canonical"));
            }
        }
    }
    identity(
        BackendKind::Local,
        format!("file://{authority}/{}", segments.join("/")),
    )
}

#[cfg(unix)]
fn os_bytes(name: &OsStr) -> Cow<'_, [u8]> {
    use std::os::unix::ffi::OsStrExt as _;
    Cow::Borrowed(name.as_bytes())
}

#[cfg(not(unix))]
fn os_bytes(name: &OsStr) -> Cow<'_, [u8]> {
    Cow::Owned(name.to_string_lossy().into_owned().into_bytes())
}

/// Splits `host[:port]`: `[v6][:port]`, an unbracketed IPv6 address (no port), or `name[:port]`.
/// An empty port means none, as in the `url` crate. IPv6 zone ids (`fe80::1%2`) are rejected: the
/// connect fails rather than two interfaces sharing one identity.
fn host_and_port(authority: &str) -> Result<(String, Option<u16>), EndpointError> {
    if authority.contains('@') {
        return Err(EndpointError(
            "an endpoint authority must not carry credentials",
        ));
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (address, after) = rest
            .split_once(']')
            .ok_or(EndpointError("an IPv6 host is missing its closing bracket"))?;
        let port = match after {
            "" => None,
            after => Some(
                after
                    .strip_prefix(':')
                    .ok_or(EndpointError("unexpected text after an IPv6 host"))?,
            ),
        };
        (ipv6(address)?, port)
    } else if authority.matches(':').count() > 1 {
        (ipv6(authority)?, None)
    } else {
        let (host, port) = authority
            .split_once(':')
            .map_or((authority, None), |(host, port)| (host, Some(port)));
        (host.to_ascii_lowercase(), port)
    };
    if host.is_empty() {
        return Err(EndpointError("an endpoint has no host"));
    }
    let port = port
        .filter(|port| !port.is_empty())
        .map(|port| {
            port.parse::<u16>()
                .map_err(|_| EndpointError("an endpoint port is not a number"))
        })
        .transpose()?;
    Ok((host, port))
}

fn ipv6(address: &str) -> Result<String, EndpointError> {
    address
        .parse::<Ipv6Addr>()
        .map(|address| format!("[{address}]"))
        .map_err(|_| EndpointError("an IPv6 host is not a valid address"))
}

/// Path names with `.` and empty segments dropped and `..` resolved; `..` above the start is an
/// error, never clamped.
fn normalize(path: &str) -> Result<Vec<&str>, EndpointError> {
    let mut segments = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments
                    .pop()
                    .ok_or(EndpointError("an endpoint path escapes its root"))?;
            }
            segment => segments.push(segment),
        }
    }
    Ok(segments)
}

fn encode(name: &[u8]) -> String {
    percent_encode(name, SEGMENT).to_string()
}

fn authority_text(host: &str, port: Option<u16>) -> String {
    port.map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"))
}

fn derive(
    kind: BackendKind,
    scheme: &str,
    host: &str,
    port: Option<u16>,
    segments: &[String],
) -> Derived {
    let mut value = format!("{scheme}://{}", authority_text(host, port));
    for segment in segments {
        value.push('/');
        value.push_str(segment);
    }
    identity(kind, value)
}

fn identity(kind: BackendKind, value: String) -> Derived {
    BackendIdentity::new(kind, value)
        .map_err(|_| EndpointError("the endpoint identity exceeds the model limits"))
}

#[cfg(test)]
#[path = "endpoint_tests.rs"]
mod tests;
