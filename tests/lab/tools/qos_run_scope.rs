//! Resolve cleanup endpoints before allowing any destructive backend operation.
use std::env::current_dir;
use std::error::Error;
use std::fs::canonicalize;
use std::path::{Path, PathBuf};

use percent_encoding::percent_decode_str;

pub(crate) fn validated_endpoint(endpoint: &str, run_id: &str) -> Result<String, Box<dyn Error>> {
    if !(run_id.starts_with("nightly-") || run_id.starts_with("release-"))
        || run_id.len() > 88
        || run_id.contains("..")
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    {
        return Err("an isolated lab run identifier is required".into());
    }
    let (path, remote) = endpoint_path(endpoint)?;
    let parts: Vec<_> = path.split(['/', '\\']).collect();
    if !parts.contains(&run_id)
        || parts
            .iter()
            .any(|part| matches!(*part, "." | "..") || part.contains('\0'))
    {
        return Err("the source endpoint must be scoped to the isolated run".into());
    }
    if remote {
        return Ok(endpoint.to_owned());
    }
    validated_local_endpoint(Path::new(&path), run_id)
}

fn validated_local_endpoint(path: &Path, run_id: &str) -> Result<String, Box<dyn Error>> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        current_dir()?.join(path)
    };
    let mut anchor = PathBuf::new();
    for component in absolute.components() {
        anchor.push(component.as_os_str());
        if component.as_os_str() == run_id {
            break;
        }
    }
    let parent = anchor
        .parent()
        .ok_or("the isolated run requires a parent directory")?;
    let expected = canonicalize(parent)?
        .join(run_id)
        .join(absolute.strip_prefix(&anchor)?);
    let resolved = canonicalize(&absolute)?;
    if resolved != expected {
        return Err("cleanup cannot follow a symlink at or below the isolated run root".into());
    }
    Ok(resolved
        .to_str()
        .ok_or("cleanup requires a UTF-8 root")?
        .to_owned())
}

fn endpoint_path(endpoint: &str) -> Result<(String, bool), Box<dyn Error>> {
    let path = if let Some((scheme, rest)) = endpoint.split_once("://") {
        if !matches!(scheme, "nfs" | "smb" | "hdfs" | "s3" | "s3+https") {
            return Err("unsupported isolated cleanup scheme".into());
        }
        // The S3 endpoint parser permits slashes in credentials and splits at the last @.
        let authority = rest.rsplit_once('@').map_or(rest, |(_, rest)| rest);
        let authority = authority.split(['?', '#']).next().unwrap_or_default();
        authority.find('/').map_or("", |start| &authority[start..])
    } else if endpoint.starts_with("smb:")
        || endpoint.starts_with("s3:")
        || endpoint.starts_with("s3@")
    {
        endpoint
            .split_once(':')
            .ok_or("a scoped endpoint path is required")?
            .1
    } else {
        return Ok((endpoint.to_owned(), false));
    };
    Ok((percent_decode_str(path).decode_utf8()?.into_owned(), true))
}
