"""Build standard URLs for the 4 backends with project-specific parameters."""

from __future__ import annotations


def nfs_url(
    host: str,
    export: str,
    sub_path: str = "",
    port: int | None = None,
    uid: int = 1000,
    gid: int = 1000,
    options: str = "",
) -> str:
    """`options` is appended to the query as given, e.g. `version=4.1&noresvport=true`.

    Leave `uid` / `gid` out of `options`: nfs-rs takes the first match, so the ones built from the
    arguments win.
    """
    host_part = f"{host}:{port}" if port else host
    path = f"/{export.lstrip('/')}"
    if sub_path:
        path += ":/" + sub_path.lstrip("/")
    extra = f"&{options.lstrip('?&')}" if options else ""
    return f"nfs://{host_part}{path}?uid={uid}&gid={gid}{extra}"


def s3_url(
    bucket: str,
    host: str,
    access_key: str = "",
    secret_key: str = "",
    prefix: str = "",
    port: int | None = None,
    use_https: bool = False,
    compat: str = "",
    tls_verify: bool = True,
    bucket_type: str = "",
) -> str:
    """`compat` (sg|dxn|hcp|ontap), `tls_verify=False` (HTTPS only) and `bucket_type` (nas, with
    compat=ontap) become the URL's options; the defaults are left out, as the crate requires.

    The crate takes the key pair as written (split at the last '@', not percent-decoded).
    """
    if compat not in ("", "sg", "dxn", "hcp", "ontap"):
        raise ValueError(f"compat must be empty, sg, dxn, hcp or ontap: {compat}")
    if bucket_type not in ("", "nas"):
        raise ValueError(f"bucket_type must be empty or nas: {bucket_type}")
    if bucket_type == "nas" and compat != "ontap":
        raise ValueError("bucket_type=nas needs compat=ontap")
    if not tls_verify and not use_https:
        raise ValueError("tls_verify=False needs use_https=True")
    scheme = "s3+https" if use_https else "s3"
    auth = ""
    if access_key or secret_key:
        auth = f"{access_key}:{secret_key}@"
    host_part = f"{bucket}.{host}"
    if port:
        host_part += f":{port}"
    path = f"/{prefix.lstrip('/')}" if prefix else "/"
    options = [f"compat={compat}"] if compat else []
    if not tls_verify:
        options.append("tls_verify=false")
    if bucket_type:
        options.append(f"bucket_type={bucket_type}")
    query = f"?{'&'.join(options)}" if options else ""
    return f"{scheme}://{auth}{host_part}{path}{query}"


def local_url(path: str) -> str:
    return path  # local 无 scheme
