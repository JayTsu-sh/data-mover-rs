"""Build standard URLs for the 4 backends with project-specific parameters."""

from __future__ import annotations

from urllib.parse import quote




def nfs_url(
    host: str,
    export: str,
    sub_path: str = "",
    port: int | None = None,
    uid: int = 1000,
    gid: int = 1000,
) -> str:
    host_part = f"{host}:{port}" if port else host
    path = f"/{export.lstrip('/')}"
    if sub_path:
        path += ":/" + sub_path.lstrip("/")
    return f"nfs://{host_part}{path}?uid={uid}&gid={gid}"


def s3_url(
    bucket: str,
    host: str,
    access_key: str = "",
    secret_key: str = "",
    prefix: str = "",
    port: int | None = None,
    use_https: bool = False,
) -> str:
    scheme = "s3+https" if use_https else "s3"
    auth = ""
    if access_key or secret_key:
        auth = f"{quote(access_key)}:{quote(secret_key)}@"
    host_part = f"{bucket}.{host}"
    if port:
        host_part += f":{port}"
    path = f"/{prefix.lstrip('/')}" if prefix else "/"
    return f"{scheme}://{auth}{host_part}{path}"


def local_url(path: str) -> str:
    return path  # local 无 scheme
