"""Build standard URLs for the 4 backends with project-specific parameters."""

from __future__ import annotations

from urllib.parse import quote


def cifs_url(
    host: str,
    share: str,
    user: str = "",
    password: str = "",
    sub_path: str = "",
    port: int | None = None,
    smb2_only: bool = True,
    anon: bool = False,
) -> str:
    auth = ""
    if user or password:
        auth = f"{quote(user)}:{quote(password)}@"
    elif anon:
        auth = "guest:@"

    host_part = f"{host}:{port}" if port else host
    path = f"/{share}"
    if sub_path:
        path += "/" + sub_path.lstrip("/")

    params: list[str] = []
    if smb2_only is False:
        params.append("smb2_only=false")
    if anon:
        params.append("anon=true")
    query = "?" + "&".join(params) if params else ""

    return f"smb://{auth}{host_part}{path}{query}"


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
