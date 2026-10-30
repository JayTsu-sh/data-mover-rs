"""Directed QoS routes: all seven regressions and all five-family cross pairs."""
FAMILIES = ("local", "nfs41", "cifs", "s3", "hdfs")
SOURCES = ("local", "nfs3", "nfs40", "nfs41", "cifs", "s3", "hdfs")


def profiles(config):
    """Endpoints contain private env only; returned names are safe evidence labels."""
    result = []
    for source in SOURCES:
        destinations = ["local"]
        if source in FAMILIES:
            destinations += [kind for kind in FAMILIES if kind != source and kind != "local"]
        for destination in destinations:
            name = f"{source}-to-{destination}"
            source_env = config["sources"][source]["env"]
            target = config["destinations"][destination]
            env = dict(source_env)
            for key, value in target.get("env", {}).items():
                if key in env and env[key] != value:
                    raise ValueError(f"conflicting endpoint environment key: {key}")
                env[key] = value
            if destination != "local":
                env["QOS_DESTINATION"] = target["endpoint"].replace("{pair}", name)
            default = destination == "local"
            result.append(dict(name=name, source_kind=source, destination_kind=destination,
                               env=env, transfer_profile="default" if default else "cross",
                               size_mib=128, chunk_bytes=8 * 1024 * 1024 if source == "s3" else 2 * 1024 * 1024,
                               performance_size_mib=512 if default and source not in ("nfs40", "s3") else 128,
                               soft_mibps=4 if default else 8))
    return result


def validate_override(original, revised):
    """A longer performance fixture must retain the actual route and finite limits."""
    fixed = {key: value for key, value in original.items() if key != "performance_size_mib"}
    candidate = {key: value for key, value in revised.items() if key != "performance_size_mib"}
    if fixed != candidate:
        raise ValueError("performance requalification changed the route, policy or finite limits")
