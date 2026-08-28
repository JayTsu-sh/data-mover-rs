#!/usr/bin/env python3
"""Enforce the target deep-module dependency directions."""

import re
import sys
from pathlib import Path


PUBLIC_MODULES = {"model", "storage", "traversal", "metadata", "transfer", "integrity"}
TARGET_MODULES = PUBLIC_MODULES | {"runtime"}
BACKENDS = {"local", "nfs", "cifs", "s3", "hdfs"}
ALLOWED = {
    "model": set(),
    "storage": {"model", "runtime"},
    "traversal": {"model", "storage", "runtime"},
    "metadata": {"model", "storage", "runtime"},
    "integrity": {"model", "storage", "runtime"},
    "transfer": {"model", "storage", "metadata", "integrity", "runtime"},
    "runtime": {"model"},
}
CRATE_REFERENCE = re.compile(r"\bcrate::\s*([A-Za-z_]\w*)")
CRATE_GROUP = re.compile(r"\bcrate::\s*\{([^{}]*)\}", re.DOTALL)
CRATE_ALIAS = re.compile(r"\buse\s+crate\s+as\s+([A-Za-z_]\w*)\s*;")
PATH_ALIAS = re.compile(
    r"\buse\s+crate::([A-Za-z_]\w*(?:::[A-Za-z_]\w*)*)\s+as\s+([A-Za-z_]\w*)\s*;"
)
BACKEND_REFERENCE = re.compile(r"\bcrate::storage::backends::\s*([A-Za-z_]\w*)")
BACKEND_GROUP = re.compile(r"\bcrate::storage::backends::\s*\{([^{}]*)\}", re.DOTALL)
SUPER_REFERENCE = re.compile(r"\b((?:super::)+)([A-Za-z_]\w*)")
BACKEND_KIND_BRANCH = re.compile(r"\bBackendKind\s*::")
PRODUCT_TERM = re.compile(r"terrasync", re.IGNORECASE)
TOKEN = re.compile(r"[A-Za-z_]\w*|::|[{},;]")


def rust_files(path: Path):
    return sorted(path.rglob("*.rs")) if path.is_dir() else []


def relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def code_only(text: str) -> str:
    result = list(text)
    index = 0
    while index < len(text):
        if text.startswith("//", index):
            end = text.find("\n", index)
            end = len(text) if end < 0 else end
        elif text.startswith("/*", index):
            depth, end = 1, index + 2
            while end < len(text) and depth:
                if text.startswith("/*", end):
                    depth += 1
                    end += 2
                elif text.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
        else:
            raw = re.match(r"(?:br|r)(#{0,16})\"", text[index:])
            if raw:
                terminator = '"' + raw.group(1)
                end = text.find(terminator, index + len(raw.group(0)))
                end = len(text) if end < 0 else end + len(terminator)
            elif text[index] == '"' or (text[index] == "b" and index + 1 < len(text) and text[index + 1] == '"') or re.match(r"(?:b)?'(?:\\.|[^'\\])'", text[index:]):
                quote_index = index + 1 if text[index] == "b" else index
                quote = text[quote_index]
                end = quote_index + 1
                while end < len(text):
                    if text[end] == "\\":
                        end += 2
                    elif text[end] == quote:
                        end += 1
                        break
                    else:
                        end += 1
            else:
                index += 1
                continue
        for position in range(index, end):
            if result[position] != "\n":
                result[position] = " "
        index = end
    return "".join(result)


def parse_use_tree(tokens: list[str], index: int, prefix: tuple[str, ...] = ()):
    paths = []
    if index >= len(tokens) or not re.fullmatch(r"[A-Za-z_]\w*", tokens[index]):
        return paths, index
    path = prefix + (tokens[index],)
    index += 1
    if index < len(tokens) and tokens[index] == "as":
        alias = tokens[index + 1] if index + 1 < len(tokens) else ""
        return [(path, alias)], index + 2
    if index < len(tokens) and tokens[index] == "::":
        index += 1
        if index < len(tokens) and tokens[index] == "{":
            index += 1
            while index < len(tokens) and tokens[index] != "}":
                children, index = parse_use_tree(tokens, index, path)
                paths.extend(children)
                if index < len(tokens) and tokens[index] == ",":
                    index += 1
            return paths, index + 1
        return parse_use_tree(tokens, index, path)
    return [(path, "")], index


def resolved_paths(code: str) -> set[tuple[str, ...]]:
    tokens = TOKEN.findall(code)
    paths = set()
    aliases = {}
    for index in range(len(tokens) - 4):
        if tokens[index:index + 4] == ["extern", "crate", "self", "as"]:
            aliases[tokens[index + 4]] = ("crate",)
    index = 0
    while index < len(tokens):
        if tokens[index] == "use":
            imported, index = parse_use_tree(tokens, index + 1)
            for path, alias in imported:
                if path and path[0] in aliases:
                    path = aliases[path[0]] + path[1:]
                paths.add(path)
                if alias:
                    aliases[alias] = path
        else:
            index += 1
    for index, token in enumerate(tokens):
        if token == "crate" and index + 2 < len(tokens) and tokens[index + 1] == "::":
            if tokens[index + 2] != "{":
                paths.add(("crate", tokens[index + 2]))
        if token in aliases and index + 2 < len(tokens) and tokens[index + 1] == "::":
            suffix = []
            cursor = index + 2
            while cursor < len(tokens) and re.fullmatch(r"[A-Za-z_]\w*", tokens[cursor]):
                suffix.append(tokens[cursor])
                if cursor + 1 >= len(tokens) or tokens[cursor + 1] != "::":
                    break
                cursor += 2
            paths.add(aliases[token] + tuple(suffix))
    return paths


def grouped_roots(contents: str) -> set[str]:
    return {
        match.group(1)
        for item in contents.split(",")
        if (match := re.match(r"\s*([A-Za-z_]\w*)", item)) and match.group(1) not in {"self", "super"}
    }


def dependency_roots(code: str, module_depth: int) -> set[str]:
    roots = {path[1] for path in resolved_paths(code) if len(path) > 1 and path[0] == "crate"}
    for contents in CRATE_GROUP.findall(code):
        roots.update(grouped_roots(contents))
    for alias in CRATE_ALIAS.findall(code):
        roots.update(re.findall(rf"\b{re.escape(alias)}::([A-Za-z_]\w*)", code))
    for prefix, alias in PATH_ALIAS.findall(code):
        if re.search(rf"\b{re.escape(alias)}::", code):
            roots.add(prefix.split("::", maxsplit=1)[0])
    for supers, root in SUPER_REFERENCE.findall(code):
        if supers.count("super::") >= module_depth:
            roots.add(root)
    return roots


def backend_roots(code: str, backend_depth: int) -> set[str]:
    roots = {
        path[3]
        for path in resolved_paths(code)
        if len(path) > 3 and path[:3] == ("crate", "storage", "backends")
    }
    for contents in BACKEND_GROUP.findall(code):
        roots.update(grouped_roots(contents))
    for prefix, alias in PATH_ALIAS.findall(code):
        if prefix == "storage::backends" and re.search(rf"\b{re.escape(alias)}::", code):
            roots.update(re.findall(rf"\b{re.escape(alias)}::([A-Za-z_]\w*)", code))
        if prefix == "storage":
            roots.update(
                re.findall(rf"\b{re.escape(alias)}::backends::([A-Za-z_]\w*)", code)
            )
    for alias in CRATE_ALIAS.findall(code):
        roots.update(
            re.findall(
                rf"\b{re.escape(alias)}::storage::backends::([A-Za-z_]\w*)",
                code,
            )
        )
    for supers, root in SUPER_REFERENCE.findall(code):
        if supers.count("super::") >= backend_depth and root in BACKENDS:
            roots.add(root)
    return roots


def validate(root: Path) -> list[str]:
    source = root / "src"
    errors = []
    lib_text = code_only((source / "lib.rs").read_text())
    for module in sorted(PUBLIC_MODULES):
        declaration = re.compile(rf"\bpub\s+mod\s+{re.escape(module)}\s*;")
        if not declaration.search(lib_text) or not (source / module / "mod.rs").is_file():
            errors.append(f"public module {module} is missing or has incorrect visibility")
    if not re.search(r"\bpub\s*\(\s*crate\s*\)\s+mod\s+runtime\s*;", lib_text) or not (source / "runtime" / "mod.rs").is_file():
        errors.append("runtime is missing or is not crate-private")

    for module in sorted(TARGET_MODULES):
        for path in rust_files(source / module):
            text = path.read_text()
            code = code_only(text)
            relative_path = path.relative_to(source / module)
            module_depth = len(relative_path.parts) if path.name == "mod.rs" else len(relative_path.parts) + 1
            for dependency in dependency_roots(code, module_depth):
                if dependency != module and dependency not in ALLOWED[module]:
                    errors.append(
                        f"{relative(path, root)}: {module} must not import {dependency}"
                    )
            if PRODUCT_TERM.search(text):
                errors.append(f"{relative(path, root)}: terrasync product term is forbidden")
            if module in {"transfer", "traversal"} and BACKEND_KIND_BRANCH.search(code):
                errors.append(f"{relative(path, root)}: {module} must not branch on BackendKind")

    backend_root = source / "storage" / "backends"
    backend_declarations = code_only((backend_root / "mod.rs").read_text()) if (backend_root / "mod.rs").is_file() else ""
    for backend in sorted(BACKENDS):
        facade = backend_root / backend / "mod.rs"
        backend_declaration = re.compile(rf"\bpub\s*\(\s*crate\s*\)\s+mod\s+{backend}\s*;")
        if not facade.is_file() or not backend_declaration.search(backend_declarations):
            errors.append(f"backend facade {backend} is missing")
            continue
        for path in rust_files(facade.parent):
            code = code_only(path.read_text())
            backend_relative_path = path.relative_to(facade.parent)
            backend_depth = (
                len(backend_relative_path.parts)
                if path.name == "mod.rs"
                else len(backend_relative_path.parts) + 1
            )
            for dependency in dependency_roots(code, len(path.relative_to(source).parts)):
                if dependency not in {"model", "storage"}:
                    errors.append(
                        f"{relative(path, root)}: backend {backend} must not import {dependency}"
                    )
            for imported_backend in backend_roots(code, backend_depth):
                if imported_backend != backend:
                    errors.append(
                        f"{relative(path, root)}: backend {backend} must not import backend {imported_backend}"
                    )
    return errors


def main() -> int:
    try:
        root = Path(sys.argv[1]).resolve()
        errors = validate(root)
    except (IndexError, OSError) as error:
        print(f"architecture dependency guard error: {error}", file=sys.stderr)
        return 2
    if errors:
        for error in errors:
            print(f"architecture dependency guard error: {error}", file=sys.stderr)
        return 2
    print("architecture dependency guard: passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
