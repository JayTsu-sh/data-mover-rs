# shellcheck shell=bash
# The one shell builder of S3 URLs for the skills (source it). The crate takes the key pair as
# written, undecoded: the credentials end at the last '@', so nothing is escaped here.
#
#   s3_url <bucket> <prefix>
#     S3_AK, S3_SK, S3_HOST (host[:port])
#     S3_USE_HTTPS=true      → s3+https:// (otherwise s3://)
#     S3_COMPAT=sg|dxn|hcp|ontap → ?compat=… (empty = standard S3)
#     S3_TLS_VERIFY=false    → tls_verify=false (s3+https:// only; self-signed endpoints; empty or
#                            true = verify)
#     S3_BUCKET_TYPE=nas     → bucket_type=nas (only with S3_COMPAT=ontap; empty or object = object)
# The same rule as .claude/skills/_shared/url_builder.py and examples/endpoint_support.
#
# Prints the URL; fails (status 2) on a value the crate would refuse. Call it once before any
# request to check the environment.
s3_url() {
  local bucket=$1 prefix=${2#/} scheme=s3 query=()
  [ "${S3_USE_HTTPS:-}" = true ] && scheme=s3+https
  case "${S3_COMPAT:-}" in
    '') ;;
    sg|dxn|hcp|ontap) query+=("compat=$S3_COMPAT") ;;
    *) echo "S3_COMPAT must be empty, sg, dxn, hcp or ontap: ${S3_COMPAT}" >&2; return 2 ;;
  esac
  case "${S3_TLS_VERIFY:-}" in
    ''|true) ;;
    false) [ "$scheme" = s3+https ] || { echo "S3_TLS_VERIFY=false needs S3_USE_HTTPS=true" >&2; return 2; }
           query+=("tls_verify=false") ;;
    *) echo "S3_TLS_VERIFY must be empty, true or false: ${S3_TLS_VERIFY}" >&2; return 2 ;;
  esac
  case "${S3_BUCKET_TYPE:-}" in
    ''|object) ;;
    nas) [ "${S3_COMPAT:-}" = ontap ] || { echo "S3_BUCKET_TYPE=nas needs S3_COMPAT=ontap" >&2; return 2; }
         query+=("bucket_type=nas") ;;
    *) echo "S3_BUCKET_TYPE must be empty, object or nas: ${S3_BUCKET_TYPE}" >&2; return 2 ;;
  esac
  local q=; [ ${#query[@]} -gt 0 ] && q="?$(IFS='&'; echo "${query[*]}")"
  printf '%s://%s:%s@%s.%s/%s%s\n' "$scheme" "$S3_AK" "$S3_SK" "$bucket" "$S3_HOST" "$prefix" "$q"
}
