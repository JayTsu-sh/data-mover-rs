# CIFS checkpoint protection on UNIX security-style storage

Date: 2026-10-23. Scope: investigation and proposed design, not an implemented fix.
No share, volume, ACL, or production-code changes were made for this investigation.

## Conclusion

Keep the confidentiality requirement, but do not equate it universally with the
NTFS `SE_DACL_PROTECTED` flag. Handle permission-model differences inside the CIFS
backend's existing checkpoint-protection interface. Do not change a customer's
volume security style, silently ignore failed protection, or add a transfer-request
flag that permits insecure checkpoints.

The current SMB interface does not provide sufficient evidence to implement a
general, automatic UNIX-permission fallback safely. Obtain an authoritative,
supported permission-model observation before enabling such a path. Until then,
retain fail-closed behavior; this recommendation is not a claim that the existing
UNIX-style fixture already passes checkpointed recovery.

## Primary-source findings

1. ONTAP can return a constructed ACL representing UNIX permissions. Its documented
   purpose includes copying a file's security properties to a temporary replacement
   while preserving UNIX permissions; this does not install native NTFS ACLs.
   [NetApp: preserving UNIX permissions](https://docs.netapp.com/us-en/ontap/smb-admin/how-ontap-preserves-unix-permissions-concept.html).
2. For UNIX-style volumes/qtrees, actual file access is governed by UNIX mode bits
   or NFSv4 ACLs. ONTAP's diagnostic output can show effective security style,
   UNIX UID/GID, mode, and ACL information. Therefore, neither a security-style
   name nor mode bits alone establish equivalent protection in every case.
   [NetApp: inspecting UNIX file security](https://docs.netapp.com/us-en/ontap/smb-admin/display-file-security-unix-style-volumes-task.html).
3. `SE_DACL_PROTECTED` prevents modification by inheritable ACEs. It does not mean
   owner-only access. Dropping this requirement universally would remove an actual
   NTFS protection against later inheritance changes.
   [Microsoft: security descriptor control](https://learn.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-control).
4. Mixed-style storage can change effective file security according to the client
   that changes permissions. Treat effective security as a per-object fact, not
   merely a cached share-wide label.
   [NetApp: security styles](https://docs.netapp.com/us-en/ontap/smb-admin/security-style-inheritance-concept.html).

## Current implementation and evidence

- [checkpoint_acl.rs](../../src/storage/backends/cifs/checkpoint_acl.rs) copies the
  payload DACL to an empty pointer temporary, requests protected inheritance, and
  requires the read-back DACL and protected bit to match before writing pointer
  contents. It does not establish that either file is necessarily owner-only.
- [protocol.rs](../../src/storage/backends/cifs/protocol.rs) queries and sets the
  DACL selection; the current query does not request owner/group or authoritative
  UNIX mode/NFSv4 ACL information. Matching DACL bytes alone is not a general
  proof of matching effective readers.
- [at_destination.rs](../../src/storage/backends/cifs/at_destination.rs) performs
  protection before writing checkpoint bytes. Preserve that ordering, as well as
  ownership fencing and pointer-first permission tightening.
- [codec.rs](../../src/storage/range_hash/codec.rs) persists raw source bytes for
  incomplete BLAKE3 leaves. A checkpoint is therefore not just harmless hash data.
- Existing [CIFS validation](checkpointed-range-hash-cifs-validation.md) observed
  successful SET SECURITY and unchanged DACL bytes, but the requested protected
  bit was absent on read-back. The transfer refused the checkpoint before writing
  fragments. This establishes why the current check fails; it does **not** prove
  underlying UNIX permissions, account mapping, or cross-protocol confidentiality.

## Recommended design

Keep the existing backend-private protection interface and select between two
verified implementations; generic transfer code should not learn ONTAP policy.

| Effective permission model | Required evidence before checkpoint bytes |
| --- | --- |
| Native NTFS | Required DACL, appropriate owner/group context, and protection against inheritance |
| Positively identified UNIX | Preserved effective UNIX permissions, UID/GID and applicable ACL semantics; no wider access than the intended payload/staging policy |
| Unknown, ambiguous mixed style, or unsupported observation | Refuse before writing sensitive checkpoint contents |

For the UNIX path, copying the constructed ACL is a documented candidate mechanism,
not by itself a complete verifier. Determine what the supported SMB stack can
observe authoritatively. Do not identify UNIX style solely from a missing protected
bit, a SID pattern, or a server product name. Native ONTAP CLI/NFS observations are
useful independent lab evidence; requiring storage-administrator credentials in
the generic transfer library would be a disproportionate runtime dependency.

The policy must explicitly account for source/final ACLs that are stricter than the
initial staging-file ACL. Merely matching a broadly readable stage does not prove
that a checkpoint containing source fragments meets the intended confidentiality
policy. Administrative overrides and hostile external permission changes are not
solved by a single read-back check; retain the existing exclusive-ownership premise.

## Alternatives and why they are not the first patch

- Changing the whole volume to NTFS style affects other applications and protocol
  access. A separately provisioned NTFS-style test area can be an operational
  workaround, subject to authorization, not the compatibility fix.
- Persisting only completed BLAKE3 leaves could avoid raw fragments, but requires
  closing partial leaves without rereading completed source ranges. Hash coverage
  may extend beyond committed destination coverage; sparse/out-of-order ranges and
  checkpoint publication ordering need a separate design.
- Persisting fragment commitments and rebuilding fragments from committed target
  bytes can avoid source rereads on readable staged-file backends. The target bytes
  must be verified against the original source commitments before use, or corruption
  could contaminate the supposed source hash. This is not universal: unfinished S3
  multipart data is not an ordinary readable staged file. Tiny fragment digests can
  also disclose low-entropy fragments by enumeration; digest-only is not encryption.
- Encryption with a key next to the checkpoint does not solve disclosure to the
  same readers; an externally persisted key adds a new lifecycle and dependency.

## Acceptance tests before enabling a UNIX path

These are proposed tests, not tests executed by this investigation:

1. Independently inspect effective style, UID/GID, mode and any NFSv4 ACL for the
   payload and pointer using read-only server/NFS observations.
2. Test distinct ordinary identities, including SMB-to-UNIX mappings; distinguish
   multiple SMB accounts mapped to the same UNIX identity. Check both allowed and
   denied reads over SMB and, where exposed, NFS.
3. Exercise private and permissive parents, different owners/groups, restrictive
   final metadata, applicable NFSv4 ACLs, and parent inheritance changes.
4. Use odd backend chunk sizes that actually leave raw fragments, interrupted
   checkpoint replacement and new-process recovery. Audit that no completed source
   prefix is reread and that the final independent file hash agrees.
5. Preserve native-NTFS negative tests for lost inheritance protection and unknown
   model tests that reject before writing any sensitive bytes.

The next implementation decision is gated on obtaining the supported UNIX-model
observation, not on weakening the current check to make one fixture pass.
