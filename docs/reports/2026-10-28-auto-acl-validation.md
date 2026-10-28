# Automatic ACL mapping validation — 2026-10-28

Data mover uses the published nfs-rs 0.9.1 and the same test endpoints and fixture roots as the
[four-way validation](2026-10-28-nfs-acl-four-way-validation.md). The transfer request asks only
for ACL copying. Numeric identity and group-resolution equivalence is not verified by data mover;
every cross-model result reports that uncertainty.

The [live run](2026-10-28-auto-acl-live.log) copied and checked a regular file in all four
POSIX/NFSv4 source and destination combinations. A marked NFSv4 source with an `EVERYONE@`
`WRITE_ACL` grant was copied to POSIX; the result reports its original ACE index and rights mask.
The file content was published, and an independent kernel NFS client confirmed matching current
read/write/execute decisions for four tested identities ([access log](2026-10-28-auto-acl-access.log)).

A second marked ACL could not be represented by the supported ordered POSIX template. The
[copy report](2026-10-28-auto-acl-fallback.log) identifies the original ACE and the failed proof,
and reports `AppliedConservativeAcl`. A separate client read back a POSIX ACL with all three base
permissions zero and confirmed two unprivileged identities were denied read and write access
([fallback access log](2026-10-28-auto-acl-fallback-access.log)).

These tests establish the behavior for the tested server configurations and numeric identities;
they do not establish identity-domain equivalence between arbitrary endpoints.

An unrepresentable POSIX group combination was also copied to NFSv4 with the conservative
fallback. The destination accepted the regular-file deny-all ACE and the library verified its
readback ([copy log](2026-10-28-auto-acl-nfs4-fallback.log)); a kernel client independently read
the ACL and confirmed two unprivileged identities were denied access
([access log](2026-10-28-auto-acl-nfs4-fallback-access.log)).

A POSIX-to-CIFS conservative fallback was tested against an ONTAP CIFS share. The server
acknowledged the ACL SET but returned an unprotected DACL on immediate readback. The transfer
therefore reported the ACL failure and the uncopied entries and did not publish the file. The
[readback log](2026-10-28-auto-acl-cifs-readback.log) includes the test harness's expected
exception after the copy failed and its cleanup. This server cannot safely publish this fallback
with the observed behavior.
