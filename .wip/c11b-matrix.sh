#!/bin/bash
# c11b-matrix.sh <group>: P7 C11b real-env matrix for one S3 store, sequential; logs in c11b-real/<group>-<n>.log
set -u
SP=/tmp/claude-1000/-home-keyee-Projects-data-mover-rs/1a33a43a-894b-4998-9b23-2feeca5f1173/scratchpad
cd $SP/c11b-run
export PATH=$SP/c11b-shim:$PATH
S=.claude/skills/e2e-s3
L=$SP/c11b-real
N=0
run() { local name=$1; shift; N=$((N+1)); [ -n "${ONLY:-}" ] && [[ " $ONLY " != *" $name "* ]] && return; echo "=== $name $(date +%T)"; env RUN=$(date +%s)-$GI$N "$@" > $L/$name.log 2>&1; echo "--- $name exit=$? $(grep -E 'RESULT' $L/$name.log | tail -1)"; }
case $1 in
  minio) GI=1;; ontap) GI=2;; rustfs) GI=3;; dxn) GI=4;; sg) GI=5;; esac
case $1 in
  minio)
    run minio-tv-ver timeout 1800 bash $S/scripts/traversal_versions.sh
    run minio-tv-unver S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
    run minio-do-ver timeout 3500 bash $S/scripts/directory_objects.sh
    run minio-do-unver S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh
    run minio-vm SKIP_RESUME=1 timeout 5400 bash $S/scripts/versioning_matrix.sh ;;
  ontap)
    E=$S/ontap/.env
    run ontap-tv-ver S3_ENV=$E S3_VERSIONED_BUCKET=data-mover-test-ver timeout 1800 bash $S/scripts/traversal_versions.sh
    run ontap-tv-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
    run ontap-do-ver S3_ENV=$E S3_VERSIONED_BUCKET=data-mover-test-ver timeout 3500 bash $S/scripts/directory_objects.sh
    run ontap-do-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh
    run ontap-vm S3_ENV=$E S3_VERSIONED_BUCKET=data-mover-test-ver SKIP_RESUME=1 timeout 5400 bash $S/scripts/versioning_matrix.sh
    run nas-tv S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-nas EXPECT_DIR_MARKER=0 timeout 1800 bash $S/scripts/traversal_versions.sh
    run nas-matrix S3_ENV=$E timeout 5400 bash $S/scripts/nas_matrix.sh ;;
  rustfs)
    E=$S/rustfs/.env
    run rustfs-tv-ver S3_ENV=$E EXPECT_ORDER=inferred BUCKET_PREFIX=data-mover-rustfs timeout 1800 bash $S/scripts/traversal_versions.sh
    run rustfs-tv-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
    run rustfs-do-ver S3_ENV=$E BUCKET_PREFIX=data-mover-rustfs timeout 3500 bash $S/scripts/directory_objects.sh
    run rustfs-do-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh ;;
  dxn)
    E=$S/dxn/.env
    run dxn-tv-ver S3_ENV=$E BUCKET_PREFIX=data-mover-dxn S3_LOCATION=magnascale:tos_placement timeout 1800 bash $S/scripts/traversal_versions.sh
    run dxn-tv-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
    run dxn-do-ver S3_ENV=$E BUCKET_PREFIX=data-mover-dxn S3_LOCATION=magnascale:tos_placement timeout 3500 bash $S/scripts/directory_objects.sh
    run dxn-do-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh ;;
  sg)
    E=$S/storagegrid/.env
    run sg-tv-ver S3_ENV=$E S3_VERSIONED_BUCKET=data-mover-test-ver timeout 1800 bash $S/scripts/traversal_versions.sh
    run sg-tv-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
    run sg-do-ver S3_ENV=$E S3_VERSIONED_BUCKET=data-mover-test-ver timeout 3500 bash $S/scripts/directory_objects.sh
    run sg-do-unver S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh ;;
esac
echo "=== done $1 $(date +%T)"
