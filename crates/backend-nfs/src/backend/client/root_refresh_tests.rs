//! `NFSv3` loopback fixture exercising mount, attachment, and stale-handle recovery.
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{error::Error, io};

use bytes::{Buf, BufMut, Bytes};
use nfs_rs::{Nfs3ErrorCode, NfsError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{GLOBAL_CACHE, NFSStorage, classify_role_error, create_nfs_role_storage};
use crate::model::{FailureClass, SourceVersion, StoragePath, Transience};
use crate::storage::{ReadRequest, StorageRoleFailure};

const NFS_PROGRAM: u32 = 100_003;
const MOUNT_PROGRAM: u32 = 100_005;
const ACL_PROGRAM: u32 = 100_227;
const EXPORT: &[u8] = b"export";
const ORIGINAL: &[u8] = b"original";
const REPLACEMENT: &[u8] = b"replacement";

#[derive(Clone, Copy)]
enum RootReply {
    Same,
    Replaced,
    Missing,
}

struct Server {
    next_root: RootReply,
    stale_leaf: AtomicBool,
    lookups: Mutex<Vec<(Bytes, Bytes)>>,
}

async fn start_server(next_root: RootReply) -> (String, Arc<Server>, JoinHandle<io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(Server {
        next_root,
        stale_leaf: AtomicBool::new(true),
        lookups: Mutex::new(Vec::new()),
    });
    let serving = Arc::clone(&server);
    let task = tokio::spawn(async move { serving.serve(listener).await });
    let url = format!(
        "nfs://127.0.0.1:{port}/export:configured?version=3&mountport={port}&noresvport=true"
    );
    (url, server, task)
}

fn opaque(output: &mut Vec<u8>, value: &[u8]) {
    output.put_u32(u32::try_from(value.len()).unwrap());
    output.extend_from_slice(value);
    output.resize(output.len().next_multiple_of(4), 0);
}

fn take_opaque(input: &mut Bytes) -> Bytes {
    let length = input.get_u32() as usize;
    input.split_to(length.next_multiple_of(4)).slice(..length)
}

fn lookup_ok(handle: &[u8], file_type: u32) -> Vec<u8> {
    let mut payload = 0u32.to_be_bytes().to_vec();
    opaque(&mut payload, handle);
    payload.put_u32(1); // Object attributes.
    payload.put_u32(file_type);
    payload.extend_from_slice(&[0; 80]); // Remaining fattr3 fields.
    payload.put_u32(0); // Parent attributes are optional.
    payload
}

fn lookup_error(status: u32) -> Vec<u8> {
    let mut payload = status.to_be_bytes().to_vec();
    payload.put_u32(0);
    payload
}

impl Server {
    fn lookup(&self, mut args: Bytes) -> Vec<u8> {
        let parent = take_opaque(&mut args);
        let name = take_opaque(&mut args);
        assert!(args.is_empty());
        let mut lookups = self.lookups.lock().unwrap();
        lookups.push((parent.clone(), name.clone()));
        match (parent.as_ref(), name.as_ref()) {
            (EXPORT, b"configured") => match self.next_root {
                _ if lookups.len() == 1 => lookup_ok(ORIGINAL, 2),
                RootReply::Same => lookup_ok(ORIGINAL, 2),
                RootReply::Replaced => lookup_ok(REPLACEMENT, 2),
                RootReply::Missing => lookup_error(2), // NFS3ERR_NOENT.
            },
            (ORIGINAL, b"leaf") if self.stale_leaf.swap(false, Ordering::AcqRel) => {
                lookup_error(70) // NFS3ERR_STALE.
            }
            (ORIGINAL, b"leaf") => lookup_ok(b"leaf", 1),
            (REPLACEMENT, b"leaf") => lookup_ok(b"unrelated-leaf", 1),
            (ORIGINAL, b"denied") => lookup_error(13), // NFS3ERR_ACCES.
            _ => panic!("unexpected LOOKUP {parent:?}/{name:?}"),
        }
    }

    async fn serve(&self, listener: TcpListener) -> io::Result<()> {
        let (mut stream, _) = listener.accept().await?;
        loop {
            let marker = stream.read_u32().await?;
            assert_eq!(marker >> 31, 1);
            let length = (marker & 0x7fff_ffff) as usize;
            assert!((24..=4096).contains(&length));
            let mut record = vec![0; length];
            stream.read_exact(&mut record).await?;
            let mut request = Bytes::from(record);
            let xid = request.get_u32();
            assert_eq!(request.get_u32(), 0); // CALL.
            assert_eq!(request.get_u32(), 2); // ONC RPC version.
            let program = request.get_u32();
            assert_eq!(request.get_u32(), 3); // Program version.
            let procedure = request.get_u32();
            for _ in 0..2 {
                request.advance(4); // Auth flavor.
                take_opaque(&mut request);
            }
            let payload = match (program, procedure) {
                (MOUNT_PROGRAM, 1) => {
                    let mut payload = 0u32.to_be_bytes().to_vec();
                    opaque(&mut payload, EXPORT);
                    payload.put_u32(0); // No listed auth flavors.
                    payload
                }
                (NFS_PROGRAM, 19) => {
                    let mut payload = Vec::new();
                    for word in [0, 0, 4096, 4096, 4096, 4096, 4096, 4096, 4096] {
                        payload.put_u32(word);
                    }
                    payload.put_u64(u64::MAX);
                    payload.extend_from_slice(&[0; 12]); // Time delta and properties.
                    payload
                }
                (ACL_PROGRAM, 0) | (MOUNT_PROGRAM, 3) => Vec::new(), // NFSACL NULL, MOUNT UMNT.
                (NFS_PROGRAM, 3) => self.lookup(request),
                _ => panic!("unexpected RPC program {program} procedure {procedure}"),
            };
            let mut reply = Vec::new();
            for word in [xid, 1, 0, 0, 0, 0] {
                reply.put_u32(word);
            }
            reply.extend_from_slice(&payload);
            stream
                .write_u32(0x8000_0000 | u32::try_from(reply.len()).unwrap())
                .await?;
            stream.write_all(&reply).await?;
            if (program, procedure) == (MOUNT_PROGRAM, 3) {
                return Ok(());
            }
        }
    }
}

#[tokio::test]
async fn stale_lookup_keeps_the_configured_root_binding() {
    for next_root in [RootReply::Same, RootReply::Replaced, RootReply::Missing] {
        timeout(Duration::from_secs(5), async {
            let (url, server, task) = start_server(next_root).await;
            let (storage, prefix) = NFSStorage::mount_and_build(&url, None, None).await.unwrap();
            let storage = storage.attach_root(prefix, false).await.unwrap();
            let clone = storage.clone();
            let scope = storage.get_root_fh();
            let cached = (PathBuf::from("cached"), scope.clone());
            let neighbor = (PathBuf::from("cached"), Bytes::from_static(b"other-scope"));
            NFSStorage::cache_directory(cached.clone(), &Bytes::from_static(b"cached"));
            NFSStorage::cache_directory(neighbor.clone(), &Bytes::from_static(b"neighbor"));

            let ordinary = storage.role_lookup_fh(Path::new("leaf")).await;
            let fresh = clone
                .role_lookup_with_attrs(&StoragePath::new("leaf").unwrap(), true)
                .await;
            match next_root {
                RootReply::Same => {
                    assert_eq!(
                        ordinary
                            .unwrap_or_else(|error| panic!("{:?}", error.class))
                            .fh,
                        b"leaf"[..]
                    );
                    assert_eq!(
                        fresh.unwrap_or_else(|error| panic!("{:?}", error.class)).0,
                        b"leaf"[..]
                    );
                    assert!(GLOBAL_CACHE.get(&cached).is_some());
                    assert_eq!(storage.refresh_generation.load(Ordering::Acquire), 1);
                    let lookup_count = server.lookups.lock().unwrap().len();
                    assert!(clone.maybe_refresh_root_fh(0).await.is_ok());
                    assert_eq!(server.lookups.lock().unwrap().len(), lookup_count);
                }
                RootReply::Replaced | RootReply::Missing => {
                    let (Err(ordinary), Err(fresh)) = (ordinary, fresh) else {
                        panic!("stale and fresh lookups must refuse a changed root");
                    };
                    server.stale_leaf.store(true, Ordering::Release);
                    let legacy = storage.lookup_fh(Path::new("leaf")).await.unwrap_err();
                    for error in [ordinary, fresh, classify_role_error(legacy)] {
                        assert_eq!(error.class, FailureClass::Conflict);
                        assert_eq!(error.transience, Transience::Permanent);
                        if matches!(next_root, RootReply::Missing) {
                            assert!(matches!(
                                error.cause.as_deref(),
                                Some(NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOENT))
                            ));
                        }
                    }
                    assert_eq!(storage.refresh_generation.load(Ordering::Acquire), 0);
                    assert!(GLOBAL_CACHE.get(&cached).is_none());
                    assert!(
                        server
                            .lookups
                            .lock()
                            .unwrap()
                            .iter()
                            .all(|(parent, _)| parent.as_ref() != REPLACEMENT)
                    );
                }
            }
            assert_eq!(storage.rpc_root_fh(), ORIGINAL);
            assert_eq!(clone.get_root_fh(), scope);
            assert!(GLOBAL_CACHE.get(&neighbor).is_some());
            GLOBAL_CACHE.invalidate(&neighbor);
            drop(clone);
            drop(storage);
            task.await.unwrap().unwrap();
        })
        .await
        .unwrap_or_else(|error| panic!("NFSv3 root refresh fixture timed out: {error}"));
    }
}

#[tokio::test]
async fn read_role_retains_sdk_open_refusal() {
    timeout(Duration::from_secs(5), async {
        let (url, _, task) = start_server(RootReply::Same).await;
        let storage = create_nfs_role_storage(&url, None, false, None)
            .await
            .unwrap();
        let role = storage.read_source().unwrap();
        let request = ReadRequest {
            path: StoragePath::new("denied").unwrap(),
            range: Some(0..1),
            expected_source: None,
            maximum_chunk_bytes: 1,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        };
        let Err(error @ StorageRoleFailure::Entry(_)) = role.read(request).await else {
            panic!("read must report the NFS opening refusal");
        };
        let sdk = std::iter::successors(error.source(), |&error| error.source())
            .find_map(|source| source.downcast_ref::<NfsError>());
        assert!(matches!(
            sdk,
            Some(NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_ACCES))
        ));
        let StorageRoleFailure::Entry(failure) = error else {
            unreachable!("the refusal is entry-scoped");
        };
        assert_eq!(failure.class(), FailureClass::PermissionDenied);
        drop(role);
        drop(storage);
        task.await.unwrap().unwrap();
    })
    .await
    .unwrap_or_else(|error| panic!("NFSv3 read fixture timed out: {error}"));
}
