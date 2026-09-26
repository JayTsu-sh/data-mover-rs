//! Test doubles for the CIFS directory seam.

use std::sync::{Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::{self, BoxStream};

use super::namespace::{CifsDirectory, CifsListedChild};

pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A directory handing out children read beforehand, for fakes that list whole directories.
pub(super) struct MemoryDirectory {
    children: Mutex<Option<Vec<CifsListedChild>>>,
}

impl MemoryDirectory {
    pub(super) fn boxed(children: Vec<CifsListedChild>) -> Box<dyn CifsDirectory> {
        Box::new(Self {
            children: Mutex::new(Some(children)),
        })
    }
}

#[async_trait]
impl CifsDirectory for MemoryDirectory {
    fn entries(&self) -> BoxStream<'_, smb_domain::Result<CifsListedChild>> {
        let children = locked(&self.children).take().unwrap_or_default();
        stream::iter(children.into_iter().map(Ok)).boxed()
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        Ok(())
    }
}
