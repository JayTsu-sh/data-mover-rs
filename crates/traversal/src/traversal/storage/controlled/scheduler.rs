//! The actor polls receipts/pages even while the one output bundle is backpressured.
use futures::{FutureExt as _, StreamExt as _};

use super::arena::{Key, Stage};
use super::publish::{Phase, Published, Target};
use super::{Acknowledged, Boundary, Runner};
use crate::traversal::{ListingOutcome, TraversalTerminalFailure};

impl Runner {
    pub(super) async fn walk(&mut self) -> Result<(), TraversalTerminalFailure> {
        self.root().await?;
        loop {
            if let Some(failure) = self.consumer.failure() {
                return Err(failure);
            }
            if self.consumer.stopped.is_cancelled() {
                return Ok(());
            }
            while let Some(Some(done)) = self.acknowledgements.next().now_or_never() {
                self.acknowledged(done)?;
            }
            if let Some(result) = self
                .sending
                .as_mut()
                .and_then(|future| future.as_mut().now_or_never())
            {
                self.sending = None;
                self.published(result?)?;
                continue;
            }
            if let Some(Some(page)) = self.reads.next().now_or_never() {
                self.read(page)?;
                continue;
            }
            if self.nodes.len() == 0 && self.sending.is_none() && self.continuation.is_none() {
                return Ok(());
            }
            if self.drive_one()? {
                continue;
            }
            if self.reads.is_empty() && self.acknowledgements.is_empty() && self.sending.is_none() {
                return Err(self
                    .blocked_error
                    .clone()
                    .unwrap_or(TraversalTerminalFailure::Internal));
            }
            tokio::select! {
                biased;
                ()=self.consumer.stopped.cancelled()=>return Ok(()),
                ()=self.consumer.failed()=>return Err(self.stopped()),
                Some(done)=self.acknowledgements.next(),if !self.acknowledgements.is_empty()=>self.acknowledged(done)?,
                result=async {match &mut self.sending {Some(future)=>future.await,None=>std::future::pending().await}}=>{
                    self.sending=None;self.published(result?)?;
                }
                Some(page)=self.reads.next(),if !self.reads.is_empty()=>self.read(page)?,
            }
        }
    }

    fn drive_one(&mut self) -> Result<bool, TraversalTerminalFailure> {
        if self.sending.is_none() && self.continuation.is_some() && self.continue_bundle()? {
            return Ok(true);
        }
        for _ in 0..self.nodes.ready_len() {
            let Some(key) = self.nodes.next() else {
                break;
            };
            let Some(live) = self.nodes.get(key) else {
                continue;
            };
            let progressed = match live.stage {
                Stage::Open => self.open(key)?,
                Stage::Ready => self.advance(key)?,
                Stage::Children if live.files == 0 && live.children == 0 && live.bundles == 0 => {
                    self.finish(key)?
                }
                Stage::Finalize => self.finalize(key)?,
                _ => false,
            };
            if progressed {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn advance(&mut self, key: Key) -> Result<bool, TraversalTerminalFailure> {
        let node = self
            .nodes
            .get(key)
            .and_then(|live| live.node.as_ref())
            .ok_or(TraversalTerminalFailure::Internal)?;
        if let Some(failure) = &node.page.terminal {
            return Err(failure.clone());
        }
        if !node.page.items.is_empty() {
            return self.start_bundle(key);
        }
        if !node.page.below.is_empty() {
            return self.hidden(key);
        }
        if node.page.statistics.is_some() {
            if self.output_busy() {
                self.nodes.queue(key, false);
                return Ok(false);
            }
            let item = self
                .nodes
                .get_mut(key)
                .and_then(|live| live.node.as_mut())
                .and_then(|node| node.page.statistics.take())
                .ok_or(TraversalTerminalFailure::Internal)?;
            self.nodes
                .get_mut(key)
                .ok_or(TraversalTerminalFailure::Internal)?
                .bundles += 1;
            self.send_item(item, Target::Continue(key));
            self.nodes.queue(key, false);
            return Ok(true);
        }
        let before = self.resources.available();
        let live = self
            .nodes
            .get_mut(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let node = live
            .node
            .as_mut()
            .ok_or(TraversalTerminalFailure::Internal)?;
        node.page = super::page::Prepared::default();
        let required = node.next_page_credit;
        let released = self.resources.available() > before;
        if released {
            self.nodes.wake_blocked();
        }
        if self.reads.len() >= self.read_limit {
            self.nodes.queue(key, false);
            return Ok(released);
        }
        let charge = match self.resources.reserve(required) {
            Ok(charge) => charge,
            // An ancestor may have reached physical EOF and be the only way to free a native
            // handle. Probe it without an estimated reservation; any actual page must still fit.
            Err(_) if self.listings >= self.listing_limit => self.resources.reserve(0)?,
            Err(error) => {
                self.park(key, error);
                return Ok(released);
            }
        };
        let live = self
            .nodes
            .get_mut(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let node = live.node.take().ok_or(TraversalTerminalFailure::Internal)?;
        live.stage = Stage::Reading;
        self.reads.push(super::io::page(
            key,
            node,
            self.shared.clone(),
            self.observations.clone(),
            self.consumer.clone(),
            super::resources::PageCharge::new(charge),
        ));
        Ok(true)
    }

    pub(super) fn read(&mut self, page: super::io::Read) -> Result<(), TraversalTerminalFailure> {
        let Some(live) = self.nodes.get_mut(page.key) else {
            return Ok(());
        };
        if live.node.is_some() || live.stage != Stage::Reading {
            return Err(TraversalTerminalFailure::Internal);
        }
        live.node = Some(page.node);
        page.result?;
        live.eof = page.eof;
        live.stage = if page.eof {
            self.listings = self
                .listings
                .checked_sub(1)
                .ok_or(TraversalTerminalFailure::Internal)?;
            Stage::Children
        } else {
            Stage::Ready
        };
        self.nodes.queue(page.key, page.eof);
        self.nodes.wake_blocked();
        Ok(())
    }

    pub(super) fn acknowledged(
        &mut self,
        (key, phase, result): Acknowledged,
    ) -> Result<(), TraversalTerminalFailure> {
        let Some(live) = self.nodes.get_mut(key) else {
            return Ok(());
        }; // Generation rejects stale receipts.
        match result {
            Ok(true) => {}
            Ok(false) => return Err(TraversalTerminalFailure::ConsumerFailed),
            Err(_) => return Err(self.stopped()),
        }
        match phase {
            Phase::Prepare => {
                live.prepares = live
                    .prepares
                    .checked_sub(1)
                    .ok_or(TraversalTerminalFailure::Internal)?;
                if live.prepares == 0 {
                    live.stage = if live.eof {
                        Stage::Children
                    } else {
                        Stage::Open
                    };
                    self.nodes.queue(key, true);
                }
            }
            Phase::File => {
                live.files = live
                    .files
                    .checked_sub(1)
                    .ok_or(TraversalTerminalFailure::Internal)?;
                self.nodes.queue(key, true);
            }
            Phase::Finalize => {
                self.complete(key)?;
            }
        }
        self.nodes.wake_blocked();
        Ok(())
    }

    fn published(&mut self, result: Published) -> Result<(), TraversalTerminalFailure> {
        if let Some(receipt) = result.receipt {
            let Target::Work(key, phase) = result.target else {
                return Err(TraversalTerminalFailure::Internal);
            };
            let charge = result
                .phase_charge
                .ok_or(TraversalTerminalFailure::Internal)?;
            self.acknowledgements.push(
                async move {
                    let done = receipt.await;
                    drop(charge);
                    (key, phase, done)
                }
                .boxed(),
            );
        }
        if let Some(remainder) = result.remainder {
            self.continuation = Some((result.target, remainder));
        } else {
            match result.target {
                Target::Continue(key) | Target::Work(key, Phase::File) => self.bundle_done(key)?,
                Target::Work(key, Phase::Prepare) => {
                    if let Some(parent) = self.nodes.get(key).and_then(|live| live.parent) {
                        self.bundle_done(parent)?;
                    }
                }
                Target::FinalStatistics(key) => {
                    if let Some(live) = self.nodes.get_mut(key) {
                        live.stage = Stage::Finalize;
                        self.nodes.queue(key, true);
                    }
                }
                Target::Work(_, Phase::Finalize) => {}
            }
        }
        self.nodes.wake_blocked();
        Ok(())
    }

    fn bundle_done(&mut self, key: Key) -> Result<(), TraversalTerminalFailure> {
        if let Some(live) = self.nodes.get_mut(key) {
            live.bundles = live
                .bundles
                .checked_sub(1)
                .ok_or(TraversalTerminalFailure::Internal)?;
            self.nodes.queue(key, false);
        }
        Ok(())
    }

    fn finish(&mut self, key: Key) -> Result<bool, TraversalTerminalFailure> {
        let live = self
            .nodes
            .get(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let node = live
            .node
            .as_ref()
            .ok_or(TraversalTerminalFailure::Internal)?;
        if node.statistics.is_some() {
            if self.output_busy() {
                self.nodes.queue(key, true);
                return Ok(false);
            }
            let bytes = std::mem::size_of::<crate::traversal::DirectoryStatistics>()
                .saturating_add(node.work.path.as_str().len());
            let charge = match self.resources.reserve(bytes) {
                Ok(charge) => charge,
                Err(error) => {
                    self.park(key, error);
                    return Ok(false);
                }
            };
            let live = self
                .nodes
                .get_mut(key)
                .ok_or(TraversalTerminalFailure::Internal)?;
            let node = live
                .node
                .as_mut()
                .ok_or(TraversalTerminalFailure::Internal)?;
            let mut statistics = node
                .statistics
                .take()
                .ok_or(TraversalTerminalFailure::Internal)?;
            let value = statistics.snapshot(&node.work.path, Some(ListingOutcome::Complete), false);
            live.stage = Stage::FinalStatistics;
            self.send_item(super::Held { value, charge }, Target::FinalStatistics(key));
        } else {
            self.nodes
                .get_mut(key)
                .ok_or(TraversalTerminalFailure::Internal)?
                .stage = Stage::Finalize;
            self.nodes.queue(key, true);
        }
        Ok(true)
    }

    pub(super) fn complete(&mut self, key: Key) -> Result<(), TraversalTerminalFailure> {
        let live = self
            .nodes
            .remove(key)
            .ok_or(TraversalTerminalFailure::Internal)?;
        let node = live.node.ok_or(TraversalTerminalFailure::Internal)?;
        if !live.eof || live.files != 0 || live.children != 0 || live.bundles != 0 {
            return Err(TraversalTerminalFailure::Internal);
        }
        debug_assert!(matches!(node.boundary, Boundary::PolicyBoundary));
        self.totals.absorb(&node.counts);
        debug_assert!(node.charge.bytes() >= super::arena::storage_bytes());
        if let Some(parent) = live.parent
            && let Some(live) = self.nodes.get_mut(parent)
        {
            live.children = live
                .children
                .checked_sub(1)
                .ok_or(TraversalTerminalFailure::Internal)?;
            self.nodes.queue(parent, true);
        }
        drop(node);
        self.nodes.wake_blocked();
        Ok(())
    }

    pub(super) fn output_busy(&self) -> bool {
        self.sending.is_some() || self.continuation.is_some()
    }
}
