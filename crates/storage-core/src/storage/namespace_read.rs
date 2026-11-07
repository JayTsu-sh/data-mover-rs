//! 有界、只读的路径确认；调用方决定是否据此修改任何存储。

use std::time::Duration;

use thiserror::Error;
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;

use super::{CapabilityUnavailable, SourceDescriptor, Storage, StorageRoleFailure};
use crate::model::{FailureClass, StoragePath, Transience};

/// 单次检测与识别故障后的恢复分别计时；不隐式采用扫描策略。
#[derive(Clone, Copy, Debug)]
pub struct NamespaceReadPolicy {
    pub operation_timeout: Duration,
    pub recovery_window: Duration,
    pub retry_interval: Duration,
}

#[derive(Clone, Debug, Error)]
pub enum NamespaceReadCause {
    #[error("namespace stat role failed: {0}; {diagnostic}", diagnostic = role_diagnostic(.0))]
    Role(#[from] StorageRoleFailure),
    #[error("namespace stat response deadline elapsed")]
    Timeout,
}

#[derive(Debug, Error)]
pub enum NamespaceReadFailure {
    #[error("namespace read requires positive operation timeout and retry interval")]
    InvalidPolicy,
    #[error(transparent)]
    Capability(#[from] CapabilityUnavailable),
    #[error("namespace stat cancelled at {path}; last failure: {last:?}")]
    Cancelled {
        path: StoragePath,
        last: Option<Box<NamespaceReadCause>>,
    },
    #[error(
        "namespace stat failed at {path} after {attempts} attempts: first={first}; last={last}"
    )]
    Operation {
        path: StoragePath,
        attempts: u32,
        first: Box<NamespaceReadCause>,
        #[source]
        last: Box<NamespaceReadCause>,
    },
}

/// `descriptor=None` 只表示本次路径 stat 的准确 NotFound，不证明历史、父目录或整棵树缺失。
#[derive(Debug)]
pub struct NamespaceReadOutcome {
    pub descriptor: Option<SourceDescriptor>,
    pub attempts: u32,
}

/// 只重试可安全重发的 stat；没有写入、后台任务、游标或 checkpoint。
/// 连接由已有 role/client 维护，本函数不把失效的 session 伪装为恢复成功。
///
/// # Errors
/// 保留首末有类型原因。权限／认证等明确错误不重试；首次故障后的单一截止时间
/// 约束全部退避与在途请求，取消可打断任意等待。
pub async fn read_namespace_entry(
    storage: &Storage,
    path: &StoragePath,
    policy: NamespaceReadPolicy,
    cancel: &CancellationToken,
) -> Result<NamespaceReadOutcome, NamespaceReadFailure> {
    if policy.operation_timeout.is_zero() || policy.retry_interval.is_zero() {
        return Err(NamespaceReadFailure::InvalidPolicy);
    }
    let namespace = storage.namespace()?;
    let mut recovery = ReadRecovery::default();
    loop {
        if cancel.is_cancelled() {
            return Err(recovery.cancelled(path));
        }
        let deadline = recovery.deadline(policy.operation_timeout);
        recovery.attempts = recovery.attempts.saturating_add(1);
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(recovery.cancelled(path)),
            result = timeout_at(deadline, namespace.stat(path)) => result,
        };
        let cause = match result {
            Ok(Ok(descriptor)) => return Ok(recovery.success(Some(descriptor))),
            Ok(Err(StorageRoleFailure::Entry(error)))
                if error.class() == FailureClass::NotFound =>
            {
                return Ok(recovery.success(None));
            }
            Ok(Err(error)) => NamespaceReadCause::Role(error),
            Err(_) => NamespaceReadCause::Timeout,
        };
        recovery.record(path, cause, policy.recovery_window);
        recovery.wait(path, policy.retry_interval, cancel).await?;
    }
}

#[cfg(test)]
#[path = "namespace_read_tests.rs"]
mod tests;

#[derive(Default)]
struct ReadRecovery {
    first: Option<NamespaceReadCause>,
    last: Option<NamespaceReadCause>,
    until: Option<Instant>,
    attempts: u32,
}

impl ReadRecovery {
    fn deadline(&self, limit: Duration) -> Instant {
        let operation = Instant::now() + limit;
        self.until.map_or(operation, |until| until.min(operation))
    }

    fn record(&mut self, path: &StoragePath, cause: NamespaceReadCause, window: Duration) {
        if self.first.is_none() {
            self.until = Some(Instant::now() + window);
            self.first = Some(cause.clone());
        }
        tracing::warn!(path = %path, attempt = self.attempts, cause = %cause, "namespace stat attempt failed");
        self.last = Some(cause);
    }

    fn success(&self, descriptor: Option<SourceDescriptor>) -> NamespaceReadOutcome {
        if self.attempts > 1 {
            tracing::info!(attempts = self.attempts, "namespace stat recovered");
        }
        NamespaceReadOutcome {
            descriptor,
            attempts: self.attempts,
        }
    }

    fn cancelled(&self, path: &StoragePath) -> NamespaceReadFailure {
        NamespaceReadFailure::Cancelled {
            path: path.clone(),
            last: self.last.clone().map(Box::new),
        }
    }

    fn failed(&self, path: &StoragePath) -> NamespaceReadFailure {
        // 只在 record 后调用；不将内部状态缺失转换为源端不存在。
        let last = self.last.clone().unwrap_or(NamespaceReadCause::Timeout);
        NamespaceReadFailure::Operation {
            path: path.clone(),
            attempts: self.attempts,
            first: Box::new(self.first.clone().unwrap_or_else(|| last.clone())),
            last: Box::new(last),
        }
    }

    async fn wait(
        &self,
        path: &StoragePath,
        interval: Duration,
        cancel: &CancellationToken,
    ) -> Result<(), NamespaceReadFailure> {
        let until = self.until.unwrap_or_else(Instant::now);
        if !self.last.as_ref().is_some_and(retryable) || Instant::now() >= until {
            return Err(self.failed(path));
        }
        let next = (Instant::now() + interval).min(until);
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(self.cancelled(path)),
            () = sleep_until(next) => {}
        }
        if Instant::now() >= until {
            return Err(self.failed(path));
        }
        Ok(())
    }
}

fn retryable(cause: &NamespaceReadCause) -> bool {
    let (class, transience) = match cause {
        NamespaceReadCause::Timeout => return true,
        NamespaceReadCause::Role(StorageRoleFailure::Entry(error)) => {
            (error.class(), error.transience())
        }
        NamespaceReadCause::Role(StorageRoleFailure::Session(error)) => {
            (error.class(), error.transience())
        }
    };
    transience != Transience::Permanent
        && matches!(
            class,
            FailureClass::Connectivity | FailureClass::Timeout | FailureClass::Throttled
        )
}

fn role_diagnostic(error: &StorageRoleFailure) -> &str {
    match error {
        StorageRoleFailure::Entry(error) => error.diagnostic(),
        StorageRoleFailure::Session(error) => error.diagnostic(),
    }
}
