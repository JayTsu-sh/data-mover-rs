//! 有界、只读的路径确认；调用方决定是否据此修改任何存储。

use std::{future::Future, sync::Arc, time::Duration};

use thiserror::Error;
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;

use super::{
    CapabilityUnavailable, EntryObservationFailure, SourceDescriptor, Storage, StorageRoleFailure,
    observe_descriptor,
};
use crate::model::{
    FailureClass, MetadataFamily, MetadataObservation, MetadataObservationFailure, ObservationMode,
    ObservationPlan, ObservedEntry, StoragePath, Transience,
};

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
    #[error("source observation failed: {0}; {diagnostic}", diagnostic = observation_diagnostic(.0))]
    Observation(Arc<EntryObservationFailure>),
    #[error("required source metadata was not observed: {0:?}")]
    MissingMetadata(Vec<MetadataObservationFailure>),
    #[error("source observation response deadline elapsed")]
    ObservationTimeout,
}

#[derive(Debug, Error)]
pub enum NamespaceReadFailure {
    #[error("source read requires positive operation timeout and retry interval")]
    InvalidPolicy,
    #[error(transparent)]
    Capability(#[from] CapabilityUnavailable),
    #[error("source read cancelled at {path}; last failure: {last:?}")]
    Cancelled {
        path: StoragePath,
        last: Option<Box<NamespaceReadCause>>,
    },
    #[error("source read failed at {path} after {attempts} attempts: first={first}; last={last}")]
    Operation {
        path: StoragePath,
        attempts: u32,
        first: Box<NamespaceReadCause>,
        #[source]
        last: Box<NamespaceReadCause>,
    },
}

/// None 只表示初始 stat 的准确 NotFound，不证明历史、父目录或整棵树缺失。
/// 返回分支由调用方是否提供观察计划决定。
#[derive(Debug)]
pub enum NamespaceReadOutcome {
    Descriptor {
        descriptor: Option<SourceDescriptor>,
        attempts: u32,
    },
    Observation {
        entry: Option<ObservedEntry>,
        attempts: u32,
    },
}

/// 读取当前路径；plan=None 时只执行 stat，不借用元数据角色或读取链接。
/// 提供计划时在同一检测／恢复窗口内捕获路径和所需属性，不读取文件数据或历史。
/// Required 属性缺失或失败不会变成成功观察，BestEffort 保留失败事实。
/// 只有初始 stat 的 `NotFound` 表示缺失，后续属性或链接读取的 `NotFound` 明确失败。
/// 不发出写入或后台任务；调用方自行决定是否授权清理。
///
/// # Errors
/// 保留首末有类型原因；权限／认证等明确错误不重试，恢复不重置整个窗口。
/// 取消可打断在途请求和退避；无效策略或缺少 namespace 角色在 IO 前拒绝。
pub async fn read_namespace_entry(
    storage: &Storage,
    path: &StoragePath,
    plan: Option<ObservationPlan>,
    policy: NamespaceReadPolicy,
    cancel: &CancellationToken,
) -> Result<NamespaceReadOutcome, NamespaceReadFailure> {
    if policy.operation_timeout.is_zero() || policy.retry_interval.is_zero() {
        return Err(NamespaceReadFailure::InvalidPolicy);
    }
    let namespace = storage.namespace()?;
    if let Some(plan) = plan {
        let (entry, attempts) = read_recovered(
            path,
            policy,
            cancel,
            || observe_current(storage, path, plan),
            NamespaceReadCause::ObservationTimeout,
        )
        .await?;
        Ok(NamespaceReadOutcome::Observation { entry, attempts })
    } else {
        let (descriptor, attempts) = read_recovered(
            path,
            policy,
            cancel,
            || async {
                match namespace.stat(path).await {
                    Ok(descriptor) => Ok(Some(descriptor)),
                    Err(StorageRoleFailure::Entry(error))
                        if error.class() == FailureClass::NotFound =>
                    {
                        Ok(None)
                    }
                    Err(error) => Err(NamespaceReadCause::Role(error)),
                }
            },
            NamespaceReadCause::Timeout,
        )
        .await?;
        Ok(NamespaceReadOutcome::Descriptor {
            descriptor,
            attempts,
        })
    }
}

async fn observe_current(
    storage: &Storage,
    path: &StoragePath,
    plan: ObservationPlan,
) -> Result<Option<ObservedEntry>, NamespaceReadCause> {
    let namespace = storage
        .namespace()
        .map_err(|error| NamespaceReadCause::Observation(Arc::new(error.into())))?;
    let descriptor = match namespace.stat(path).await {
        Ok(descriptor) => descriptor,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(NamespaceReadCause::Role(error)),
    };
    let entry = observe_descriptor(storage, descriptor, plan)
        .await
        .map_err(|error| NamespaceReadCause::Observation(Arc::new(error)))?;
    let facts = entry.metadata();
    let failures: Vec<_> = [
        required_failure(facts.acl(), MetadataFamily::Acl, plan.acl()),
        required_failure(facts.xattrs(), MetadataFamily::Xattrs, plan.xattrs()),
        required_failure(facts.tags(), MetadataFamily::Tags, plan.tags()),
        required_failure(
            facts.ownership_mode(),
            MetadataFamily::OwnershipMode,
            plan.ownership_mode(),
        ),
        required_failure(
            facts.timestamps(),
            MetadataFamily::Timestamps,
            plan.timestamps(),
        ),
    ]
    .into_iter()
    .flatten()
    .collect();
    if failures.is_empty() {
        Ok(Some(entry))
    } else {
        Err(NamespaceReadCause::MissingMetadata(failures))
    }
}

fn required_failure<T>(
    fact: &MetadataObservation<T>,
    family: MetadataFamily,
    mode: ObservationMode,
) -> Option<MetadataObservationFailure> {
    if mode != ObservationMode::Required {
        return None;
    }
    let (class, transience) = match fact {
        MetadataObservation::Failed { class, transience } => (*class, *transience),
        MetadataObservation::Unsupported => (FailureClass::Unsupported, Transience::Permanent),
        MetadataObservation::NotRequested => (FailureClass::InvalidInput, Transience::Permanent),
        _ => return None,
    };
    Some(MetadataObservationFailure {
        family,
        class,
        transience,
    })
}

async fn read_recovered<T, F, Fut>(
    path: &StoragePath,
    policy: NamespaceReadPolicy,
    cancel: &CancellationToken,
    mut operation: F,
    timeout_cause: NamespaceReadCause,
) -> Result<(T, u32), NamespaceReadFailure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, NamespaceReadCause>>,
{
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
            result = timeout_at(deadline, operation()) => result,
        };
        let cause = match result {
            Ok(Ok(value)) => return Ok(recovery.success(value)),
            Ok(Err(cause)) => cause,
            Err(_) => timeout_cause.clone(),
        };
        recovery.record(path, cause, policy.recovery_window);
        recovery.wait(path, policy.retry_interval, cancel).await?;
    }
}

#[cfg(test)]
#[path = "source_observation_read_tests.rs"]
mod source_observation_tests;
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
        tracing::warn!(path = %path, attempt = self.attempts, cause = %cause, "source read attempt failed");
        self.last = Some(cause);
    }

    fn success<T>(&self, value: T) -> (T, u32) {
        if self.attempts > 1 {
            tracing::info!(attempts = self.attempts, "source read recovered");
        }
        (value, self.attempts)
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
    match cause {
        NamespaceReadCause::Timeout | NamespaceReadCause::ObservationTimeout => true,
        NamespaceReadCause::Role(error) => role_retryable(error),
        NamespaceReadCause::Observation(error) => match error.as_ref() {
            EntryObservationFailure::Role(error) => role_retryable(error),
            _ => false,
        },
        NamespaceReadCause::MissingMetadata(failures) => {
            !failures.is_empty()
                && failures
                    .iter()
                    .all(|failure| transient_read(failure.class, failure.transience))
        }
    }
}

fn role_retryable(error: &StorageRoleFailure) -> bool {
    let (class, transience) = match error {
        StorageRoleFailure::Entry(error) => (error.class(), error.transience()),
        StorageRoleFailure::Session(error) => (error.class(), error.transience()),
    };
    transient_read(class, transience)
}

fn transient_read(class: FailureClass, transience: Transience) -> bool {
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

fn observation_diagnostic(error: &EntryObservationFailure) -> &str {
    match error {
        EntryObservationFailure::Role(error) => role_diagnostic(error),
        _ => "source observation could not be captured",
    }
}
