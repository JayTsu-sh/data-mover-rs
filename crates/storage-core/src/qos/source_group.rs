//! 配置快照到原生源读取预算的编译与共享，生命周期由 `QoS` 管理器持有。

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use num_traits::ToPrimitive;
use thiserror::Error;

use super::{QosConfig, QosManager, parse_bandwidth_string};
use crate::error::StorageError;
use crate::storage::{SourceQosGroup, SourceQosPolicy, SourceQosValueError};

#[derive(Debug, Error)]
pub enum SourceQosSetupError {
    #[error("invalid source QoS configuration: {0}")]
    Configuration(#[from] StorageError),
    #[error("invalid source QoS policy: {0}")]
    Policy(#[from] SourceQosValueError),
}

#[derive(Debug, Default)]
pub(super) struct SourceGroups(Mutex<Option<(Arc<QosConfig>, SourceQosGroup)>>);

impl QosManager {
    /// 同一管理器及其 clone 的文件和归档共用一个读取预算。
    ///
    /// 热更新后的调用使用新预算；已打开的流保留原预算。
    /// 未配置 `max_io_bytes` 时，首次编译的 maximum 决定该快照的读取上限。
    /// 本方法不申请 IO permit，不创建后台任务。
    ///
    /// # Errors
    /// 配置无法解析或不能构成有效读取策略时保留其类型化原因。
    pub fn source_group(&self, maximum: u64) -> Result<SourceQosGroup, SourceQosSetupError> {
        // 锁仅保护同步编译；即使持锁者 panic，已有完整预算仍可安全复用。
        let mut cached = self
            .source_groups
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // 快照必须在锁内读取，防止延迟的旧调用覆盖新配置的预算。
        let config = self.config();
        if let Some((owner, group)) = cached.as_ref()
            && Arc::ptr_eq(owner, &config)
        {
            return Ok(group.clone());
        }
        let group = SourceQosGroup::new(compile_policy(&config, maximum)?);
        *cached = Some((config, group.clone()));
        Ok(group)
    }
}

fn compile_policy(
    config: &QosConfig,
    maximum: u64,
) -> Result<SourceQosPolicy, SourceQosSetupError> {
    let bandwidth = config
        .bandwidth
        .as_deref()
        .map(parse_bandwidth_string)
        .transpose()?;
    let hard = config
        .hard_bandwidth
        .as_deref()
        .map(parse_bandwidth_string)
        .transpose()?;
    Ok(SourceQosPolicy::new(
        bandwidth.map(|soft| {
            (
                soft,
                hard.unwrap_or_else(|| {
                    (soft.to_f64().unwrap_or(f64::MAX) * f64::from(config.peak_rate))
                        .max(0.0)
                        .to_u64()
                        .unwrap_or(u64::MAX)
                })
                .max(soft),
                config.peak_duration.unwrap_or(Duration::ZERO),
            )
        }),
        config.max_io_bytes.unwrap_or(maximum).max(1),
        config.iops.map(|soft| {
            (
                soft,
                config.hard_iops.unwrap_or(soft).max(soft),
                config.iops_peak_duration.unwrap_or(Duration::ZERO),
            )
        }),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use tokio::runtime::Handle;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test(start_paused = true)]
    async fn manager_clones_share_admission_budget_across_files() -> TestResult {
        let manager = QosManager::try_new(None, 1.0, Some(1))?;
        let first = manager.source_group(8)?.transfer_budget();
        let second = manager.clone().source_group(1024)?.transfer_budget();
        assert_eq!(second.read_size(1024), 8);
        let start = Instant::now();
        let cancel = CancellationToken::new();
        let (first_result, second_result) =
            tokio::join!(first.admit_read(1, &cancel), second.admit_read(1, &cancel));
        assert!(first_result.is_ok() && second_result.is_ok());
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        assert_eq!(first.stats().source_read_operations, 1);
        assert_eq!(second.stats().source_read_operations, 1);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn update_replaces_future_budget_and_keeps_existing_streams() -> TestResult {
        let manager = QosManager::try_new(None, 1.0, Some(1))?;
        let old = manager.source_group(8)?.transfer_budget();
        manager.update_iops(2)?;
        let new = manager.clone().source_group(16)?.transfer_budget();
        assert_eq!(old.read_size(32), 8);
        assert_eq!(new.read_size(32), 16);
        let cancel = CancellationToken::new();
        let start = Instant::now();
        assert!(new.admit_read(1, &cancel).await.is_ok());
        assert_eq!(start.elapsed(), Duration::from_millis(500));
        assert!(old.admit_read(1, &cancel).await.is_ok());
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_calls_after_update_compile_one_shared_budget() -> TestResult {
        let manager = QosManager::try_new(None, 1.0, Some(1))?;
        let _old_stream = manager.source_group(8)?.transfer_budget();
        manager.update_iops(2)?;
        let mut threads = Vec::new();
        for _ in 0..8 {
            let manager = manager.clone();
            let runtime = Handle::current();
            threads.push(std::thread::spawn(move || {
                let _guard = runtime.enter();
                manager.source_group(16)
            }));
        }
        let mut groups = Vec::new();
        for thread in threads {
            groups.push(
                thread
                    .join()
                    .map_err(|_| "source group thread panicked")??,
            );
        }
        let cancel = CancellationToken::new();
        let start = Instant::now();
        let admissions = groups
            .iter()
            .map(|group| async { group.transfer_budget().admit_read(1, &cancel).await });
        assert!(join_all(admissions).await.iter().all(Result::is_ok));
        assert_eq!(start.elapsed(), Duration::from_secs(4));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn independent_managers_do_not_share_budget_and_cancel_does_not_charge() -> TestResult {
        let one = QosManager::try_new(None, 1.0, Some(1))?;
        let two = QosManager::try_new(None, 1.0, Some(1))?;
        let first = one.source_group(8)?.transfer_budget();
        let second = two.source_group(8)?.transfer_budget();
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(first.admit_read(1, &cancel).await.is_err());
        assert_eq!(first.stats().source_read_operations, 0);
        let start = Instant::now();
        let cancel = CancellationToken::new();
        let (a, b) = tokio::join!(first.admit_read(1, &cancel), second.admit_read(1, &cancel));
        assert!(a.is_ok() && b.is_ok());
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        Ok(())
    }
}
