//! 恢复生产方累计事实；watch 可合并通知，不能用通知次数推算累计数。
use std::sync::{Mutex, PoisonError};

/// 当前 recoverable traversal 执行的累计数，与状态通知是否被及时消费无关。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TraversalRecoveryCounters {
    pub network_faults: u64,
    /// 真正发起的新连接请求数，不含取消前的防抖等待。
    pub reconnect_attempts: u64,
    /// 原故障操作验证成功的恢复窗口数；排空旧连接后的接管不再计数。
    pub successful_recoveries: u64,
}

#[derive(Default)]
pub(super) struct RecoveryCounters(Mutex<TraversalRecoveryCounters>);

impl RecoveryCounters {
    // 仅故障/重连冷路径修改；一次锁读取完整三元组，避免跨字段混入不同进度。
    fn update(&self, action: impl FnOnce(&mut TraversalRecoveryCounters)) {
        action(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner));
    }
    pub(super) fn fault(&self) {
        self.update(|value| value.network_faults = value.network_faults.saturating_add(1));
    }
    pub(super) fn attempt(&self) {
        self.update(|value| value.reconnect_attempts = value.reconnect_attempts.saturating_add(1));
    }
    pub(super) fn recovered(&self) {
        self.update(|value| {
            value.successful_recoveries = value.successful_recoveries.saturating_add(1);
        });
    }
    pub(super) fn snapshot(&self) -> TraversalRecoveryCounters {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unobserved_attempts_remain_in_the_cumulative_snapshot() {
        let counters = RecoveryCounters::default();
        counters.fault();
        counters.attempt();
        counters.attempt();
        counters.recovered();
        assert_eq!(
            counters.snapshot(),
            TraversalRecoveryCounters {
                network_faults: 1,
                reconnect_attempts: 2,
                successful_recoveries: 1,
            }
        );
        counters.fault();
        counters.attempt();
        assert_eq!(
            counters.snapshot(),
            TraversalRecoveryCounters {
                network_faults: 2,
                reconnect_attempts: 3,
                successful_recoveries: 1,
            }
        );
    }
}
