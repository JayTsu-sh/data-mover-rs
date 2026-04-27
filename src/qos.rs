// 标准库
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// 外部crate
use arc_swap::ArcSwap;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use tracing::debug;

// 内部模块
use crate::error::{Result, StorageError};

/// Governor 内部的 `RateLimiter` 类型别名
type DirectRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// 带宽 limiter（1 cell = 1 KB）
type BandwidthLimiter = DirectRateLimiter;
/// IOPS limiter（1 cell = 1 op）
type IopsLimiter = DirectRateLimiter;

/// `QoS` 统计信息，使用原子计数器避免锁竞争
#[derive(Debug)]
pub struct QosStats {
    /// 累计已传输字节数
    pub total_bytes: AtomicU64,
    /// 累计 IO 操作数
    pub total_iops: AtomicU64,
    /// 统计起始时间
    pub start_time: Instant,
}

impl QosStats {
    fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            total_iops: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// 获取当前实际带宽 (MiB/s)
    pub fn actual_bandwidth_mibps(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.total_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0) / elapsed
        } else {
            0.0
        }
    }

    /// 获取当前实际 IOPS
    pub fn actual_iops(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.total_iops.load(Ordering::Relaxed) as f64 / elapsed
        } else {
            0.0
        }
    }
}

/// `QoS` 配置快照
#[derive(Debug, Clone)]
pub struct QosConfig {
    /// 带宽限制字符串（如 "200MiB/s"）
    pub bandwidth: Option<String>,
    /// 峰值速率倍数
    pub peak_rate: f32,
    /// IOPS 限制
    pub iops: Option<u32>,
}

/// `QoS` 管理器
///
/// 封装带宽和 IOPS 两个 Governor `RateLimiter`，支持 `ArcSwap` 热更新。
/// `Clone + Send + Sync`，可直接 clone 传递，无需外层 Mutex。
#[derive(Clone, Debug)]
pub struct QosManager {
    bandwidth_limiter: Option<Arc<ArcSwap<BandwidthLimiter>>>,
    iops_limiter: Option<Arc<ArcSwap<IopsLimiter>>>,
    stats: Arc<QosStats>,
    config: Arc<ArcSwap<QosConfig>>,
}

/// 将字节数转换为 cells（1 cell = 1 KB），最小为 1
fn bytes_to_cells(bytes: u64) -> NonZeroU32 {
    let cells = u32::try_from(bytes.div_ceil(1024).max(1)).unwrap_or(u32::MAX);
    // cells 至少为 1，NonZeroU32::new 不会返回 None
    NonZeroU32::new(cells).unwrap_or(NonZeroU32::MIN)
}

/// 构建带宽 limiter
/// 1 cell = 1 KB，速率 = `base_rate_bps` / 1024 cells/sec
/// burst = `base_rate` * `peak_rate` / 1024 cells
const MIN_PEAK_RATE_BPS: u64 = 2 * 1024 * 1024; // 2MB/s

/// 用基准速率 + 显式 burst（字节数）构建一个 governor `BandwidthLimiter`。
///
/// 这是带宽 limiter 的"内核"——`build_bandwidth_limiter` 通过 `peak_rate`
/// 算出 burst 后调本函数，`build_bandwidth_limiter_with_burst` 直接传 burst。
fn build_bandwidth_limiter_inner(base_rate_bps: u64, burst_bytes: u64) -> Result<BandwidthLimiter> {
    if burst_bytes < MIN_PEAK_RATE_BPS {
        debug!(
            "[QoS] burst 容量较小 ({burst_bytes} B)，对于大于 burst 的单次 IO 会被分批限流；\
             如这是有意为之（严格平均速率），可忽略。"
        );
    }

    // 基准速率换算为 cells/sec（1 cell = 1 KB）
    let cells_per_sec = (base_rate_bps / 1024).max(1);
    let rate = NonZeroU32::new(cells_per_sec as u32)
        .ok_or_else(|| StorageError::ConfigError("带宽速率过小，换算后为0 cells/sec".to_string()))?;

    // burst 容量（cells）
    let burst_cells = (burst_bytes / 1024).max(1);
    let burst = NonZeroU32::new(burst_cells as u32)
        .ok_or_else(|| StorageError::ConfigError("burst 过小，换算后为 0 cells".to_string()))?;

    debug!(
        "[QoS] 带宽限制: base={}B/s ({}cells/s), burst={}B ({}cells)",
        base_rate_bps, cells_per_sec, burst_bytes, burst_cells
    );

    let quota = Quota::per_second(rate).allow_burst(burst);
    Ok(RateLimiter::direct(quota))
}

fn build_bandwidth_limiter(bandwidth_str: &str, peak_rate: f32) -> Result<BandwidthLimiter> {
    let base_rate_bps = parse_bandwidth_string(bandwidth_str)?;
    let burst_bytes = (base_rate_bps as f64 * f64::from(peak_rate)).round() as u64;
    build_bandwidth_limiter_inner(base_rate_bps, burst_bytes)
}

/// 显式 burst 版本：用基准速率字符串 + burst 字节数。
/// 仅供 `try_new_with_burst` / `update_bandwidth_with_burst` 内部使用。
fn build_bandwidth_limiter_with_burst(bandwidth_str: &str, burst_bytes: u64) -> Result<BandwidthLimiter> {
    let base_rate_bps = parse_bandwidth_string(bandwidth_str)?;
    build_bandwidth_limiter_inner(base_rate_bps, burst_bytes)
}

/// 构建 IOPS limiter（1 cell = 1 op）
fn build_iops_limiter(iops: u32) -> Result<IopsLimiter> {
    let rate = NonZeroU32::new(iops).ok_or_else(|| StorageError::ConfigError("IOPS 值必须大于 0".to_string()))?;

    // burst 允许 IOPS 的 10% 或至少 10 个操作的突发
    let burst_ops = (iops / 10).max(10).min(iops);
    let burst =
        NonZeroU32::new(burst_ops).ok_or_else(|| StorageError::ConfigError("IOPS burst 计算异常".to_string()))?;

    debug!("[QoS] IOPS 限制: rate={} ops/s, burst={} ops", iops, burst_ops);

    let quota = Quota::per_second(rate).allow_burst(burst);
    Ok(RateLimiter::direct(quota))
}

impl QosManager {
    /// 创建新的 `QoS` 管理器（基于 `peak_rate` 倍数）
    ///
    /// - `bandwidth`: 带宽限制字符串，如 "200MiB/s"，None 则不限速
    /// - `peak_rate`: 峰值速率倍数（相对于基准速率）。**这同时决定了
    ///   token bucket 的 burst 容量**：`burst = base_rate × peak_rate × 1秒`。
    ///   `peak_rate = 1.0` 意味着允许 1 秒带宽量的瞬时突发（典型场景下足够，
    ///   小文件可能在突发窗口内瞬时穿过）。如需"严格平均速率，无突发"，请用
    ///   [`try_new_with_burst`](Self::try_new_with_burst) 指定一个小 burst
    ///   （例如等于一次 IO 的 chunk 大小）。
    /// - `iops`: IOPS 限制，None 则不限制
    pub fn try_new(bandwidth: Option<&str>, peak_rate: f32, iops: Option<u32>) -> Result<Self> {
        let bandwidth_limiter = match bandwidth {
            Some(bw) => {
                let limiter = build_bandwidth_limiter(bw, peak_rate)?;
                Some(Arc::new(ArcSwap::from_pointee(limiter)))
            }
            None => None,
        };

        let iops_limiter = match iops {
            Some(iops_val) if iops_val > 0 => {
                let limiter = build_iops_limiter(iops_val)?;
                Some(Arc::new(ArcSwap::from_pointee(limiter)))
            }
            _ => None,
        };

        let config = QosConfig {
            bandwidth: bandwidth.map(std::string::ToString::to_string),
            peak_rate,
            iops,
        };

        Ok(Self {
            bandwidth_limiter,
            iops_limiter,
            stats: Arc::new(QosStats::new()),
            config: Arc::new(ArcSwap::from_pointee(config)),
        })
    }

    /// 创建新的 `QoS` 管理器（显式指定 burst 字节数）
    ///
    /// 适合需要 **严格平均速率** 的场景，例如：
    /// - 嵌入到长稳 daemon 中（HSM copytool / 后台同步），不希望小文件被
    ///   1 秒带宽量的 burst 一次性"瞬时穿过"造成共享带宽尖刺
    /// - 自动测试需要可预测的 wall-clock 行为
    ///
    /// - `bandwidth`: 带宽限制字符串，如 "200MiB/s"
    /// - `burst_bytes`: token bucket 的 burst 上限（字节）。建议设置为
    ///   一次 IO 的 chunk 大小（如 4 MiB），这样 burst 只能容纳一个 chunk，
    ///   后续 chunks 严格按 `bandwidth` 速率推进
    /// - `iops`: IOPS 限制，None 则不限制
    ///
    /// # 示例
    ///
    /// ```ignore
    /// // 严格 8 MiB/s，最多 1 MiB 的瞬时突发：
    /// // 16 MiB 文件至少需要 ~1.875s 完成（首块 1 MiB 立即过，剩余 15 MiB 走 8 MiB/s）。
    /// let qos = QosManager::try_new_with_burst("8MiB/s", 1024 * 1024, None)?;
    /// ```
    pub fn try_new_with_burst(bandwidth: &str, burst_bytes: u64, iops: Option<u32>) -> Result<Self> {
        let limiter = build_bandwidth_limiter_with_burst(bandwidth, burst_bytes)?;
        let bandwidth_limiter = Some(Arc::new(ArcSwap::from_pointee(limiter)));

        let iops_limiter = match iops {
            Some(iops_val) if iops_val > 0 => {
                let l = build_iops_limiter(iops_val)?;
                Some(Arc::new(ArcSwap::from_pointee(l)))
            }
            _ => None,
        };

        // 把 burst 等价 peak_rate 写回 config 快照，便于外部检视
        let base_rate_bps = parse_bandwidth_string(bandwidth)?;
        let derived_peak_rate = if base_rate_bps == 0 {
            1.0
        } else {
            (burst_bytes as f64 / base_rate_bps as f64) as f32
        };
        let config = QosConfig {
            bandwidth: Some(bandwidth.to_string()),
            peak_rate: derived_peak_rate,
            iops,
        };

        Ok(Self {
            bandwidth_limiter,
            iops_limiter,
            stats: Arc::new(QosStats::new()),
            config: Arc::new(ArcSwap::from_pointee(config)),
        })
    }

    /// 获取带宽限流（异步等待直到令牌可用）
    ///
    /// 每个 `DataChunk` 在读取前调用，bytes 为 chunk 大小
    pub async fn acquire_bandwidth(&self, bytes: u64) {
        if let Some(bw) = &self.bandwidth_limiter {
            let limiter = bw.load();
            let cells = bytes_to_cells(bytes);

            // Governor 的 until_n_ready 对 NonZeroU32 有上限检查
            // 如果 cells > burst 容量，需要分批 acquire
            let cells_u32 = cells.get();
            let mut remaining = cells_u32;

            while remaining > 0 {
                // 每次最多申请 burst 容量大小（避免超过 InsufficientCapacity）
                // 使用一个保守的分块策略：每次最多 4096 cells (= 4 MiB)
                let batch = remaining.min(4096);
                if let Some(n) = NonZeroU32::new(batch) {
                    let _ = limiter.until_n_ready(n).await;
                }
                remaining -= batch;
            }

            // 更新统计
            self.stats.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// 获取 IOPS 限流（每个 IO 操作前调用）
    pub async fn acquire_iops(&self) {
        if let Some(iops) = &self.iops_limiter {
            let limiter = iops.load();
            limiter.until_ready().await;

            // 更新统计
            self.stats.total_iops.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 同时获取带宽和 IOPS 限流
    ///
    /// 在 `read_data` 循环内，每个 chunk 前调用此方法
    pub async fn acquire(&self, bytes: u64) {
        // 并行执行两个限流（任一个阻塞时不影响另一个的计时）
        tokio::join!(self.acquire_bandwidth(bytes), self.acquire_iops());
    }

    /// 热更新带宽限制（迁移任务不中断）
    pub fn update_bandwidth(&self, new_rate: &str, peak_rate: f32) -> Result<()> {
        let new_limiter = build_bandwidth_limiter(new_rate, peak_rate)?;
        if let Some(bw) = &self.bandwidth_limiter {
            bw.store(Arc::new(new_limiter));
        }
        // 更新配置快照
        let mut new_config = (**self.config.load()).clone();
        new_config.bandwidth = Some(new_rate.to_string());
        new_config.peak_rate = peak_rate;
        self.config.store(Arc::new(new_config));
        Ok(())
    }

    /// 热更新 IOPS 限制
    pub fn update_iops(&self, new_iops: u32) -> Result<()> {
        let new_limiter = build_iops_limiter(new_iops)?;
        if let Some(iops) = &self.iops_limiter {
            iops.store(Arc::new(new_limiter));
        }
        // 更新配置快照
        let mut new_config = (**self.config.load()).clone();
        new_config.iops = Some(new_iops);
        self.config.store(Arc::new(new_config));
        Ok(())
    }

    /// 获取 `QoS` 统计信息
    pub fn stats(&self) -> &QosStats {
        &self.stats
    }

    /// 获取当前配置
    pub fn config(&self) -> Arc<QosConfig> {
        self.config.load_full()
    }

    /// 是否启用了任何 `QoS` 限制
    pub fn is_enabled(&self) -> bool {
        self.bandwidth_limiter.is_some() || self.iops_limiter.is_some()
    }

    /// 不需要显式 shutdown — Governor 无后台任务，Drop 时自动清理
    pub fn shutdown(&self) {
        let stats = &self.stats;
        let elapsed = stats.start_time.elapsed();
        debug!(
            "[QoS] Shutdown: 累计传输 {} bytes ({:.2} MiB/s), {} IO ops ({:.0} IOPS), 运行 {:.1}s",
            stats.total_bytes.load(Ordering::Relaxed),
            stats.actual_bandwidth_mibps(),
            stats.total_iops.load(Ordering::Relaxed),
            stats.actual_iops(),
            elapsed.as_secs_f64()
        );
    }
}

/// 将带宽字符串按份数均分，返回新的带宽字符串（单位 B/s）
///
/// # 参数
/// - `bandwidth`: 带宽限制字符串（如 "2GiB/s"），None 则返回 None
/// - `divisor`: 均分份数（如 worker 数量）
///
/// # 返回值
/// - 均分后的带宽字符串（如 "536870912b/s"），或 None
pub fn divide_bandwidth(bandwidth: &Option<String>, divisor: usize) -> Option<String> {
    let bw_str = bandwidth.as_ref()?;
    let total_bps = parse_bandwidth_string(bw_str).ok()?;
    let per_worker_bps = total_bps / divisor.max(1) as u64;
    Some(format!("{per_worker_bps}b/s"))
}

// 解析带宽字符串，支持格式如"1GiB/s"或"200MiB/s"，大小写不敏感，数字和单位之间可带空格
pub fn parse_bandwidth_string(bandwidth: &str) -> Result<u64> {
    // 去除字符串中的空格，转为小写以便统一处理
    let bandwidth = bandwidth.replace(' ', "").to_lowercase();

    // 定义支持的单位及其对应的字节数
    let units = [
        ("gib/s", 1024 * 1024 * 1024), // gibibytes per second
        ("gib", 1024 * 1024 * 1024),   // gibibytes (隐含每秒)
        ("gb/s", 1024 * 1024 * 1024),  // gigabytes per second
        ("gb", 1024 * 1024 * 1024),    // gigabytes (隐含每秒)
        ("mib/s", 1024 * 1024),        // mebibytes per second
        ("mib", 1024 * 1024),          // mebibytes (隐含每秒)
        ("mb/s", 1024 * 1024),         // megabytes per second
        ("mb", 1024 * 1024),           // megabytes (隐含每秒)
        ("kib/s", 1024),               // kibibytes per second
        ("kib", 1024),                 // kibibytes (隐含每秒)
        ("kb/s", 1024),                // kilobytes per second
        ("kb", 1024),                  // kilobytes (隐含每秒)
        ("b/s", 1),                    // bytes per second
        ("b", 1),                      // bytes (隐含每秒)
    ];

    // 尝试匹配每个单位
    for (unit, multiplier) in &units {
        if bandwidth.ends_with(unit) {
            // 提取数字部分
            let number_str = &bandwidth[0..bandwidth.len() - unit.len()];
            // 确保数字部分不为空
            if number_str.is_empty() {
                continue;
            }
            // 解析数字
            let Ok(number) = number_str.parse::<f64>() else {
                continue; // 尝试下一个单位
            };

            // 计算最终的bps值
            let bytes_per_second = (number * f64::from(*multiplier)).round() as u64;
            return Ok(bytes_per_second);
        }
    }

    // 如果没有匹配到任何单位，尝试直接解析为数字（假设单位为字节/秒）
    if let Ok(bytes_per_second) = bandwidth.parse::<u64>() {
        return Ok(bytes_per_second);
    }

    Err(StorageError::ConfigError(
        "无效的带宽格式，请使用如'1GiB/s'或'200MiB/s'的格式".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_bandwidth_string_parsing() {
        // 测试各种带宽字符串格式的解析
        let test_cases = [
            ("1GiB/s", 1024 * 1024 * 1024),
            ("200MiB/s", 200 * 1024 * 1024),
            ("1 GIB/s", 1024 * 1024 * 1024),
            ("200 mib/s", 200 * 1024 * 1024),
            ("1GB/s", 1024 * 1024 * 1024),
            ("200MB/s", 200 * 1024 * 1024),
            ("1GiB", 1024 * 1024 * 1024),
            ("200MiB", 200 * 1024 * 1024),
            ("1000000000", 1000000000),
            ("1024", 1024),
        ];

        for (input, expected) in test_cases.iter() {
            let result = parse_bandwidth_string(input).unwrap();
            assert_eq!(result, *expected, "解析'{}'失败", input);
        }

        // 测试错误情况
        let invalid_cases = ["invalid", "123XYZ", "abc GiB/s"];

        for input in invalid_cases.iter() {
            let result = parse_bandwidth_string(input);
            assert!(result.is_err(), "解析无效字符串'{}'应该失败", input);
        }
    }

    #[test]
    fn test_bytes_to_cells() {
        assert_eq!(bytes_to_cells(0).get(), 1); // 最小 1 cell
        assert_eq!(bytes_to_cells(1).get(), 1); // 1 byte -> 1 cell (1 KB)
        assert_eq!(bytes_to_cells(1024).get(), 1); // 1024 bytes -> 1 cell
        assert_eq!(bytes_to_cells(1025).get(), 2); // 1025 bytes -> 2 cells
        assert_eq!(bytes_to_cells(2 * 1024 * 1024).get(), 2048); // 2 MiB -> 2048 cells
    }

    #[tokio::test]
    async fn test_qos_manager_bandwidth_only() {
        // 测试仅带宽限制的 QosManager
        let qos = QosManager::try_new(Some("10MiB/s"), 2.0, None).unwrap();
        assert!(qos.is_enabled());

        // 执行几次 acquire，确保不会 panic 或死锁
        for _ in 0..5 {
            qos.acquire_bandwidth(1024).await; // 1 KB
        }

        assert!(qos.stats().total_bytes.load(Ordering::Relaxed) >= 5 * 1024);
        qos.shutdown();
    }

    #[tokio::test]
    async fn test_qos_manager_iops_only() {
        // 测试仅 IOPS 限制的 QosManager
        let qos = QosManager::try_new(None, 1.0, Some(1000)).unwrap();
        assert!(qos.is_enabled());

        // 执行几次 acquire_iops
        for _ in 0..5 {
            qos.acquire_iops().await;
        }

        assert_eq!(qos.stats().total_iops.load(Ordering::Relaxed), 5);
        qos.shutdown();
    }

    #[tokio::test]
    async fn test_qos_manager_both() {
        // 测试同时启用带宽和 IOPS 限制
        let qos = QosManager::try_new(Some("100MiB/s"), 2.0, Some(5000)).unwrap();
        assert!(qos.is_enabled());

        // 使用 acquire 同时限流
        for _ in 0..3 {
            qos.acquire(2 * 1024 * 1024).await; // 2 MiB
        }

        assert!(qos.stats().total_bytes.load(Ordering::Relaxed) >= 3 * 2 * 1024 * 1024);
        assert_eq!(qos.stats().total_iops.load(Ordering::Relaxed), 3);
        qos.shutdown();
    }

    #[tokio::test]
    async fn test_qos_manager_disabled() {
        // 测试不启用任何 QoS
        let qos = QosManager::try_new(None, 1.0, None).unwrap();
        assert!(!qos.is_enabled());

        // acquire 应该立即返回
        qos.acquire(1024 * 1024).await;
        qos.acquire_iops().await;

        // 无 limiter 时不计入统计
        assert_eq!(qos.stats().total_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(qos.stats().total_iops.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_qos_manager_clone() {
        // 测试 clone 后共享状态
        let qos = QosManager::try_new(Some("100MiB/s"), 2.0, Some(1000)).unwrap();
        let qos2 = qos.clone();

        qos.acquire_bandwidth(1024).await;
        qos2.acquire_bandwidth(1024).await;

        // 两个 clone 共享统计
        assert!(qos.stats().total_bytes.load(Ordering::Relaxed) >= 2048);
        assert!(qos2.stats().total_bytes.load(Ordering::Relaxed) >= 2048);
    }

    #[tokio::test]
    async fn test_qos_manager_hot_update() {
        // 测试热更新
        let qos = QosManager::try_new(Some("100MiB/s"), 2.0, Some(1000)).unwrap();

        // 更新带宽
        qos.update_bandwidth("200MiB/s", 3.0).unwrap();
        let config = qos.config();
        assert_eq!(config.bandwidth.as_deref(), Some("200MiB/s"));
        assert_eq!(config.peak_rate, 3.0);

        // 更新 IOPS
        qos.update_iops(2000).unwrap();
        let config = qos.config();
        assert_eq!(config.iops, Some(2000));

        // 更新后仍能正常 acquire
        qos.acquire(1024).await;
        qos.acquire_iops().await;
    }

    #[tokio::test]
    async fn test_rate_limiting_effectiveness() {
        // 测试限速是否真正生效
        // 设置很低的速率：10 KB/s，然后尝试传输 50 KB
        let qos = QosManager::try_new(Some("10KiB/s"), 1.0, None).unwrap();

        let start = Instant::now();

        // 传输 50 KB (每次 10 KB，共 5 次)
        for _ in 0..5 {
            qos.acquire_bandwidth(10 * 1024).await;
        }

        let elapsed = start.elapsed();
        // 以 10 KB/s 传输 50 KB 应该至少需要几秒
        // burst=1 意味着第一次 acquire 可能立即通过，但后续需要等待
        assert!(
            elapsed >= Duration::from_millis(500),
            "限速应该生效，实际耗时 {:?}",
            elapsed
        );

        qos.shutdown();
    }

    #[test]
    fn test_invalid_qos_config() {
        // 测试无效配置
        let result = QosManager::try_new(Some("invalid"), 1.0, None);
        assert!(result.is_err());
    }

    /// `try_new` 配 `peak_rate=1.0` 时，burst = 1 秒带宽，对小于该 burst 的传输
    /// 不会有节流效果——这是 governor token bucket 的固有行为。本测试 **记录**
    /// 这个行为，证明它不是 bug，而是 burst 容量决定的设计选择。
    #[tokio::test]
    async fn test_try_new_default_burst_bursty_behavior() {
        let qos = QosManager::try_new(Some("8MiB/s"), 1.0, None).unwrap();
        let start = Instant::now();
        // 整 8 MiB 一次性 acquire — 全在 burst 窗口内，应当瞬时完成
        qos.acquire_bandwidth(8 * 1024 * 1024).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(200),
            "8 MiB ≤ burst 应当立即返回（实测 {elapsed:?}）"
        );
        qos.shutdown();
    }

    /// `try_new_with_burst` 显式设小 burst 后，应当严格按平均速率推进。
    /// 8 MiB/s + 1 MiB burst → 8 MiB 总量需要 ≥ 7 × 0.125 s = 0.875 s
    /// （首块 1 MiB 立即过，剩 7 块每块等 0.125 s 重生）。
    #[tokio::test]
    async fn test_try_new_with_burst_enforces_average_rate() {
        let qos = QosManager::try_new_with_burst("8MiB/s", 1024 * 1024, None).unwrap();
        let start = Instant::now();
        for _ in 0..8 {
            qos.acquire_bandwidth(1024 * 1024).await;
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(700),
            "8 MiB at 8 MiB/s with 1 MiB burst should take ≥ 700 ms, got {elapsed:?}"
        );
        // 上限：合理实现不应远超目标平均速率所需时间
        assert!(
            elapsed < Duration::from_millis(2500),
            "应当接近 0.875s，最多 2.5s（实测 {elapsed:?}）"
        );

        // config 快照里 peak_rate 应当 = burst / base_rate = 1 MiB / 8 MiB = 0.125
        let cfg = qos.config();
        assert!((cfg.peak_rate - 0.125).abs() < 0.001);
        qos.shutdown();
    }

    /// `try_new_with_burst` 不带 IOPS 限制时也能正常工作。
    #[tokio::test]
    async fn test_try_new_with_burst_iops_optional() {
        let qos = QosManager::try_new_with_burst("100MiB/s", 4 * 1024 * 1024, Some(500)).unwrap();
        qos.acquire(64 * 1024).await;
        let cfg = qos.config();
        assert_eq!(cfg.iops, Some(500));
        qos.shutdown();
    }

    /// 非法配置（带宽串解析失败）应当返回错误。
    #[test]
    fn test_try_new_with_burst_invalid_bandwidth() {
        let res = QosManager::try_new_with_burst("not-a-rate", 1024, None);
        assert!(res.is_err());
    }
}
