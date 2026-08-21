// 标准库
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// 外部crate
use arc_swap::ArcSwap;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use num_traits::ToPrimitive;
use tracing::debug;

// 内部模块
use crate::error::{Result, StorageError};

/// Governor 内部的 `RateLimiter` 类型别名
type DirectRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// 带宽 limiter（cell 大小按配置速率自适应）
type BandwidthLimiter = DirectRateLimiter;
/// IOPS limiter（1 cell = 1 op）
type IopsLimiter = DirectRateLimiter;

const PEAK_SHAPING_WINDOW: Duration = Duration::from_millis(10);

#[derive(Debug)]
struct BandwidthController {
    sustained: BandwidthLimiter,
    cell_bytes: u64,
    burst_cells: u32,
    max_io_bytes: u64,
    peak_bytes_per_second: u64,
}

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
            let bytes = self
                .total_bytes
                .load(Ordering::Relaxed)
                .to_f64()
                .unwrap_or(f64::MAX);
            bytes / (1024.0 * 1024.0) / elapsed
        } else {
            0.0
        }
    }

    /// 获取当前实际 IOPS
    pub fn actual_iops(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let operations = self
                .total_iops
                .load(Ordering::Relaxed)
                .to_f64()
                .unwrap_or(f64::MAX);
            operations / elapsed
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
    /// 硬峰值速率倍数
    pub peak_rate: f32,
    /// sustained token bucket 容量，也是非零时的单次源端 IO 上限
    pub burst_bytes: Option<u64>,
    /// IOPS 限制
    pub iops: Option<u32>,
}

/// `QoS` 管理器
///
/// 封装带宽和 IOPS 两个 Governor `RateLimiter`，支持 `ArcSwap` 热更新。
/// `Clone + Send + Sync`，可直接 clone 传递，无需外层 Mutex。
#[derive(Clone, Debug)]
pub struct QosManager {
    bandwidth_limiter: Option<Arc<ArcSwap<BandwidthController>>>,
    iops_limiter: Option<Arc<ArcSwap<IopsLimiter>>>,
    peak_next_at: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
    stats: Arc<QosStats>,
    config: Arc<ArcSwap<QosConfig>>,
}

/// 将字节数按 limiter 的自适应粒度转换为 cells，最小为 1。
fn bytes_to_cells(bytes: u64, cell_bytes: u64) -> NonZeroU32 {
    let cells = u32::try_from(bytes.div_ceil(cell_bytes).max(1)).unwrap_or(u32::MAX);
    // cells 至少为 1，NonZeroU32::new 不会返回 None
    NonZeroU32::new(cells).unwrap_or(NonZeroU32::MIN)
}

/// 构建持续带宽 token bucket + 硬峰值 pacer。
const MIN_PEAK_RATE_BPS: u64 = 2 * 1024 * 1024; // 2MB/s

/// 用基准速率 + 显式 burst（字节数）构建一个 governor `BandwidthLimiter`。
///
/// 这是带宽 limiter 的"内核"——`build_bandwidth_limiter` 通过 `peak_rate`
/// 算出 burst 后调本函数，`build_bandwidth_limiter_with_burst` 直接传 burst。
fn build_bandwidth_limiter_inner(
    base_rate_bps: u64,
    peak_rate: f32,
    burst_bytes: u64,
) -> Result<BandwidthController> {
    if burst_bytes == 0 {
        return Err(StorageError::ConfigError(
            "burst_bytes 必须大于 0".to_string(),
        ));
    }
    if burst_bytes < MIN_PEAK_RATE_BPS {
        debug!(
            "[QoS] burst 容量较小 ({burst_bytes} B)，对于大于 burst 的单次 IO 会被分批限流；\
             如这是有意为之（严格平均速率），可忽略。"
        );
    }

    // 常规速率使用 1 byte/cell；仅当速率超过 governor 的 u32 cell 上限时
    // 扩大 cell，避免极小 burst 被固定 1 KiB 粒度严重过度计费。
    let cell_bytes = base_rate_bps
        .max(burst_bytes)
        .div_ceil(u64::from(u32::MAX))
        .max(1);
    let cells_per_sec = base_rate_bps.div_ceil(cell_bytes).max(1);
    let rate =
        NonZeroU32::new(u32::try_from(cells_per_sec).map_err(|_| {
            StorageError::ConfigError("带宽速率超过 limiter 支持的范围".to_string())
        })?)
        .ok_or_else(|| {
            StorageError::ConfigError("带宽速率过小，换算后为0 cells/sec".to_string())
        })?;

    // burst 容量（cells）
    let burst_cells = burst_bytes.div_ceil(cell_bytes).max(1);
    let burst_cells_u32 = u32::try_from(burst_cells)
        .map_err(|_| StorageError::ConfigError("burst 超过 limiter 支持的范围".to_string()))?;
    let burst = NonZeroU32::new(burst_cells_u32)
        .ok_or_else(|| StorageError::ConfigError("burst 过小，换算后为 0 cells".to_string()))?;

    debug!(
        "[QoS] 带宽限制: base={}B/s ({}cells/s), burst={}B ({}cells)",
        base_rate_bps, cells_per_sec, burst_bytes, burst_cells
    );

    let peak_bytes_per_second = checked_rounded_u64(
        base_rate_bps
            .to_f64()
            .ok_or_else(|| StorageError::ConfigError("带宽速率无法转换为浮点数".to_string()))?
            * f64::from(peak_rate),
        "peak bandwidth",
    )?;
    if peak_bytes_per_second == 0 {
        return Err(StorageError::ConfigError(
            "peak bandwidth 必须大于 0".to_string(),
        ));
    }
    let peak_quantum = u64::try_from(
        u128::from(peak_bytes_per_second)
            .saturating_mul(PEAK_SHAPING_WINDOW.as_nanos())
            .div_ceil(1_000_000_000),
    )
    .unwrap_or(u64::MAX)
    .max(1);
    let max_io_bytes = peak_quantum.min(burst_bytes);
    let sustained = RateLimiter::direct(Quota::per_second(rate).allow_burst(burst));
    Ok(BandwidthController {
        sustained,
        cell_bytes,
        burst_cells: burst_cells_u32,
        max_io_bytes,
        peak_bytes_per_second,
    })
}

fn build_bandwidth_limiter(bandwidth_str: &str, peak_rate: f32) -> Result<BandwidthController> {
    let base_rate_bps = parse_bandwidth_string(bandwidth_str)?;
    let base_rate = base_rate_bps
        .to_f64()
        .ok_or_else(|| StorageError::ConfigError("带宽速率无法转换为浮点数".to_string()))?;
    let burst_bytes = checked_rounded_u64(base_rate * f64::from(peak_rate), "burst")?;
    build_bandwidth_limiter_inner(base_rate_bps, peak_rate, burst_bytes)
}

fn checked_rounded_u64(value: f64, field: &str) -> Result<u64> {
    if !value.is_finite() || value.is_sign_negative() {
        return Err(StorageError::ConfigError(format!(
            "{field} 必须是 0..={} 范围内的有限数值",
            u64::MAX
        )));
    }
    value
        .round()
        .to_u64()
        .ok_or_else(|| StorageError::ConfigError(format!("{field} 超过 u64 支持的范围")))
}

/// 显式 burst 版本：用基准速率字符串 + burst 字节数。
/// 仅供 `try_new_with_burst` 内部使用。
fn build_bandwidth_limiter_with_burst(
    bandwidth_str: &str,
    burst_bytes: u64,
) -> Result<BandwidthController> {
    let base_rate_bps = parse_bandwidth_string(bandwidth_str)?;
    build_bandwidth_limiter_inner(base_rate_bps, 1.0, burst_bytes)
}

/// 构建 IOPS limiter（1 cell = 1 op）
fn build_iops_limiter(iops: u32) -> Result<IopsLimiter> {
    let rate = NonZeroU32::new(iops)
        .ok_or_else(|| StorageError::ConfigError("IOPS 值必须大于 0".to_string()))?;

    // 硬 IOPS 上限不预留多操作 burst；所有 clone 共享同一个 limiter。
    let burst_ops = 1;
    let burst = NonZeroU32::new(burst_ops)
        .ok_or_else(|| StorageError::ConfigError("IOPS burst 计算异常".to_string()))?;

    debug!(
        "[QoS] IOPS 限制: rate={} ops/s, burst={} ops",
        iops, burst_ops
    );

    let quota = Quota::per_second(rate).allow_burst(burst);
    Ok(RateLimiter::direct(quota))
}

impl QosManager {
    /// 创建新的 `QoS` 管理器（基于 `peak_rate` 倍数）
    ///
    /// - `bandwidth`: 带宽限制字符串，如 "200MiB/s"，None 则不限速
    /// - `peak_rate`: 相对基准速率的硬峰值倍数。持续 token bucket 容量为
    ///   `base_rate × peak_rate × 1秒`，但实际源端 IO 仍按 10 ms 量子以
    ///   `base_rate × peak_rate` pacing，不允许初始 bucket 造成瞬时穿透。
    /// - `iops`: IOPS 限制，None 则不限制
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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

        let configured_burst = bandwidth.and_then(|bw| {
            let base = parse_bandwidth_string(bw).ok()?.to_f64()?;
            checked_rounded_u64(base * f64::from(peak_rate), "burst").ok()
        });
        let config = QosConfig {
            bandwidth: bandwidth.map(std::string::ToString::to_string),
            peak_rate,
            burst_bytes: configured_burst,
            iops,
        };

        Ok(Self {
            bandwidth_limiter,
            iops_limiter,
            peak_next_at: Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now())),
            stats: Arc::new(QosStats::new()),
            config: Arc::new(ArcSwap::from_pointee(config)),
        })
    }

    /// 创建新的 `QoS` 管理器（显式指定 burst 字节数）
    ///
    /// 适合需要显式限制单次源端 IO 大小的严格整形场景，例如：
    /// - 嵌入到长稳 daemon 中（HSM copytool / 后台同步），不希望小文件被
    ///   大 storage chunk 造成共享带宽尖刺
    /// - 自动测试需要可预测的 wall-clock 行为
    ///
    /// - `bandwidth`: 带宽限制字符串，如 "200MiB/s"
    /// - `burst_bytes`: token bucket 容量，同时作为单次源端 IO 上限。
    ///   小于 storage chunk 时自动拆分；必须大于 0。
    /// - `iops`: IOPS 限制，None 则不限制
    ///
    /// # 示例
    ///
    /// ```ignore
    /// // 严格 8 MiB/s，单次源端 IO 不超过 1 MiB：
    /// // 16 MiB 文件至少需要约 2s 完成。
    /// let qos = QosManager::try_new_with_burst("8MiB/s", 1024 * 1024, None)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub fn try_new_with_burst(
        bandwidth: &str,
        burst_bytes: u64,
        iops: Option<u32>,
    ) -> Result<Self> {
        let limiter = build_bandwidth_limiter_with_burst(bandwidth, burst_bytes)?;
        let bandwidth_limiter = Some(Arc::new(ArcSwap::from_pointee(limiter)));

        let iops_limiter = match iops {
            Some(iops_val) if iops_val > 0 => {
                let l = build_iops_limiter(iops_val)?;
                Some(Arc::new(ArcSwap::from_pointee(l)))
            }
            _ => None,
        };

        let config = QosConfig {
            bandwidth: Some(bandwidth.to_string()),
            peak_rate: 1.0,
            burst_bytes: Some(burst_bytes),
            iops,
        };

        Ok(Self {
            bandwidth_limiter,
            iops_limiter,
            peak_next_at: Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now())),
            stats: Arc::new(QosStats::new()),
            config: Arc::new(ArcSwap::from_pointee(config)),
        })
    }

    /// 获取带宽限流（异步等待直到令牌可用）
    ///
    /// 每个 `DataChunk` 在读取前调用，bytes 为 chunk 大小
    pub async fn acquire_bandwidth(&self, bytes: u64) {
        let mut remaining = bytes;
        while remaining > 0 {
            let granted = self.acquire_bandwidth_grant(remaining).await;
            if granted == 0 {
                break;
            }
            remaining -= granted;
        }
    }

    /// 获取一个受持续带宽、硬峰值和 burst 共同约束的数据分片大小。
    ///
    /// 本方法只计算字节流量，不消费 IOPS；协议 adapter 应在发起真实请求前
    /// 单独调用 [`Self::acquire_iops`]。
    pub async fn acquire_bandwidth_grant(&self, requested_bytes: u64) -> u64 {
        let Some(bw) = &self.bandwidth_limiter else {
            return requested_bytes;
        };
        if requested_bytes == 0 {
            return 0;
        }
        let controller = bw.load_full();
        let granted = requested_bytes.min(controller.max_io_bytes);
        let mut remaining_cells = bytes_to_cells(granted, controller.cell_bytes).get();
        while remaining_cells > 0 {
            let batch = remaining_cells.min(controller.burst_cells);
            if let Some(cells) = NonZeroU32::new(batch) {
                controller
                    .sustained
                    .until_n_ready(cells)
                    .await
                    .unwrap_or_else(|_| unreachable!("batch is bounded by limiter burst"));
            }
            remaining_cells -= batch;
        }

        let nanos = u128::from(granted)
            .saturating_mul(1_000_000_000)
            .div_ceil(u128::from(controller.peak_bytes_per_second));
        let pacing = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
        let deadline = {
            let mut next_at = self.peak_next_at.lock().await;
            let now = tokio::time::Instant::now();
            let start = (*next_at).max(now);
            let deadline = start + pacing;
            *next_at = deadline;
            deadline
        };
        tokio::time::sleep_until(deadline).await;
        self.stats.total_bytes.fetch_add(granted, Ordering::Relaxed);
        granted
    }

    /// 为“一次 bandwidth grant 对应一次协议请求”的 backend 获取组合 permit。
    /// 流式协议（如 S3 `ByteStream`）应分别调用
    /// [`Self::acquire_iops`] 和 [`Self::acquire_bandwidth_grant`]。
    pub async fn acquire_io(&self, requested_bytes: u64) -> u64 {
        let granted = self.acquire_bandwidth_grant(requested_bytes).await;
        if granted > 0 {
            self.acquire_iops().await;
        }
        granted
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
        self.acquire_bandwidth(bytes).await;
        self.acquire_iops().await;
    }

    /// 热更新带宽限制（迁移任务不中断）
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub fn update_bandwidth(&self, new_rate: &str, peak_rate: f32) -> Result<()> {
        let new_limiter = build_bandwidth_limiter(new_rate, peak_rate)?;
        if let Some(bw) = &self.bandwidth_limiter {
            bw.store(Arc::new(new_limiter));
        }
        // 更新配置快照
        let mut new_config = (**self.config.load()).clone();
        new_config.bandwidth = Some(new_rate.to_string());
        new_config.peak_rate = peak_rate;
        let base = parse_bandwidth_string(new_rate)?;
        new_config.burst_bytes = Some(checked_rounded_u64(
            base.to_f64()
                .ok_or_else(|| StorageError::ConfigError("带宽速率无法转换为浮点数".to_string()))?
                * f64::from(peak_rate),
            "burst",
        )?);
        self.config.store(Arc::new(new_config));
        Ok(())
    }

    /// 热更新 IOPS 限制
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
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
    #[must_use]
    pub fn stats(&self) -> &QosStats {
        &self.stats
    }

    /// 获取当前配置
    #[must_use]
    pub fn config(&self) -> Arc<QosConfig> {
        self.config.load_full()
    }

    /// 是否启用了任何 `QoS` 限制
    #[must_use]
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
#[must_use]
pub fn divide_bandwidth(bandwidth: &Option<String>, divisor: usize) -> Option<String> {
    let bw_str = bandwidth.as_ref()?;
    let total_bps = parse_bandwidth_string(bw_str).ok()?;
    let per_worker_bps = total_bps / divisor.max(1) as u64;
    Some(format!("{per_worker_bps}b/s"))
}

// 解析带宽字符串，支持格式如"1GiB/s"或"200MiB/s"，大小写不敏感，数字和单位之间可带空格
///
/// # Errors
///
/// Returns an error when the requested storage operation cannot be completed.
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
            let bytes_per_second =
                checked_rounded_u64(number * f64::from(*multiplier), "bandwidth")?;
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
    use crate::AssertTestValue;

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
            ("1000000000", 1_000_000_000),
            ("1024", 1024),
        ];

        for (input, expected) in &test_cases {
            let result = parse_bandwidth_string(input).assert_value("test value should be present");
            assert_eq!(result, *expected, "解析'{input}'失败");
        }

        // 测试错误情况
        let invalid_cases = ["invalid", "123XYZ", "abc GiB/s"];

        for input in &invalid_cases {
            let result = parse_bandwidth_string(input);
            assert!(result.is_err(), "解析无效字符串'{input}'应该失败");
        }
    }

    #[test]
    fn bandwidth_rejects_negative_and_non_finite_values() {
        assert!(parse_bandwidth_string("-1MiB/s").is_err());
        assert!(parse_bandwidth_string("NaNMiB/s").is_err());
        assert!(parse_bandwidth_string("infMiB/s").is_err());
    }

    #[test]
    fn test_bytes_to_cells() {
        assert_eq!(bytes_to_cells(0, 1).get(), 1);
        assert_eq!(bytes_to_cells(1, 1).get(), 1);
        assert_eq!(bytes_to_cells(1024, 1).get(), 1024);
        assert_eq!(bytes_to_cells(1025, 1024).get(), 2);
        assert_eq!(bytes_to_cells(2 * 1024 * 1024, 1024).get(), 2048);
    }

    #[tokio::test]
    async fn test_qos_manager_bandwidth_only() {
        // 测试仅带宽限制的 QosManager
        let qos = QosManager::try_new(Some("10MiB/s"), 2.0, None)
            .assert_value("test value should be present");
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
        let qos =
            QosManager::try_new(None, 1.0, Some(1000)).assert_value("test value should be present");
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
        let qos = QosManager::try_new(Some("100MiB/s"), 2.0, Some(5000))
            .assert_value("test value should be present");
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
        let qos = QosManager::try_new(None, 1.0, None).assert_value("test value should be present");
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
        let qos = QosManager::try_new(Some("100MiB/s"), 2.0, Some(1000))
            .assert_value("test value should be present");
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
        let qos = QosManager::try_new(Some("100MiB/s"), 2.0, Some(1000))
            .assert_value("test value should be present");

        // 更新带宽
        qos.update_bandwidth("200MiB/s", 3.0)
            .assert_value("test value should be present");
        let config = qos.config();
        assert_eq!(config.bandwidth.as_deref(), Some("200MiB/s"));
        assert!((config.peak_rate - 3.0).abs() < f32::EPSILON);

        // 更新 IOPS
        qos.update_iops(2000)
            .assert_value("test value should be present");
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
        let qos = QosManager::try_new(Some("10KiB/s"), 1.0, None)
            .assert_value("test value should be present");

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
            "限速应该生效，实际耗时 {elapsed:?}"
        );

        qos.shutdown();
    }

    #[test]
    fn test_invalid_qos_config() {
        // 测试无效配置
        let result = QosManager::try_new(Some("invalid"), 1.0, None);
        assert!(result.is_err());
    }

    /// 即使初始 sustained bucket 中有完整 burst，硬峰值 pacer 也必须阻止小文件瞬时穿透。
    #[tokio::test]
    async fn default_burst_still_obeys_hard_peak_bandwidth() {
        let qos = QosManager::try_new(Some("8MiB/s"), 1.0, None)
            .assert_value("test value should be present");
        let start = Instant::now();
        qos.acquire_bandwidth(8 * 1024 * 1024).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900),
            "8 MiB at a hard 8 MiB/s peak must take about one second; got {elapsed:?}"
        );
        qos.shutdown();
    }

    /// 显式小 burst 负责 IO 拆分，但不能作为初始免等待额度绕过硬峰值。
    #[tokio::test]
    async fn test_try_new_with_burst_enforces_average_rate() {
        let qos = QosManager::try_new_with_burst("8MiB/s", 1024 * 1024, None)
            .assert_value("test value should be present");
        let start = Instant::now();
        for _ in 0..8 {
            qos.acquire_bandwidth(1024 * 1024).await;
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900),
            "8 MiB at a hard 8 MiB/s peak should take about one second, got {elapsed:?}"
        );
        // 上限：合理实现不应远超目标平均速率所需时间
        assert!(
            elapsed < Duration::from_millis(2500),
            "应当接近 1s，最多 2.5s（实测 {elapsed:?}）"
        );

        let cfg = qos.config();
        assert!((cfg.peak_rate - 1.0).abs() < f32::EPSILON);
        assert_eq!(cfg.burst_bytes, Some(1024 * 1024));
        qos.shutdown();
    }

    /// `try_new_with_burst` 不带 IOPS 限制时也能正常工作。
    #[tokio::test]
    async fn test_try_new_with_burst_iops_optional() {
        let qos = QosManager::try_new_with_burst("100MiB/s", 4 * 1024 * 1024, Some(500))
            .assert_value("test value should be present");
        qos.acquire(64 * 1024).await;
        let cfg = qos.config();
        assert_eq!(cfg.iops, Some(500));
        qos.shutdown();
    }

    /// 小于 storage chunk 的非零 burst 必须成为单次实际 IO 的上限；
    /// permit 在返回前还必须按硬峰值速率预付该子块的时间，不能初始穿透。
    #[tokio::test]
    async fn small_burst_limits_each_io_grant_and_paces_the_first_grant() {
        let qos = QosManager::try_new_with_burst("1MiB/s", 4 * 1024, None)
            .assert_value("create strict qos");
        let started = Instant::now();

        let granted = qos.acquire_io(128 * 1024).await;
        let elapsed = started.elapsed();

        assert_eq!(granted, 4 * 1024);
        assert!(
            elapsed >= Duration::from_millis(3),
            "4 KiB at 1 MiB/s must be paced before IO; got {elapsed:?}"
        );
    }

    #[test]
    fn zero_burst_is_rejected() {
        let result = QosManager::try_new_with_burst("1MiB/s", 0, None);
        assert!(matches!(result, Err(StorageError::ConfigError(_))));
    }

    #[tokio::test]
    async fn iops_limit_has_no_multi_operation_initial_burst() {
        let qos = QosManager::try_new(None, 1.0, Some(100)).assert_value("create IOPS qos");
        let started = Instant::now();
        for _ in 0..5 {
            assert_eq!(qos.acquire_io(4096).await, 4096);
        }
        assert!(
            started.elapsed() >= Duration::from_millis(35),
            "five operations at 100 IOPS must span about 40 ms"
        );
    }

    #[tokio::test]
    async fn cloned_managers_share_the_same_iops_schedule() {
        let qos = QosManager::try_new(None, 1.0, Some(100)).assert_value("create IOPS qos");
        let cloned = qos.clone();
        let started = Instant::now();
        let left = tokio::spawn(async move {
            qos.acquire_io(4096).await;
            qos.acquire_io(4096).await;
        });
        let right = tokio::spawn(async move {
            cloned.acquire_io(4096).await;
            cloned.acquire_io(4096).await;
        });
        left.await.assert_value("join left");
        right.await.assert_value("join right");
        assert!(
            started.elapsed() >= Duration::from_millis(25),
            "four shared operations at 100 IOPS must span about 30 ms"
        );
    }

    /// 非法配置（带宽串解析失败）应当返回错误。
    #[test]
    fn test_try_new_with_burst_invalid_bandwidth() {
        let res = QosManager::try_new_with_burst("not-a-rate", 1024, None);
        assert!(res.is_err());
    }
}
