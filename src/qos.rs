// 标准库
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

const PEAK_SHAPING_WINDOW: Duration = Duration::from_millis(10);
type DirectRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

#[derive(Debug)]
struct BandwidthController {
    soft_bytes_per_second: u64,
    hard_bytes_per_second: u64,
    credit_capacity_bytes: u64,
    max_io_bytes: u64,
    state: tokio::sync::Mutex<BandwidthState>,
}

#[derive(Debug)]
struct BandwidthState {
    credit_bytes: f64,
    credit_updated_at: tokio::time::Instant,
    last_soft_release_at: tokio::time::Instant,
    last_release_at: tokio::time::Instant,
}

#[derive(Debug)]
struct IopsController {
    soft_iops: u32,
    credit_capacity: f64,
    hard_limiter: DirectRateLimiter,
    state: tokio::sync::Mutex<IopsState>,
}

#[derive(Debug)]
struct IopsState {
    credit: f64,
    credit_updated_at: tokio::time::Instant,
    last_soft_release_at: tokio::time::Instant,
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
    /// 硬带宽上限字符串。
    pub hard_bandwidth: Option<String>,
    /// 硬峰值速率倍数
    pub peak_rate: f32,
    /// 兼容字段：旧构造入口提供的 burst/source IO 参数。
    pub burst_bytes: Option<u64>,
    /// 满信用时允许维持硬峰值的时间。
    pub peak_duration: Option<Duration>,
    /// 由 `(hard - soft) × peak_duration` 推导出的软信用容量。
    pub credit_capacity_bytes: Option<u64>,
    /// 单次非流式源端 IO 的额外上限；实际 grant 还受硬整形量子约束。
    pub max_io_bytes: Option<u64>,
    /// IOPS 限制
    pub iops: Option<u32>,
    /// IOPS 硬峰值限制。
    pub hard_iops: Option<u32>,
    /// 满信用时允许维持硬 IOPS 峰值的时间。
    pub iops_peak_duration: Option<Duration>,
    /// `(hard_iops - soft_iops) × peak_duration` 推导出的操作信用容量。
    pub iops_credit_capacity: Option<f64>,
}

/// `QoS` 管理器
///
/// 封装软 Token Bucket、硬 Leaky Bucket 和 IOPS limiter，支持 `ArcSwap` 热更新。
/// `Clone + Send + Sync`，可直接 clone 传递，无需外层 Mutex。
#[derive(Clone, Debug)]
pub struct QosManager {
    bandwidth_limiter: Option<Arc<ArcSwap<BandwidthController>>>,
    iops_limiter: Option<Arc<ArcSwap<IopsController>>>,
    stats: Arc<QosStats>,
    config: Arc<ArcSwap<QosConfig>>,
}

/// 构建软 Token Bucket + 硬 Leaky Bucket。
const MIN_PEAK_RATE_BPS: u64 = 2 * 1024 * 1024; // 2MB/s
const DEFAULT_PEAK_DURATION: Duration = Duration::from_secs(1);

/// 用基准速率 + 显式 burst（字节数）构建一个 governor `BandwidthLimiter`。
///
/// 这是带宽 limiter 的"内核"——`build_bandwidth_limiter` 通过 `peak_rate`
/// 算出 burst 后调本函数，`build_bandwidth_limiter_with_burst` 直接传 burst。
fn build_bandwidth_limiter_inner(
    soft_rate_bps: u64,
    hard_rate_bps: u64,
    peak_duration: Duration,
    max_io_bytes: u64,
) -> Result<BandwidthController> {
    if max_io_bytes == 0 {
        return Err(StorageError::ConfigError(
            "max_io_bytes 必须大于 0".to_string(),
        ));
    }
    if soft_rate_bps == 0 || hard_rate_bps == 0 {
        return Err(StorageError::ConfigError(
            "软、硬带宽必须大于 0".to_string(),
        ));
    }
    if hard_rate_bps < soft_rate_bps {
        return Err(StorageError::ConfigError(
            "硬带宽不能小于软带宽".to_string(),
        ));
    }
    if max_io_bytes < MIN_PEAK_RATE_BPS {
        debug!(
            "[QoS] 单次 IO 上限较小 ({max_io_bytes} B)，大请求会被分批整形；\
             如这是有意为之（严格平均速率），可忽略。"
        );
    }
    let peak_quantum = u64::try_from(
        u128::from(hard_rate_bps)
            .saturating_mul(PEAK_SHAPING_WINDOW.as_nanos())
            .div_ceil(1_000_000_000),
    )
    .unwrap_or(u64::MAX)
    .max(1);
    let max_io_bytes = peak_quantum.min(max_io_bytes);
    let credit_capacity_bytes = u64::try_from(
        u128::from(hard_rate_bps - soft_rate_bps)
            .saturating_mul(peak_duration.as_nanos())
            .div_ceil(1_000_000_000),
    )
    .map_err(|_| StorageError::ConfigError("软带宽信用容量超过 u64 范围".to_string()))?;
    let now = tokio::time::Instant::now();
    Ok(BandwidthController {
        soft_bytes_per_second: soft_rate_bps,
        hard_bytes_per_second: hard_rate_bps,
        credit_capacity_bytes,
        max_io_bytes,
        state: tokio::sync::Mutex::new(BandwidthState {
            credit_bytes: 0.0,
            credit_updated_at: now,
            last_soft_release_at: now,
            last_release_at: now,
        }),
    })
}

fn build_bandwidth_limiter(bandwidth_str: &str, peak_rate: f32) -> Result<BandwidthController> {
    let base_rate_bps = parse_bandwidth_string(bandwidth_str)?;
    let hard_rate_bps = checked_rounded_u64(
        base_rate_bps
            .to_f64()
            .ok_or_else(|| StorageError::ConfigError("带宽速率无法转换为浮点数".to_string()))?
            * f64::from(peak_rate),
        "hard bandwidth",
    )?;
    build_bandwidth_limiter_inner(
        base_rate_bps,
        hard_rate_bps,
        DEFAULT_PEAK_DURATION,
        hard_rate_bps,
    )
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
    build_bandwidth_limiter_inner(base_rate_bps, base_rate_bps, Duration::ZERO, burst_bytes)
}

fn build_iops_limiter(
    soft_iops: u32,
    hard_iops: u32,
    peak_duration: Duration,
) -> Result<IopsController> {
    if soft_iops == 0 || hard_iops == 0 {
        return Err(StorageError::ConfigError(
            "软、硬 IOPS 必须大于 0".to_string(),
        ));
    }
    if hard_iops < soft_iops {
        return Err(StorageError::ConfigError(
            "硬 IOPS 不能小于软 IOPS".to_string(),
        ));
    }
    let credit_capacity = f64::from(hard_iops - soft_iops) * peak_duration.as_secs_f64();
    if !credit_capacity.is_finite() {
        return Err(StorageError::ConfigError(
            "IOPS 信用容量超过支持范围".to_string(),
        ));
    }
    let now = tokio::time::Instant::now();
    let hard_rate = std::num::NonZeroU32::new(hard_iops)
        .ok_or_else(|| StorageError::ConfigError("硬 IOPS 必须大于 0".to_string()))?;
    let hard_burst = std::num::NonZeroU32::new(1)
        .ok_or_else(|| StorageError::ConfigError("IOPS burst 计算异常".to_string()))?;
    Ok(IopsController {
        soft_iops,
        credit_capacity,
        hard_limiter: RateLimiter::direct(Quota::per_second(hard_rate).allow_burst(hard_burst)),
        state: tokio::sync::Mutex::new(IopsState {
            credit: 0.0,
            credit_updated_at: now,
            last_soft_release_at: now,
        }),
    })
}

impl QosManager {
    /// 创建新的 `QoS` 管理器（基于 `peak_rate` 倍数）
    ///
    /// - `bandwidth`: 带宽限制字符串，如 "200MiB/s"，None 则不限速
    /// - `peak_rate`: 相对软带宽的硬峰值倍数。软信用按一秒峰值持续时间推导，
    ///   初始为零；实际源端 IO 按 10 ms 硬漏桶量子 pacing。
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
                let limiter = build_iops_limiter(iops_val, iops_val, Duration::ZERO)?;
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
            hard_bandwidth: bandwidth.and_then(|bw| {
                let base = parse_bandwidth_string(bw).ok()?.to_f64()?;
                checked_rounded_u64(base * f64::from(peak_rate), "hard bandwidth")
                    .ok()
                    .map(|rate| format!("{rate}B/s"))
            }),
            peak_rate,
            burst_bytes: configured_burst,
            peak_duration: bandwidth.map(|_| DEFAULT_PEAK_DURATION),
            credit_capacity_bytes: bandwidth.and_then(|bw| {
                let soft = parse_bandwidth_string(bw).ok()?;
                let hard =
                    checked_rounded_u64(soft.to_f64()? * f64::from(peak_rate), "hard bandwidth")
                        .ok()?;
                Some(hard.saturating_sub(soft))
            }),
            max_io_bytes: configured_burst,
            iops,
            hard_iops: iops,
            iops_peak_duration: iops.map(|_| Duration::ZERO),
            iops_credit_capacity: iops.map(|_| 0.0),
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
    /// 适合需要显式限制单次源端 IO 大小的严格整形场景，例如：
    /// - 嵌入到长稳 daemon 中（HSM copytool / 后台同步），不希望小文件被
    ///   大 storage chunk 造成共享带宽尖刺
    /// - 自动测试需要可预测的 wall-clock 行为
    ///
    /// - `bandwidth`: 带宽限制字符串，如 "200MiB/s"
    /// - `burst_bytes`: 兼容名称，表示单次源端 IO 上限。软、硬速率相等，
    ///   因此不产生软突发信用；小于 storage chunk 时自动拆分；必须大于 0。
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
                let l = build_iops_limiter(iops_val, iops_val, Duration::ZERO)?;
                Some(Arc::new(ArcSwap::from_pointee(l)))
            }
            _ => None,
        };

        let config = QosConfig {
            bandwidth: Some(bandwidth.to_string()),
            hard_bandwidth: Some(bandwidth.to_string()),
            peak_rate: 1.0,
            burst_bytes: Some(burst_bytes),
            peak_duration: Some(Duration::ZERO),
            credit_capacity_bytes: Some(0),
            max_io_bytes: Some(burst_bytes),
            iops,
            hard_iops: iops,
            iops_peak_duration: iops.map(|_| Duration::ZERO),
            iops_credit_capacity: iops.map(|_| 0.0),
        };

        Ok(Self {
            bandwidth_limiter,
            iops_limiter,
            stats: Arc::new(QosStats::new()),
            config: Arc::new(ArcSwap::from_pointee(config)),
        })
    }

    /// 创建软平均带宽 + 硬峰值带宽的双层整形器。
    ///
    /// 软信用容量自动按 `(hard - soft) × peak_duration` 推导；初始信用为 0。
    /// `max_io_bytes` 是协议请求上限，实际 grant 还会被 10 ms 硬整形量子截断。
    ///
    /// # Errors
    ///
    /// 软、硬带宽格式无效、硬带宽小于软带宽、最大 IO 为零或推导容量溢出时返回错误。
    pub fn try_new_with_limits(
        soft_bandwidth: &str,
        hard_bandwidth: &str,
        peak_duration: Duration,
        max_io_bytes: u64,
        iops: Option<u32>,
    ) -> Result<Self> {
        let soft_rate = parse_bandwidth_string(soft_bandwidth)?;
        let hard_rate = parse_bandwidth_string(hard_bandwidth)?;
        let limiter =
            build_bandwidth_limiter_inner(soft_rate, hard_rate, peak_duration, max_io_bytes)?;
        let credit_capacity = limiter.credit_capacity_bytes;
        let peak_rate = hard_rate
            .to_f32()
            .ok_or_else(|| StorageError::ConfigError("硬带宽无法转换为浮点数".to_string()))?
            / soft_rate
                .to_f32()
                .ok_or_else(|| StorageError::ConfigError("软带宽无法转换为浮点数".to_string()))?;
        let iops_limiter =
            match iops {
                Some(value) if value > 0 => Some(Arc::new(ArcSwap::from_pointee(
                    build_iops_limiter(value, value, Duration::ZERO)?,
                ))),
                _ => None,
            };
        Ok(Self {
            bandwidth_limiter: Some(Arc::new(ArcSwap::from_pointee(limiter))),
            iops_limiter,
            stats: Arc::new(QosStats::new()),
            config: Arc::new(ArcSwap::from_pointee(QosConfig {
                bandwidth: Some(soft_bandwidth.to_string()),
                hard_bandwidth: Some(hard_bandwidth.to_string()),
                peak_rate,
                burst_bytes: Some(credit_capacity),
                peak_duration: Some(peak_duration),
                credit_capacity_bytes: Some(credit_capacity),
                max_io_bytes: Some(max_io_bytes),
                iops,
                hard_iops: iops,
                iops_peak_duration: iops.map(|_| Duration::ZERO),
                iops_credit_capacity: iops.map(|_| 0.0),
            })),
        })
    }

    /// 创建带宽和 IOPS 都具有软持续速率、硬峰值及峰值持续时间的整形器。
    ///
    /// # Errors
    ///
    /// 任一速率为零、硬限制小于软限制、带宽格式无效或最大 IO 为零时返回错误。
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_full_limits(
        soft_bandwidth: &str,
        hard_bandwidth: &str,
        bandwidth_peak_duration: Duration,
        max_io_bytes: u64,
        soft_iops: Option<u32>,
        hard_iops: Option<u32>,
        iops_peak_duration: Duration,
    ) -> Result<Self> {
        if soft_iops.is_none() != hard_iops.is_none() {
            return Err(StorageError::ConfigError(
                "软、硬 IOPS 必须同时配置".to_string(),
            ));
        }
        let mut manager = Self::try_new_with_limits(
            soft_bandwidth,
            hard_bandwidth,
            bandwidth_peak_duration,
            max_io_bytes,
            None,
        )?;
        if let (Some(soft), Some(hard)) = (soft_iops, hard_iops) {
            let limiter = build_iops_limiter(soft, hard, iops_peak_duration)?;
            let credit_capacity = limiter.credit_capacity;
            manager.iops_limiter = Some(Arc::new(ArcSwap::from_pointee(limiter)));
            let mut config = (**manager.config.load()).clone();
            config.iops = Some(soft);
            config.hard_iops = Some(hard);
            config.iops_peak_duration = Some(iops_peak_duration);
            config.iops_credit_capacity = Some(credit_capacity);
            manager.config.store(Arc::new(config));
        }
        Ok(manager)
    }

    /// 创建仅包含软持续 IOPS 和硬峰值 IOPS 的双层漏桶整形器。
    ///
    /// # Errors
    ///
    /// 任一速率为零或硬 IOPS 小于软 IOPS 时返回错误。
    pub fn try_new_with_iops_limits(
        soft_iops: u32,
        hard_iops: u32,
        peak_duration: Duration,
    ) -> Result<Self> {
        let limiter = build_iops_limiter(soft_iops, hard_iops, peak_duration)?;
        let credit_capacity = limiter.credit_capacity;
        Ok(Self {
            bandwidth_limiter: None,
            iops_limiter: Some(Arc::new(ArcSwap::from_pointee(limiter))),
            stats: Arc::new(QosStats::new()),
            config: Arc::new(ArcSwap::from_pointee(QosConfig {
                bandwidth: None,
                hard_bandwidth: None,
                peak_rate: 1.0,
                burst_bytes: None,
                peak_duration: None,
                credit_capacity_bytes: None,
                max_io_bytes: None,
                iops: Some(soft_iops),
                hard_iops: Some(hard_iops),
                iops_peak_duration: Some(peak_duration),
                iops_credit_capacity: Some(credit_capacity),
            })),
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
        let hard_nanos = u128::from(granted)
            .saturating_mul(1_000_000_000)
            .div_ceil(u128::from(controller.hard_bytes_per_second));
        let hard_interval = Duration::from_nanos(u64::try_from(hard_nanos).unwrap_or(u64::MAX));
        let mut state = controller.state.lock().await;
        let now = tokio::time::Instant::now();

        let elapsed = now.saturating_duration_since(state.credit_updated_at);
        let soft_rate = controller
            .soft_bytes_per_second
            .to_f64()
            .unwrap_or(f64::MAX);
        let credit_capacity = controller
            .credit_capacity_bytes
            .to_f64()
            .unwrap_or(f64::MAX);
        let granted_f64 = granted.to_f64().unwrap_or(f64::MAX);
        let refilled = elapsed.as_secs_f64() * soft_rate;
        state.credit_bytes = (state.credit_bytes + refilled).min(credit_capacity);
        state.credit_updated_at = now;

        let soft_deadline = if controller.credit_capacity_bytes == 0 {
            let nanos = u128::from(granted)
                .saturating_mul(1_000_000_000)
                .div_ceil(u128::from(controller.soft_bytes_per_second));
            let interval = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
            let nominal = state.last_soft_release_at + interval;
            if now <= nominal {
                nominal
            } else {
                now + interval
            }
        } else if state.credit_bytes >= granted_f64 {
            state.credit_bytes -= granted_f64;
            now
        } else {
            let deficit = granted_f64 - state.credit_bytes;
            state.credit_bytes = 0.0;
            now + Duration::from_secs_f64(deficit / soft_rate)
        };

        let nominal_hard_deadline = state.last_release_at + hard_interval;
        let hard_deadline = if now <= nominal_hard_deadline {
            nominal_hard_deadline
        } else {
            now + hard_interval
        };
        let deadline = soft_deadline.max(hard_deadline);
        state.credit_updated_at = soft_deadline;
        if controller.credit_capacity_bytes == 0 {
            state.last_soft_release_at = deadline;
        }
        state.last_release_at = deadline;
        tokio::time::sleep_until(deadline).await;
        drop(state);
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
            let controller = iops.load_full();
            let mut state = controller.state.lock().await;
            let now = tokio::time::Instant::now();
            let elapsed = now.saturating_duration_since(state.credit_updated_at);
            state.credit = (state.credit + elapsed.as_secs_f64() * f64::from(controller.soft_iops))
                .min(controller.credit_capacity);
            state.credit_updated_at = now;

            let soft_deadline = if controller.credit_capacity == 0.0 {
                let interval = Duration::from_secs_f64(1.0 / f64::from(controller.soft_iops));
                let nominal = state.last_soft_release_at + interval;
                if now <= nominal {
                    nominal
                } else {
                    now + interval
                }
            } else if state.credit >= 1.0 {
                state.credit -= 1.0;
                now
            } else {
                let deficit = 1.0 - state.credit;
                state.credit = 0.0;
                now + Duration::from_secs_f64(deficit / f64::from(controller.soft_iops))
            };
            state.credit_updated_at = soft_deadline;
            if controller.credit_capacity == 0.0 {
                state.last_soft_release_at = soft_deadline;
            }
            tokio::time::sleep_until(soft_deadline).await;
            drop(state);
            controller.hard_limiter.until_ready().await;
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
        let hard = checked_rounded_u64(
            base.to_f64()
                .ok_or_else(|| StorageError::ConfigError("带宽速率无法转换为浮点数".to_string()))?
                * f64::from(peak_rate),
            "hard bandwidth",
        )?;
        new_config.hard_bandwidth = Some(format!("{hard}B/s"));
        new_config.burst_bytes = Some(hard);
        new_config.peak_duration = Some(DEFAULT_PEAK_DURATION);
        new_config.credit_capacity_bytes = Some(hard.saturating_sub(base));
        new_config.max_io_bytes = Some(hard);
        self.config.store(Arc::new(new_config));
        Ok(())
    }

    /// 热更新 IOPS 限制
    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub fn update_iops(&self, new_iops: u32) -> Result<()> {
        let new_limiter = build_iops_limiter(new_iops, new_iops, Duration::ZERO)?;
        if let Some(iops) = &self.iops_limiter {
            iops.store(Arc::new(new_limiter));
        }
        // 更新配置快照
        let mut new_config = (**self.config.load()).clone();
        new_config.iops = Some(new_iops);
        new_config.hard_iops = Some(new_iops);
        new_config.iops_peak_duration = Some(Duration::ZERO);
        new_config.iops_credit_capacity = Some(0.0);
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

    #[test]
    fn explicit_soft_hard_limits_derive_credit_capacity() {
        let qos = QosManager::try_new_with_limits(
            "100MiB/s",
            "150MiB/s",
            Duration::from_millis(500),
            2 * 1024 * 1024,
            None,
        )
        .assert_value("create dual-rate qos");

        let config = qos.config();
        assert_eq!(config.bandwidth.as_deref(), Some("100MiB/s"));
        assert_eq!(config.hard_bandwidth.as_deref(), Some("150MiB/s"));
        assert_eq!(config.peak_duration, Some(Duration::from_millis(500)));
        assert_eq!(config.credit_capacity_bytes, Some(25 * 1024 * 1024));
        assert_eq!(config.max_io_bytes, Some(2 * 1024 * 1024));
    }

    #[tokio::test(start_paused = true)]
    async fn hard_leaky_bucket_splits_large_chunks_at_the_shaping_quantum() {
        let qos = QosManager::try_new_with_limits(
            "100MiB/s",
            "150MiB/s",
            Duration::from_millis(500),
            8 * 1024 * 1024,
            None,
        )
        .assert_value("create dual-rate qos");

        // 150 MiB/s × 10 ms = 1.5 MiB. Initial credit is zero, so the first
        // grant is governed by the 100 MiB/s soft rate and takes 15 ms.
        let started = tokio::time::Instant::now();
        let granted = qos.acquire_bandwidth_grant(8 * 1024 * 1024).await;
        assert_eq!(granted, 1536 * 1024);
        assert_eq!(started.elapsed(), Duration::from_millis(15));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_accumulates_soft_credit_but_never_bypasses_the_hard_rate() {
        let qos = QosManager::try_new_with_limits(
            "100MiB/s",
            "150MiB/s",
            Duration::from_millis(500),
            8 * 1024 * 1024,
            None,
        )
        .assert_value("create dual-rate qos");
        tokio::time::sleep(Duration::from_millis(500)).await;

        let started = tokio::time::Instant::now();
        assert_eq!(
            qos.acquire_bandwidth_grant(8 * 1024 * 1024).await,
            1536 * 1024
        );
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(10),
            "full soft credit may reach, but never bypass, the 150 MiB/s hard schedule"
        );
    }

    #[test]
    fn dual_rate_qos_rejects_inverted_rates_and_zero_io_size() {
        assert!(
            QosManager::try_new_with_limits(
                "150MiB/s",
                "100MiB/s",
                Duration::from_millis(500),
                1024,
                None,
            )
            .is_err()
        );
        assert!(
            QosManager::try_new_with_limits(
                "100MiB/s",
                "150MiB/s",
                Duration::from_millis(500),
                0,
                None,
            )
            .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn leaky_bucket_absorbs_protocol_latency_before_the_next_slot() {
        let qos = QosManager::try_new_with_burst("1MiB/s", 4 * 1024, None)
            .assert_value("create strict qos");
        let started = tokio::time::Instant::now();

        assert_eq!(qos.acquire_bandwidth_grant(4 * 1024).await, 4 * 1024);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(qos.acquire_bandwidth_grant(4 * 1024).await, 4 * 1024);

        // Two 4 KiB releases at 1 MiB/s occupy 7.8125 ms in total. The 1 ms
        // protocol delay fits inside that schedule and must not be added again.
        assert!(
            started.elapsed() <= Duration::from_millis(8),
            "the 1 ms protocol delay must fit inside the two release slots"
        );
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

    #[tokio::test]
    async fn soft_iops_credit_bursts_only_up_to_the_governor_hard_limit() {
        let qos = QosManager::try_new_with_iops_limits(20, 100, Duration::from_millis(100))
            .assert_value("create dual-rate IOPS qos");

        // Initial credit is zero, so the first operation is paced by soft IOPS.
        let cold_started = Instant::now();
        qos.acquire_iops().await;
        assert!(cold_started.elapsed() >= Duration::from_millis(45));

        // Idle time fills at most eight operation credits. Those operations
        // must still traverse governor's 100 IOPS hard leaky bucket.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let burst_started = Instant::now();
        for _ in 0..8 {
            qos.acquire_iops().await;
        }
        let burst_elapsed = burst_started.elapsed();
        assert!(burst_elapsed >= Duration::from_millis(65));
        assert!(burst_elapsed < Duration::from_millis(180));

        // Once credit is consumed, the schedule returns to the 20 IOPS soft rate.
        let sustained_started = Instant::now();
        for _ in 0..4 {
            qos.acquire_iops().await;
        }
        assert!(sustained_started.elapsed() >= Duration::from_millis(120));
    }

    #[test]
    fn dual_iops_rejects_hard_rate_below_soft_rate() {
        assert!(QosManager::try_new_with_iops_limits(101, 100, Duration::from_secs(1)).is_err());
    }

    /// 非法配置（带宽串解析失败）应当返回错误。
    #[test]
    fn test_try_new_with_burst_invalid_bandwidth() {
        let res = QosManager::try_new_with_burst("not-a-rate", 1024, None);
        assert!(res.is_err());
    }
}
