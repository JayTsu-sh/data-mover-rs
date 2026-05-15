use blake3::Hasher;
use tracing::debug;

// 一致性检查 trait
pub trait ConsistencyCheck {
    /// 算法名称
    fn name(&self) -> &str;

    /// 更新哈希计算
    fn update(&mut self, data: &[u8]);

    /// 完成计算并返回哈希值
    fn finalize(self) -> String;

    /// 重置哈希计算器状态
    fn reset(&mut self);
}

// 哈希计算器包装器
#[derive(Clone)]
pub struct HashCalculator {
    hasher: Hasher,
}

impl ConsistencyCheck for HashCalculator {
    fn name(&self) -> &'static str {
        "BLAKE3"
    }

    fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    fn finalize(self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

impl Default for HashCalculator {
    fn default() -> Self {
        Self::new()
    }
}

impl HashCalculator {
    pub fn new() -> Self {
        Self {
            hasher: Hasher::new(),
        }
    }
}

/// 创建校验和计算器
pub fn create_hash_calculator(enable_integrity_check: bool) -> Option<HashCalculator> {
    debug!(
        "Creating hash calculator with enable_integrity_check: {}",
        enable_integrity_check
    );

    if enable_integrity_check {
        debug!("Checksum is enabled, creating Blake3 calculator");
        Some(HashCalculator::new())
    } else {
        debug!("Checksum is disabled, not creating calculator");
        None
    }
}
