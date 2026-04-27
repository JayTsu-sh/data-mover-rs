// 标准库
use std::path::{Path, PathBuf};
use std::sync::Arc;

// 外部 crate
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::error;

// 内部模块
use crate::filter::FilterExpression;
use crate::{EntryEnum, Result};

// ============================================================
// 公开类型：NdxEvent 系列（消费端使用）
// ============================================================

/// 带 NDX 编号的 entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NdxEntry {
    pub ndx: i32,
    pub entry: Arc<EntryEnum>,
}

/// 一整页目录内容，消费完 drop 即释放内存
#[derive(Debug, Serialize, Deserialize)]
pub struct DirPageResult {
    /// 本目录的 `relative_path`（root 为空字符串）
    pub dir_path: String,
    /// 本页 NDX 范围起始（含）
    pub ndx_start: i32,
    /// 文件 entries（已按 name 排序，NDX 从 `ndx_start` 连续递增）
    pub files: Vec<NdxEntry>,
    /// 子目录 entries（已按 name 排序，NDX 紧接 files 之后连续递增）
    pub subdirs: Vec<NdxEntry>,
    /// 本页段间隔的 gap NDX 值（-1 表示整棵树最后一页，无 gap）
    pub gap_ndx: i32,
}

/// `walkdir_2` 产出的事件流（页级粒度）
#[derive(Debug)]
pub enum NdxEvent {
    /// 一整页目录内容，DFS 顺序产出
    Page(DirPageResult),
    /// 遍历过程中的 per-directory 错误（不中断整棵树）
    Error { path: String, reason: String },
    /// 整棵树遍历完成，对应 rsync `NDX_DONE` = -1
    Done,
}

// ============================================================
// Reader 池通信类型
// ============================================================

/// 后端类型标识，用于 DFS Driver 中区分不同存储后端的 handle 构建方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Local,
    Nfs,
    S3,
    Cifs,
}

/// 后端特定的目录句柄，Reader Worker 用它来"打开"一个目录
#[derive(Debug, Clone)]
pub enum DirHandle {
    /// Local: 绝对路径
    Local(PathBuf),
    /// NFS: 文件句柄 + 路径
    Nfs { fh: Bytes, path: String },
    /// S3: 前缀字符串
    S3Prefix(String),
    /// CIFS/SMB: 相对路径（相对于 root）
    Cifs(String),
}

impl DirHandle {
    /// 返回此 handle 对应的后端类型
    pub fn backend_kind(&self) -> BackendKind {
        match self {
            DirHandle::Local(_) => BackendKind::Local,
            DirHandle::Nfs { .. } => BackendKind::Nfs,
            DirHandle::S3Prefix(_) => BackendKind::S3,
            DirHandle::Cifs(_) => BackendKind::Cifs,
        }
    }
}

/// 从 `EntryEnum` 提取后端对应的 `DirHandle`
///
/// 使用 `backend` 参数显式指定后端类型，避免隐式约定。
pub fn extract_dir_handle(entry: &EntryEnum, root_path: &Path, backend: BackendKind) -> DirHandle {
    match entry {
        EntryEnum::NAS(nas) => match backend {
            BackendKind::Nfs => {
                if let Some(fh) = &nas.file_handle {
                    DirHandle::Nfs {
                        fh: fh.clone(),
                        path: nas.relative_path.to_string_lossy().to_string(),
                    }
                } else {
                    // NFS entry 缺少 file_handle 时降级为路径
                    DirHandle::Nfs {
                        fh: Bytes::new(),
                        path: nas.relative_path.to_string_lossy().to_string(),
                    }
                }
            }
            BackendKind::Cifs => DirHandle::Cifs(nas.relative_path.to_string_lossy().replace('\\', "/")),
            _ => DirHandle::Local(root_path.join(&nas.relative_path)),
        },
        EntryEnum::S3(s3) => DirHandle::S3Prefix(s3.relative_path.clone()),
    }
}

/// `ReadRequest` 携带的上下文：filter + depth 信息
#[derive(Debug, Clone)]
pub struct ReadContext {
    pub match_expr: Arc<Option<FilterExpression>>,
    pub exclude_expr: Arc<Option<FilterExpression>>,
    pub current_depth: usize,
    pub max_depth: usize,
    /// 是否对当前目录内的 entry 应用 filter。
    /// false = 父目录已匹配，当前目录下所有 entry 无需过滤（对应 `skip_filter=false`）
    pub apply_filter: bool,
    /// S3 专用
    pub include_tags: bool,
    pub is_versioned: bool,
}

/// 子目录 entry，携带递归控制标志
#[derive(Debug)]
pub struct SubdirEntry {
    pub entry: Arc<EntryEnum>,
    /// 是否在页面中可见（分配 NDX）。
    /// false = 目录被 filter 跳过（`skip_entry=true`），但 `continue_scan=true` 仍需递归
    pub visible: bool,
    /// 子目录内的 entry 是否需要 filter。映射自 `should_skip` 返回的 `need_submatch`。
    /// true = 子目录需要 filter，false = 子目录继承父目录的匹配状态（不过滤）
    pub need_filter: bool,
}

/// 读取一个目录的结果
#[derive(Debug)]
pub struct ReadResult {
    pub dir_path: String,
    /// 排序后的文件 entries（非目录）
    pub files: Vec<Arc<EntryEnum>>,
    /// 排序后的子目录 entries（含 `visible`/`need_filter` 标志）
    pub subdirs: Vec<SubdirEntry>,
    /// 读取过程中的错误
    pub errors: Vec<String>,
}

/// 发送给 Reader 池的请求
pub struct ReadRequest {
    pub dir_path: String,
    pub handle: DirHandle,
    pub ctx: ReadContext,
    pub reply: oneshot::Sender<Result<ReadResult>>,
}

// ============================================================
// DFS Driver
// ============================================================

/// 窗口预读大小
const PREFETCH_WINDOW: usize = 32;

/// DFS 栈帧
struct DfsFrame {
    dir_path: String,
    /// 本目录的读取结果 receiver
    read_rx: Option<oneshot::Receiver<Result<ReadResult>>>,
    /// 所有子目录（含 `visible`/`need_filter` 标志）
    subdirs: Vec<SubdirEntry>,
    /// 已提交预读的子目录对应的 oneshot receiver
    pending_reads: Vec<Option<oneshot::Receiver<Result<ReadResult>>>>,
    /// 下一个要 DFS 下降的子目录索引
    next_child: usize,
    /// 下一个要提交预读的子目录索引（窗口前沿）
    next_prefetch: usize,
    /// 本帧是否已发送过 Page
    page_sent: bool,
}

impl DfsFrame {
    fn new_root(read_rx: oneshot::Receiver<Result<ReadResult>>) -> Self {
        Self {
            dir_path: String::new(),
            read_rx: Some(read_rx),
            subdirs: Vec::new(),
            pending_reads: Vec::new(),
            next_child: 0,
            next_prefetch: 0,
            page_sent: false,
        }
    }

    /// 构建子目录的 `ReadContext`，传递 `need_filter` → `apply_filter`
    fn build_child_ctx(&self, child_idx: usize, base_ctx: &ReadContext) -> ReadContext {
        let need_filter = self.subdirs[child_idx].need_filter;
        ReadContext {
            current_depth: base_ctx.current_depth + self.depth_offset(),
            apply_filter: need_filter,
            ..base_ctx.clone()
        }
    }

    /// 预提交子目录读取，最多到窗口边界
    async fn prefetch_subdirs(
        &mut self, req_tx: &async_channel::Sender<ReadRequest>, root_path: &Path, backend: BackendKind,
        base_ctx: &ReadContext,
    ) {
        let window_end = (self.next_child + PREFETCH_WINDOW).min(self.subdirs.len());
        while self.next_prefetch < window_end {
            let sub = &self.subdirs[self.next_prefetch];
            let (reply_tx, reply_rx) = oneshot::channel();
            let handle = extract_dir_handle(&sub.entry, root_path, backend);
            let child_ctx = self.build_child_ctx(self.next_prefetch, base_ctx);
            if req_tx
                .send(ReadRequest {
                    dir_path: sub.entry.get_relative_path().to_string_lossy().to_string(),
                    handle,
                    ctx: child_ctx,
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                break;
            }
            self.pending_reads[self.next_prefetch] = Some(reply_rx);
            self.next_prefetch += 1;
        }
    }

    /// 消费一个子目录后，滑动窗口
    async fn advance_prefetch(
        &mut self, req_tx: &async_channel::Sender<ReadRequest>, root_path: &Path, backend: BackendKind,
        base_ctx: &ReadContext,
    ) {
        if self.next_prefetch < self.subdirs.len() {
            let sub = &self.subdirs[self.next_prefetch];
            let (reply_tx, reply_rx) = oneshot::channel();
            let handle = extract_dir_handle(&sub.entry, root_path, backend);
            let child_ctx = self.build_child_ctx(self.next_prefetch, base_ctx);
            if req_tx
                .send(ReadRequest {
                    dir_path: sub.entry.get_relative_path().to_string_lossy().to_string(),
                    handle,
                    ctx: child_ctx,
                    reply: reply_tx,
                })
                .await
                .is_ok()
            {
                self.pending_reads[self.next_prefetch] = Some(reply_rx);
                self.next_prefetch += 1;
            }
        }
    }

    /// 通过 `dir_path` 的分隔符数量推算深度偏移
    fn depth_offset(&self) -> usize {
        if self.dir_path.is_empty() {
            1
        } else {
            self.dir_path.matches('/').count() + 2
        }
    }
}

/// 判断当前帧在栈中是否还有未处理的可见兄弟目录（检查整个祖先链）
fn has_more_visible_siblings(stack: &[DfsFrame]) -> bool {
    // 从直接父级到根，任一祖先还有未处理的可见子目录就返回 true
    for i in (0..stack.len().saturating_sub(1)).rev() {
        let ancestor = &stack[i];
        if ancestor.subdirs[ancestor.next_child..].iter().any(|s| s.visible) {
            return true;
        }
    }
    false
}

/// DFS 驱动器主循环，运行在单个 tokio task 中
pub async fn run_dfs_driver(
    req_tx: async_channel::Sender<ReadRequest>, out_tx: async_channel::Sender<NdxEvent>, root_path: PathBuf,
    root_handle: DirHandle, base_ctx: ReadContext,
) {
    // 从 root_handle 推导后端类型，用于子目录 handle 构建
    let backend = root_handle.backend_kind();

    // 提交 root 目录读取
    let (reply_tx, reply_rx) = oneshot::channel();
    if req_tx
        .send(ReadRequest {
            dir_path: String::new(),
            handle: root_handle,
            ctx: base_ctx.clone(),
            reply: reply_tx,
        })
        .await
        .is_err()
    {
        let _ = out_tx.send(NdxEvent::Done).await;
        return;
    }

    let mut stack: Vec<DfsFrame> = vec![DfsFrame::new_root(reply_rx)];
    let mut next_ndx: i32 = 0;

    loop {
        let depth = stack.len();
        if depth == 0 {
            break;
        }
        let frame_idx = depth - 1;

        // ① 首次进入：等待读取结果 + 窗口预读 + yield Page
        if !stack[frame_idx].page_sent {
            let Some(rx) = stack[frame_idx].read_rx.take() else {
                stack.pop();
                continue;
            };
            let result = match rx.await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    let _ = out_tx
                        .send(NdxEvent::Error {
                            path: stack[frame_idx].dir_path.clone(),
                            reason: format!("{e:?}"),
                        })
                        .await;
                    stack.pop();
                    continue;
                }
                Err(_) => {
                    error!(
                        "[DfsDriver] oneshot receiver dropped for dir: {}",
                        stack[frame_idx].dir_path
                    );
                    stack.pop();
                    continue;
                }
            };

            // 保存子目录列表，初始化 pending_reads
            let subdir_count = result.subdirs.len();
            stack[frame_idx].subdirs = result.subdirs;
            stack[frame_idx].pending_reads = (0..subdir_count).map(|_| None).collect();

            // 窗口预读：提交前 PREFETCH_WINDOW 个子目录
            stack[frame_idx]
                .prefetch_subdirs(&req_tx, &root_path, backend, &base_ctx)
                .await;

            // 发送错误
            for err in &result.errors {
                let _ = out_tx
                    .send(NdxEvent::Error {
                        path: stack[frame_idx].dir_path.clone(),
                        reason: err.clone(),
                    })
                    .await;
            }

            // 分配 NDX：只给 visible 的 entry 分配
            let ndx_start = next_ndx;
            let files: Vec<NdxEntry> = result
                .files
                .into_iter()
                .map(|e| {
                    let ndx = next_ndx;
                    next_ndx += 1;
                    NdxEntry { ndx, entry: e }
                })
                .collect();
            // 只给 visible=true 的子目录分配 NDX
            let subdir_entries: Vec<NdxEntry> = stack[frame_idx]
                .subdirs
                .iter()
                .filter(|s| s.visible)
                .map(|s| {
                    let ndx = next_ndx;
                    next_ndx += 1;
                    NdxEntry {
                        ndx,
                        entry: s.entry.clone(),
                    }
                })
                .collect();

            let has_visible_children = !subdir_entries.is_empty();
            let has_any_children = !stack[frame_idx].subdirs.is_empty();
            let has_siblings = has_more_visible_siblings(&stack);
            let gap_ndx = if (has_visible_children || has_any_children) || has_siblings {
                let g = next_ndx;
                next_ndx += 1;
                g
            } else {
                -1
            };

            // 只在有可见内容时发送 Page
            if (!files.is_empty() || !subdir_entries.is_empty())
                && out_tx
                    .send(NdxEvent::Page(DirPageResult {
                        dir_path: stack[frame_idx].dir_path.clone(),
                        ndx_start,
                        files,
                        subdirs: subdir_entries,
                        gap_ndx,
                    }))
                    .await
                    .is_err()
            {
                return;
            }

            stack[frame_idx].page_sent = true;
        }

        // ② DFS 下降到下一个子目录
        if stack[frame_idx].next_child < stack[frame_idx].subdirs.len() {
            let child_idx = stack[frame_idx].next_child;
            stack[frame_idx].next_child += 1;

            // 滑动窗口
            stack[frame_idx]
                .advance_prefetch(&req_tx, &root_path, backend, &base_ctx)
                .await;

            // 取出该子目录的 oneshot receiver
            let child_rx = stack[frame_idx].pending_reads[child_idx].take();
            let child_path = stack[frame_idx].subdirs[child_idx]
                .entry
                .get_relative_path()
                .to_string_lossy()
                .to_string();

            stack.push(DfsFrame {
                dir_path: child_path,
                read_rx: child_rx,
                subdirs: Vec::new(),
                pending_reads: Vec::new(),
                next_child: 0,
                next_prefetch: 0,
                page_sent: false,
            });
            continue;
        }

        // ③ 弹栈释放
        stack.pop();
    }

    let _ = out_tx.send(NdxEvent::Done).await;
}
