//! # BufferPool —— 缓冲池管理器
//!
//! BufferPool 在内存中缓存磁盘上的 Page，减少昂贵的磁盘 I/O。
//! 它是存储引擎中最关键的性能组件。
//!
//! ## 核心职责
//! - **fetch_page**: 获取一个 Page（先查缓存，miss 则从磁盘读）
//! - **unpin_page**: 释放一个 Page（减少引用计数）
//! - **flush_page**: 将脏页写回磁盘
//!
//! ## 架构示意
//! ```text
//!   上层模块 (HeapFile, Executor...)
//!       │
//!       ▼
//!   ┌──────────────┐
//!   │  BufferPool   │  ← 内存缓存
//!   │  frames[]     │     固定大小的 Page 数组
//!   │  page_table   │     PageId → FrameId 映射
//!   │  lru_list     │     LRU 淘汰队列
//!   └──────┬───────┘
//!          ▼
//!   ┌──────────────┐
//!   │ DiskManager   │  ← 磁盘 I/O
//!   └──────────────┘
//! ```
//!
//! ## 使用示例
//! ```no_run
//! use toydb::storage::buffer::BufferPool;
//!
//! let mut pool = BufferPool::new("test.db", 4).unwrap();
//!
//! // 获取一个新页
//! let page_id = pool.new_page().unwrap();
//!
//! // 获取 Page 并写入数据
//! {
//!     let page = pool.fetch_page(page_id).unwrap();
//!     page.write_bytes(0, b"Hello BufferPool!");
//! }
//!
//! // 用完后 unpin
//! pool.unpin_page(page_id, true); // true = 脏页
//! ```

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::Path;

use super::disk::DiskManager;
use super::page::{Page, PageId};

/// 缓冲池
///
/// 管理固定数量的内存 Frame，每个 Frame 可以缓存一个 Page。
/// 当缓冲池满时，使用 LRU 策略淘汰最久没被使用的 Page。
///
/// ## 字段说明
///
/// ```text
/// frames:     [Some(Page5), Some(Page12), None,        Some(Page3)]
/// pin_counts: [2,           0,            0,           1          ]
/// page_table: {5 → 0, 12 → 1, 3 → 3}
/// lru_list:   [1]  ← 只有 pin_count=0 的 Frame 才在这里（Frame 1 即 Page 12）
/// ```
///
/// - `frames`: 页框数组，`Option<Page>` 表示该槽位可能为空
/// - `page_table`: PageId 到 Frame 下标的映射（快速查找 Page 在哪个 Frame）
/// - `pin_counts`: 每个 Frame 的引用计数（pin_count > 0 表示正在被使用）
/// - `lru_list`: LRU 淘汰队列，存放 pin_count == 0 的 Frame 下标
/// - `disk_manager`: 底层磁盘 I/O 管理器
/// - `pool_size`: 缓冲池容量（Frame 数量）
pub struct BufferPool {
    /// 页框数组：每个位置可以放一个 Page
    frames: Vec<Option<Page>>,
    /// 页表：PageId → Frame 下标（类比 Java 的 HashMap<Integer, Integer>）
    page_table: HashMap<PageId, usize>,
    /// 每个 Frame 的引用计数
    pin_counts: Vec<u32>,
    /// LRU 淘汰队列：存放 pin_count == 0 的 Frame 下标
    /// 队头 = 最久没使用的，队尾 = 最近释放的
    lru_list: VecDeque<usize>,
    /// 磁盘管理器（上节课实现的）
    disk_manager: DiskManager,
    /// 缓冲池大小（能容纳多少个 Page）
    pool_size: usize,
}

impl BufferPool {
    /// 创建一个新的缓冲池。
    ///
    /// # 参数
    /// - `path`: 数据库文件路径
    /// - `pool_size`: 缓冲池大小（Frame 数量）
    ///
    /// # 返回
    /// - `Ok(BufferPool)`: 成功创建
    /// - `Err(io::Error)`: 文件打开失败
    ///
    /// # TODO 1: 实现这个函数
    /// 需要做的事：
    /// 1. 创建 DiskManager
    /// 2. 初始化 frames 为 pool_size 个 None
    /// 3. 初始化 pin_counts 为 pool_size 个 0
    /// 4. page_table 为空 HashMap
    /// 5. lru_list 初始化——思考：空缓冲池的所有 Frame 都是"可用"的，
    ///    它们应该放在 lru_list 里吗？
    ///
    /// 提示：`vec![None; pool_size]` 可以创建指定大小的 Vec，
    ///       但注意 Option<Page> 没有实现 Clone trait。
    ///       可以用 `(0..pool_size).map(|_| None).collect()` 代替。
    pub fn new<P: AsRef<Path>>(path: P, pool_size: usize) -> io::Result<Self> {
        // TODO 1:
        // 1. 用 DiskManager::new(path) 创建磁盘管理器
        // 2. 初始化所有字段
        // 3. 思考 lru_list 的初始状态：
        //    - 空 Frame 不是"最近没使用"，而是"从未被使用"
        //    - 但它们确实是可以被分配的空位
        //    - 建议：把所有 Frame 下标加入 lru_list（0, 1, 2, ..., pool_size-1）
        //      这样 fetch_page 时可以从 lru_list 中取空位
        let dm = DiskManager::new(path)?;
        let frames = (0..pool_size).map(|_| None).collect();
        let pin_counts = vec![0; pool_size];
        let page_table = HashMap::new();
        let lru_list = VecDeque::from_iter(0..pool_size);
        Ok(BufferPool {
            frames,
            page_table,
            pin_counts,
            lru_list,
            disk_manager: dm,
            pool_size,
        })
    }

    /// 获取一个 Page。
    ///
    /// 这是 BufferPool 最核心的方法。流程如下：
    ///
    /// ```text
    /// fetch_page(page_id):
    ///   1. page_id 在 page_table 里吗？
    ///      ├─ 是(缓存命中): 找到对应的 Frame
    ///      │   ├─ pin_count += 1
    ///      │   ├─ 从 lru_list 中移除（如果在的话）
    ///      │   └─ 返回 &mut Page
    ///      │
    ///      └─ 否(缓存未命中): 需要从磁盘加载
    ///          ├─ lru_list 为空？→ 所有 Frame 都被 pin 了，返回 Err
    ///          ├─ 从 lru_list 队头取一个 Frame (victim)
    ///          ├─ victim 里有旧 Page 且是脏页？→ 先写回磁盘
    ///          ├─ 从 page_table 中移除旧的 PageId 映射
    ///          ├─ 从磁盘读取新 Page 到这个 Frame
    ///          ├─ 更新 page_table: page_id → frame_id
    ///          ├─ pin_count = 1
    ///          └─ 返回 &mut Page
    /// ```
    ///
    /// # 参数
    /// - `page_id`: 要获取的页编号
    ///
    /// # 返回
    /// - `Ok(&mut Page)`: 成功获取（可能来自缓存或磁盘）
    /// - `Err`: 所有 Frame 都被 pin 住了，无法淘汰
    ///
    /// # TODO 2: 实现这个函数（最复杂的一个）
    /// 分两种情况处理：
    ///
    /// Case 1 - 缓存命中:
    /// ```rust,ignore
    /// if let Some(&frame_id) = self.page_table.get(&page_id) {
    ///     // 增加 pin_count
    ///     // 从 lru_list 移除 (如果在的话)
    ///     // 返回 Page 引用
    /// }
    /// ```
    ///
    /// Case 2 - 缓存未命中:
    /// ```rust,ignore
    /// // 1. 从 lru_list 弹出一个 victim frame
    /// let frame_id = self.lru_list.pop_front()
    ///     .ok_or_else(|| io::Error::new(...))?;
    /// // 2. 如果 victim 里有旧页且是脏的，写回磁盘
    /// // 3. 清理旧的 page_table 映射
    /// // 4. 从磁盘读取新 Page
    /// // 5. 放入 Frame，更新 page_table
    /// // 6. pin_count = 1
    /// ```
    ///
    /// 提示：从 VecDeque 中移除指定元素可以用
    /// `self.lru_list.retain(|&x| x != frame_id)`
    pub fn fetch_page(&mut self, page_id: PageId) -> io::Result<&mut Page> {
        // ---- Case 1: 缓存命中 ----
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.pin_counts[frame_id] += 1;
            self.lru_list.retain(|&x| x != frame_id);
            return Ok(self.frames[frame_id].as_mut().unwrap());
        }

        // ---- Case 2: 缓存未命中 ----
        // TODO(human): 实现缓存未命中的逻辑
        // 需要完成以下步骤：
        // 1. 从 lru_list 弹出一个 victim frame（如果没有可用的，返回错误）
        let frame_id = self
            .lru_list
            .pop_front()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No available frame"))?;
        // 2. 如果 victim frame 里有旧页且是脏的，写回磁盘
        // 3. 清理旧页在 page_table 中的映射
        if let Some(ref old_page) = self.frames[frame_id] {
            if old_page.dirty {
                self.disk_manager.write_page(old_page)?;
            }
            self.page_table.remove(&old_page.id);
        }
        // 4. 创建新的 Page，用 disk_manager.read_page() 从磁盘读取数据
        let mut page = Page::new(page_id);
        self.disk_manager.read_page(&mut page)?;
        // 5. 将新页放入 frame，更新 page_table 映射
        self.frames[frame_id] = Some(page);
        self.page_table.insert(page_id, frame_id);
        // 6. 设置 pin_count = 1
        self.pin_counts[frame_id] = 1;
        // 7. 返回 &mut Page
        Ok(self.frames[frame_id].as_mut().unwrap())
    }

    /// 释放一个 Page（减少引用计数）。
    ///
    /// 当上层模块用完一个 Page 后，必须调用 unpin 告诉缓冲池
    /// "我不再需要这个 Page 了"。
    ///
    /// # 参数
    /// - `page_id`: 要释放的页编号
    /// - `is_dirty`: 调用者是否修改过这个 Page
    ///
    /// # 行为
    /// 1. 查找 page_id 对应的 Frame
    /// 2. 如果 is_dirty 为 true，标记 Page 为脏
    /// 3. pin_count 减 1
    /// 4. 如果 pin_count 变为 0，将 Frame 加入 lru_list 队尾
    ///
    /// # TODO 3: 实现这个函数
    /// ```rust,ignore
    /// // 1. 从 page_table 查找 frame_id
    /// // 2. 如果 is_dirty，设置 page.dirty = true
    /// // 3. pin_count -= 1 (注意：不要减到负数)
    /// // 4. 如果 pin_count == 0，push_back 到 lru_list
    /// ```
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) {
        // 1. 查找 page_id 在哪个 Frame
        // 2. 如果找不到，直接 return（Page 不在缓冲池中）
        // 3. 如果 is_dirty，标记 dirty
        // 4. pin_count 减 1
        // 5. 如果减到 0，加入 lru_list
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            // 只能把 dirty 从 false 改为 true，不能用 false 覆盖 true
            if is_dirty {
                self.frames[frame_id].as_mut().unwrap().dirty = true;
            }
            // 防止 u32 下溢
            if self.pin_counts[frame_id] > 0 {
                self.pin_counts[frame_id] -= 1;
                if self.pin_counts[frame_id] == 0 {
                    self.lru_list.push_back(frame_id);
                }
            }
        }
    }

    /// 将指定 Page 强制写回磁盘。
    ///
    /// 不管 Page 是否为脏，都写入磁盘，然后清除脏标记。
    ///
    /// # 参数
    /// - `page_id`: 要刷盘的页编号
    ///
    /// # TODO 4: 实现这个函数
    /// ```rust,ignore
    /// // 1. 从 page_table 查找 frame_id
    /// // 2. 如果 Page 存在，调用 disk_manager.write_page()
    /// // 3. 清除 dirty 标记
    /// ```
    pub fn flush_page(&mut self, page_id: PageId) -> io::Result<()> {
        // 1. 查找 page_id 对应的 Frame
        // 2. 如果不存在，返回 Ok(()）
        // 3. 获取 Frame 中的 Page，写回磁盘
        // 4. 将 page.dirty 设为 false
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page = self.frames[frame_id].as_mut().unwrap();
            self.disk_manager.write_page(page)?;
            page.dirty = false;
        }
        Ok(())
    }

    /// 分配一个新 Page 并放入缓冲池。
    ///
    /// 这个方法已经帮你实现好了，展示了 fetch_page 的一种使用模式。
    /// 理解这段代码有助于你实现 fetch_page。
    pub fn new_page(&mut self) -> io::Result<PageId> {
        // 1. 让 DiskManager 分配一个新的 PageId
        let page_id = self.disk_manager.allocate_page();

        // 2. 需要一个 Frame 来放这个新 Page
        //    从 lru_list 获取一个可用的 Frame
        let frame_id = self.lru_list.pop_front().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "BufferPool is full: all frames are pinned",
            )
        })?;

        // 3. 如果这个 Frame 里有旧页，需要先处理
        if let Some(ref old_page) = self.frames[frame_id] {
            let old_page_id = old_page.id;
            // 脏页要先写回磁盘
            if old_page.dirty {
                self.disk_manager.write_page(old_page)?;
            }
            // 从 page_table 移除旧映射
            self.page_table.remove(&old_page_id);
        }

        // 4. 创建新的空白 Page，放入 Frame
        let page = Page::new(page_id);
        self.frames[frame_id] = Some(page);

        // 5. 更新映射和引用计数
        self.page_table.insert(page_id, frame_id);
        self.pin_counts[frame_id] = 1; // 新页默认 pin 住

        Ok(page_id)
    }

    /// 将所有脏页写回磁盘。
    ///
    /// 通常在数据库关闭时调用，确保所有修改都持久化。
    /// 这个方法也已经帮你实现好了。
    pub fn flush_all(&mut self) -> io::Result<()> {
        for frame_id in 0..self.pool_size {
            if let Some(ref page) = self.frames[frame_id] {
                if page.dirty {
                    self.disk_manager.write_page(page)?;
                    // 注意：这里不能直接修改 page.dirty，因为我们持有的是 ref
                    // 需要在循环后单独处理
                }
            }
        }
        // 清除所有脏标记
        for frame_id in 0..self.pool_size {
            if let Some(ref mut page) = self.frames[frame_id] {
                page.dirty = false;
            }
        }
        Ok(())
    }
}

// ============================================================
// 🧪 单元测试
// ============================================================
// 运行测试: cargo test storage::buffer
//
// 这些测试从简单到复杂，帮你逐步验证实现。
// 建议先看测试理解预期行为，再填 TODO。
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_db_path(name: &str) -> String {
        format!("/tmp/toydb_test_buffer_{}.db", name)
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    // ---- 测试 1: 基础创建 ----
    #[test]
    fn test_new_buffer_pool() {
        let path = test_db_path("new");
        cleanup(&path);

        let pool = BufferPool::new(&path, 4);
        assert!(pool.is_ok());

        cleanup(&path);
    }

    // ---- 测试 2: 分配新页 ----
    #[test]
    fn test_new_page() {
        let path = test_db_path("new_page");
        cleanup(&path);

        let mut pool = BufferPool::new(&path, 4).unwrap();
        let page_id = pool.new_page().unwrap();
        assert_eq!(page_id, 0);

        let page_id2 = pool.new_page().unwrap();
        assert_eq!(page_id2, 1);

        cleanup(&path);
    }

    // ---- 测试 3: fetch 刚创建的页 ----
    #[test]
    fn test_fetch_existing_page() {
        let path = test_db_path("fetch");
        cleanup(&path);

        let mut pool = BufferPool::new(&path, 4).unwrap();
        let page_id = pool.new_page().unwrap();

        // 写入数据
        {
            let page = pool.fetch_page(page_id).unwrap();
            page.write_bytes(0, b"Hello Buffer!");
        }

        // 再次 fetch 应该能读到数据（缓存命中）
        {
            let page = pool.fetch_page(page_id).unwrap();
            assert_eq!(page.read_bytes(0, 13), b"Hello Buffer!");
        }

        cleanup(&path);
    }

    // ---- 测试 4: unpin 后可以被淘汰 ----
    #[test]
    fn test_unpin_and_evict() {
        let path = test_db_path("evict");
        cleanup(&path);

        // 只有 2 个 Frame 的缓冲池
        let mut pool = BufferPool::new(&path, 2).unwrap();

        // 创建 Page 0 和 Page 1，填满缓冲池
        let p0 = pool.new_page().unwrap();
        let p1 = pool.new_page().unwrap();

        // 写入数据
        {
            let page = pool.fetch_page(p0).unwrap();
            page.write_bytes(0, b"Page Zero");
        }
        {
            let page = pool.fetch_page(p1).unwrap();
            page.write_bytes(0, b"Page One");
        }

        // unpin 两个页（new_page 和 fetch_page 各 pin 了一次，需要 unpin 两次）
        pool.unpin_page(p0, true); // dirty
        pool.unpin_page(p0, false); // new_page 的 pin
        pool.unpin_page(p1, true);
        pool.unpin_page(p1, false);

        // 现在创建 Page 2，应该淘汰 Page 0（最久没用的）
        let p2 = pool.new_page().unwrap();
        assert_eq!(p2, 2);

        // Page 0 被淘汰了，再 fetch 应该从磁盘读回来（会淘汰 Page 1）
        pool.unpin_page(p2, false);
        pool.unpin_page(p2, false);
        {
            let page = pool.fetch_page(p0).unwrap();
            assert_eq!(page.read_bytes(0, 9), b"Page Zero");
        }

        cleanup(&path);
    }

    // ---- 测试 5: 缓冲池满时 pin 住所有页应该报错 ----
    #[test]
    fn test_buffer_pool_full_error() {
        let path = test_db_path("full");
        cleanup(&path);

        let mut pool = BufferPool::new(&path, 2).unwrap();

        // 创建两个页，不 unpin（pin_count > 0）
        let _p0 = pool.new_page().unwrap();
        let _p1 = pool.new_page().unwrap();

        // 再创建一个应该失败
        let result = pool.new_page();
        assert!(result.is_err());

        cleanup(&path);
    }

    // ---- 测试 6: flush 将脏页写回磁盘 ----
    #[test]
    fn test_flush_page() {
        let path = test_db_path("flush");
        cleanup(&path);

        {
            let mut pool = BufferPool::new(&path, 4).unwrap();
            let page_id = pool.new_page().unwrap();

            {
                let page = pool.fetch_page(page_id).unwrap();
                page.write_bytes(0, b"Flushed!");
            }

            pool.unpin_page(page_id, true);
            pool.flush_page(page_id).unwrap();
        }

        // 重新打开，数据应该还在（因为已经 flush 过）
        {
            let mut pool = BufferPool::new(&path, 4).unwrap();
            let page = pool.fetch_page(0).unwrap();
            assert_eq!(page.read_bytes(0, 8), b"Flushed!");
        }

        cleanup(&path);
    }

    // ---- 测试 7: 持久化 —— 脏页淘汰时自动写回 ----
    #[test]
    fn test_dirty_page_eviction_persists() {
        let path = test_db_path("dirty_evict");
        cleanup(&path);

        {
            // 只有 1 个 Frame！
            let mut pool = BufferPool::new(&path, 1).unwrap();

            // 创建 Page 0 并写入
            let p0 = pool.new_page().unwrap();
            {
                let page = pool.fetch_page(p0).unwrap();
                page.write_bytes(0, b"Survive!");
            }
            pool.unpin_page(p0, true); // 标记为脏
            pool.unpin_page(p0, false);

            // 创建 Page 1，淘汰 Page 0（脏页应该自动写回磁盘）
            let _p1 = pool.new_page().unwrap();
        }

        // 重新打开，Page 0 的数据应该还在
        {
            let mut pool = BufferPool::new(&path, 2).unwrap();
            let page = pool.fetch_page(0).unwrap();
            assert_eq!(page.read_bytes(0, 8), b"Survive!");
        }

        cleanup(&path);
    }

    // ---- 测试 8: LRU 顺序正确 ----
    #[test]
    fn test_lru_order() {
        let path = test_db_path("lru_order");
        cleanup(&path);

        // 3 个 Frame
        let mut pool = BufferPool::new(&path, 3).unwrap();

        // 创建 Page 0, 1, 2
        let p0 = pool.new_page().unwrap();
        let p1 = pool.new_page().unwrap();
        let p2 = pool.new_page().unwrap();

        // 写入标识数据
        {
            let page = pool.fetch_page(p0).unwrap();
            page.write_bytes(0, b"AAA");
        }
        {
            let page = pool.fetch_page(p1).unwrap();
            page.write_bytes(0, b"BBB");
        }
        {
            let page = pool.fetch_page(p2).unwrap();
            page.write_bytes(0, b"CCC");
        }

        // 全部 unpin (每个 page 被 new_page pin 1次, fetch_page pin 1次)
        pool.unpin_page(p0, true);
        pool.unpin_page(p0, false);
        pool.unpin_page(p1, true);
        pool.unpin_page(p1, false);
        pool.unpin_page(p2, true);
        pool.unpin_page(p2, false);

        // 再次访问 Page 0，让它变成"最近使用"
        {
            let page = pool.fetch_page(p0).unwrap();
            assert_eq!(page.read_bytes(0, 3), b"AAA");
        }
        pool.unpin_page(p0, false);

        // 现在 LRU 顺序应该是: Page 1 (最久) → Page 2 → Page 0 (最近)
        // 创建 Page 3，应该淘汰 Page 1
        let _p3 = pool.new_page().unwrap();

        // Page 1 被淘汰，unpin p3 后 fetch p1 会从磁盘读回
        pool.unpin_page(_p3, false);
        {
            let page = pool.fetch_page(p1).unwrap();
            assert_eq!(page.read_bytes(0, 3), b"BBB"); // 脏页淘汰时已写回
        }

        cleanup(&path);
    }
}
