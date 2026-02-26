//! # HeapFile —— 堆文件（表的物理存储）
//!
//! HeapFile 是存储引擎的最上层，它把所有下层组件串联起来：
//! BufferPool → Page → SlottedPage → Tuple
//!
//! 它向上提供"表"级别的接口：插入一行、读取一行、全表扫描。
//!
//! ## 使用示例
//! ```no_run
//! use toydb::storage::heap::{HeapFile, RecordId};
//!
//! let mut heap = HeapFile::new("users.db", 64).unwrap();
//!
//! // 插入一条记录（序列化后的 Tuple 字节）
//! let rid = heap.insert_record(b"some tuple bytes").unwrap();
//!
//! // 按 RecordId 读取
//! let data = heap.get_record(&rid).unwrap();
//!
//! // 全表扫描
//! let all_records = heap.scan().unwrap();
//! ```

use std::io;
use std::path::Path;

use super::buffer::BufferPool;
use super::page::PageId;
use super::slotted_page::SlottedPage;

// ============================================================
// RecordId — 全局唯一的行定位符
// ============================================================

/// 记录 ID：定位一条记录在文件中的确切位置。
///
/// `(page_id, slot_id)` 就像一个二维坐标：
/// - 先找到哪个 Page（门牌号）
/// - 再找到 Page 内的哪个 Slot（房间号）
///
/// # 类比 Java
/// ```java
/// // 类似 JDBC 的 RowId
/// class RecordId {
///     int pageId;
///     short slotId;
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RecordId {
    /// 页编号
    pub page_id: PageId,
    /// 页内槽位编号
    pub slot_id: u16,
}

// ============================================================
// HeapFile — 堆文件
// ============================================================

/// 堆文件：管理一张表的所有 Page。
///
/// HeapFile 拥有一个 BufferPool，通过它来获取和管理 Page。
/// 每个 Page 内部用 SlottedPage 格式存储变长记录。
///
/// ## 字段说明
/// - `buffer_pool`: 缓冲池，管理 Page 的内存缓存
/// - `num_pages`: 当前文件中有多少个 Page
///
/// ## 设计说明
/// 在我们的简化实现中，HeapFile 拥有 BufferPool（而不是引用），
/// 这意味着每个表有自己的 BufferPool。真实数据库通常共享一个全局 BufferPool。
pub struct HeapFile {
    /// 缓冲池
    buffer_pool: BufferPool,
    /// 当前 Page 数量
    num_pages: u32,
}

impl HeapFile {
    /// 创建一个新的 HeapFile。
    ///
    /// # 参数
    /// - `path`: 数据库文件路径
    /// - `pool_size`: BufferPool 大小（Frame 数量）
    ///
    /// # TODO 1: 实现这个函数
    ///
    /// ```rust,ignore
    /// // 1. 创建 BufferPool
    /// // 2. num_pages 初始化为 0
    /// //   （简化处理——真实数据库会从文件头读取）
    /// ```
    pub fn new<P: AsRef<Path>>(path: P, pool_size: usize) -> io::Result<Self> {
        // TODO: 创建 BufferPool 并初始化 HeapFile
        // 提示：类似 BufferPool::new，就 3-4 行代码
        let buffer_pool = BufferPool::new(path, pool_size)?;
        let num_pages = 0;
        Ok(Self {
            buffer_pool,
            num_pages,
        })
    }

    /// 插入一条记录，返回 RecordId。
    ///
    /// # 流程
    /// ```text
    /// insert_record(data):
    ///   1. 如果有 Page（num_pages > 0）：
    ///      a. fetch 最后一个 Page (page_id = num_pages - 1)
    ///      b. 用 SlottedPage::new() 创建视图
    ///      c. 尝试 insert(data)
    ///      d. 如果成功 → unpin（dirty=true），返回 RecordId
    ///      e. 如果空间不足 → unpin（dirty=false），继续到步骤 2
    ///
    ///   2. 分配新 Page：
    ///      a. buffer_pool.new_page()
    ///      b. fetch 新 Page
    ///      c. SlottedPage::init() 初始化页头
    ///      d. SlottedPage::insert(data) 插入记录
    ///      e. num_pages += 1
    ///      f. unpin（dirty=true）两次（new_page 和 fetch 各 pin 一次）
    ///      g. 返回 RecordId
    /// ```
    ///
    /// # TODO 2: 实现这个函数（最复杂的）
    pub fn insert_record(&mut self, data: &[u8]) -> io::Result<RecordId> {
        // TODO: 按照上面的流程实现
        //
        // 提示1：SlottedPage::new(page.data_mut()) 创建视图
        // 提示2：insert 返回 Option<u16>，None 表示空间不足
        // 提示3：注意 pin/unpin 的配对——每次 fetch_page 都要对应一次 unpin_page
        // 提示4：new_page 也会 pin，所以新分配的页需要 unpin 两次
        if self.num_pages > 0 {
            let page_id = self.num_pages - 1;
            let page = self.buffer_pool.fetch_page(page_id)?;
            let mut slotted = SlottedPage::new(page.data_mut());
            if let Some(slot_id) = slotted.insert(data) {
                self.buffer_pool.unpin_page(page_id, true);
                return Ok(RecordId { page_id, slot_id });
            }
            // 空间不足，unpin 这个页（没修改，dirty=false）
            self.buffer_pool.unpin_page(page_id, false);
        }
        let page_id = self.buffer_pool.new_page()?; // pin 第1次
        let page = self.buffer_pool.fetch_page(page_id)?; // pin 第2次
        let mut slotted = SlottedPage::new(page.data_mut());
        slotted.init();
        let slot_id = match slotted.insert(data) {
            Some(sid) => sid,
            None => {
                // 记录超过单页容量，先释放两次 pin 再返回错误
                self.buffer_pool.unpin_page(page_id, false); // unpin fetch_page
                self.buffer_pool.unpin_page(page_id, false); // unpin new_page
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "record too large to fit in a single page",
                ));
            }
        };
        self.buffer_pool.unpin_page(page_id, true); // unpin fetch_page（页已修改）
        self.buffer_pool.unpin_page(page_id, false); // unpin new_page（仅释放 pin）
        self.num_pages += 1;
        Ok(RecordId { page_id, slot_id })
    }

    /// 按 RecordId 读取一条记录。
    ///
    /// # 返回
    /// - `Ok(Some(Vec<u8>))`: 成功读取
    /// - `Ok(None)`: 记录不存在或已删除
    /// - `Err`: I/O 错误
    ///
    /// # TODO 3: 实现这个函数
    ///
    /// ```rust,ignore
    /// // 1. fetch_page(record_id.page_id)
    /// // 2. SlottedPage::new(page.data_mut())
    /// // 3. slotted.get(record_id.slot_id)
    /// // 4. 如果有值，复制一份 (to_vec()) 并 unpin → 返回 Some
    /// //    如果 None → unpin → 返回 None
    /// ```
    ///
    /// 注意：要在 unpin 之前把数据复制出来，因为 unpin 后 Page 可能被淘汰！
    pub fn get_record(&mut self, record_id: &RecordId) -> io::Result<Option<Vec<u8>>> {
        if record_id.page_id >= self.num_pages {
            return Ok(None);
        }
        let page = self.buffer_pool.fetch_page(record_id.page_id)?;
        let slotted = SlottedPage::new(page.data_mut());
        let data = slotted.get(record_id.slot_id).map(|d| d.to_vec());
        self.buffer_pool.unpin_page(record_id.page_id, false);
        Ok(data)
    }

    /// 全表扫描：返回所有有效记录。
    ///
    /// # 返回
    /// `Vec<(RecordId, Vec<u8>)>` — 每个元素是 (记录ID, 记录字节数据)
    ///
    /// # TODO 4: 实现这个函数
    ///
    /// ```rust,ignore
    /// // 1. 创建结果 Vec
    /// // 2. for page_id in 0..self.num_pages:
    /// //    a. fetch_page(page_id)
    /// //    b. SlottedPage::new(page.data_mut())
    /// //    c. for slot_id in 0..slotted.num_slots():
    /// //       if let Some(data) = slotted.get(slot_id):
    /// //         results.push((RecordId { page_id, slot_id }, data.to_vec()))
    /// //    d. unpin_page(page_id, false)
    /// // 3. 返回 results
    /// ```
    pub fn scan(&mut self) -> io::Result<Vec<(RecordId, Vec<u8>)>> {
        let mut results = Vec::new();
        for page_id in 0..self.num_pages {
            let page = self.buffer_pool.fetch_page(page_id)?;
            let slotted = SlottedPage::new(page.data_mut());
            for slot_id in 0..slotted.num_slots() {
                if let Some(data) = slotted.get(slot_id) {
                    results.push((RecordId { page_id, slot_id }, data.to_vec()));
                }
            }
            self.buffer_pool.unpin_page(page_id, false);
        }
        Ok(results)
    }
}

// ============================================================
// 🧪 单元测试
// ============================================================
// 运行测试: cargo test storage::heap
//
// 这些测试覆盖了 HeapFile 的核心功能。
// 注意：这些测试依赖你的 SlottedPage 实现！
// 请确保先完成 1.4（slotted_page.rs）再跑这些测试。
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_db_path(name: &str) -> String {
        format!("/tmp/toydb_test_heap_{}.db", name)
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    // ---- 测试 1: 创建 HeapFile ----
    #[test]
    fn test_new_heap_file() {
        let path = test_db_path("new");
        cleanup(&path);

        let heap = HeapFile::new(&path, 8);
        assert!(heap.is_ok());

        cleanup(&path);
    }

    // ---- 测试 2: 插入并读取一条记录 ----
    #[test]
    fn test_insert_and_get() {
        let path = test_db_path("insert_get");
        cleanup(&path);

        let mut heap = HeapFile::new(&path, 8).unwrap();

        let data = b"Hello, HeapFile!";
        let rid = heap.insert_record(data).unwrap();
        assert_eq!(rid.page_id, 0);
        assert_eq!(rid.slot_id, 0);

        let result = heap.get_record(&rid).unwrap();
        assert_eq!(result.unwrap(), data);

        cleanup(&path);
    }

    // ---- 测试 3: 插入多条记录 ----
    #[test]
    fn test_multiple_inserts() {
        let path = test_db_path("multi_insert");
        cleanup(&path);

        let mut heap = HeapFile::new(&path, 8).unwrap();

        let r0 = heap.insert_record(b"Record 0").unwrap();
        let r1 = heap.insert_record(b"Record 1").unwrap();
        let r2 = heap.insert_record(b"Record 2").unwrap();

        assert_eq!(heap.get_record(&r0).unwrap().unwrap(), b"Record 0");
        assert_eq!(heap.get_record(&r1).unwrap().unwrap(), b"Record 1");
        assert_eq!(heap.get_record(&r2).unwrap().unwrap(), b"Record 2");

        cleanup(&path);
    }

    // ---- 测试 4: 全表扫描 ----
    #[test]
    fn test_scan() {
        let path = test_db_path("scan");
        cleanup(&path);

        let mut heap = HeapFile::new(&path, 8).unwrap();

        heap.insert_record(b"Alice").unwrap();
        heap.insert_record(b"Bob").unwrap();
        heap.insert_record(b"Charlie").unwrap();

        let results = heap.scan().unwrap();
        assert_eq!(results.len(), 3);

        // 验证内容（顺序应该和插入顺序一致）
        assert_eq!(results[0].1, b"Alice");
        assert_eq!(results[1].1, b"Bob");
        assert_eq!(results[2].1, b"Charlie");

        cleanup(&path);
    }

    // ---- 测试 5: 跨页插入 ----
    #[test]
    fn test_cross_page_insert() {
        let path = test_db_path("cross_page");
        cleanup(&path);

        let mut heap = HeapFile::new(&path, 8).unwrap();

        // 插入大记录，强制跨页
        let big_record = vec![0xAB_u8; 2048]; // 2048 字节，一页放不下两条
        let r0 = heap.insert_record(&big_record).unwrap();
        let r1 = heap.insert_record(&big_record).unwrap();

        // 应该在不同的 Page 上
        assert_ne!(r0.page_id, r1.page_id);

        // 都能正确读回
        assert_eq!(heap.get_record(&r0).unwrap().unwrap(), big_record);
        assert_eq!(heap.get_record(&r1).unwrap().unwrap(), big_record);

        cleanup(&path);
    }

    // ---- 测试 6: 扫描包含跨页的记录 ----
    #[test]
    fn test_scan_multiple_pages() {
        let path = test_db_path("scan_multi_page");
        cleanup(&path);

        let mut heap = HeapFile::new(&path, 8).unwrap();

        // 插入足够多的记录，使得跨越多个 Page
        let record = vec![0xCD_u8; 500]; // 每条 500 字节
        for _ in 0..20 {
            heap.insert_record(&record).unwrap();
        }

        let results = heap.scan().unwrap();
        assert_eq!(results.len(), 20);

        // 所有记录内容应该一致
        for (_, data) in &results {
            assert_eq!(data, &record);
        }

        cleanup(&path);
    }

    // ---- 测试 7: 读取不存在的记录 ----
    #[test]
    fn test_get_nonexistent() {
        let path = test_db_path("nonexistent");
        cleanup(&path);

        let mut heap = HeapFile::new(&path, 8).unwrap();
        heap.insert_record(b"something").unwrap();

        // slot_id 1 不存在
        let rid = RecordId {
            page_id: 0,
            slot_id: 1,
        };
        let result = heap.get_record(&rid).unwrap();
        assert!(result.is_none());

        cleanup(&path);
    }
}
