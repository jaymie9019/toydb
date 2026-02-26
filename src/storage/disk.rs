//! # DiskManager —— 磁盘页面读写管理器
//!
//! DiskManager 负责将 Page 从磁盘文件读入内存，以及将脏页写回磁盘。
//! 它是存储引擎最底层的 I/O 组件。
//!
//! ## 核心职责
//! - **read_page**: 根据 PageId 从文件中读取一个 Page
//! - **write_page**: 将一个 Page 写回文件中对应的位置
//! - **allocate_page**: 分配一个新的 PageId
//!
//! ## 寻址原理
//! ```text
//! PageId 0 → 文件偏移 0
//! PageId 1 → 文件偏移 4096
//! PageId 2 → 文件偏移 8192
//! 公式: offset = page_id * PAGE_SIZE
//! ```
//!
//! ## 使用示例
//! ```no_run
//! use toydb::storage::disk::DiskManager;
//! use toydb::storage::page::Page;
//!
//! let mut dm = DiskManager::new("test.db").unwrap();
//! let page_id = dm.allocate_page();
//!
//! let mut page = Page::new(page_id);
//! page.write_bytes(0, b"Hello!");
//! dm.write_page(&page).unwrap();
//!
//! let mut loaded = Page::new(page_id);
//! dm.read_page(&mut loaded).unwrap();
//! assert_eq!(loaded.read_bytes(0, 6), b"Hello!");
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::page::{PAGE_SIZE, Page, PageId};

/// 磁盘管理器
///
/// 管理一个数据库文件，提供按 Page 粒度的读写操作。
///
/// ## 字段说明
/// - `file`: 底层数据库文件句柄（Java 类比: `RandomAccessFile`）
/// - `next_page_id`: 下一个可分配的 PageId（单调递增）
pub struct DiskManager {
    /// 数据库文件句柄
    file: File,
    /// 下一个可用的页编号
    /// 每次 allocate_page() 后自增 1
    next_page_id: PageId,
}

impl DiskManager {
    /// 创建一个新的 DiskManager，打开（或创建）指定的数据库文件。
    ///
    /// # 参数
    /// - `path`: 数据库文件路径（如 "data.db"）
    ///
    /// # 返回
    /// - `Ok(DiskManager)`: 成功打开/创建文件
    /// - `Err(io::Error)`: 文件操作失败
    ///
    /// # TODO: 实现这个函数
    /// 需要做两件事：
    /// 1. 用 OpenOptions 打开文件（read + write + create）
    /// 2. 通过文件大小计算 next_page_id（文件大小 / PAGE_SIZE）
    ///
    /// 提示：
    /// ```rust,ignore
    /// let file = OpenOptions::new()
    ///     .read(true)
    ///     .write(true)
    ///     .create(true)
    ///     .open(path)?;
    /// ```
    /// 获取文件大小：`file.metadata()?.len()` 返回 u64
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        // TODO:
        // 1. 打开文件（读写模式，不存在则创建）
        // 2. 获取文件大小
        // 3. 计算 next_page_id = 文件大小 / PAGE_SIZE
        // 4. 返回 DiskManager 实例
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let file_size = file.metadata()?.len();
        let next_page_id = (file_size / PAGE_SIZE as u64) as u32;
        Ok(Self { file, next_page_id })
    }

    /// 从文件中读取一个 Page。
    ///
    /// 根据 page.id 计算文件偏移量，将数据读入 page 的 data 中。
    ///
    /// # 参数
    /// - `page`: 要读入数据的 Page（page.id 必须已设置）
    ///
    /// # 实现步骤
    /// 1. 计算偏移量: `offset = page.id * PAGE_SIZE`
    /// 2. seek 到该偏移量
    /// 3. 读取 PAGE_SIZE 字节到 page.data_mut()
    ///
    /// # TODO: 实现这个函数
    /// 提示：
    /// - `self.file.seek(SeekFrom::Start(offset))` 跳转到指定位置
    /// - `self.file.read_exact(buf)` 从文件读满整个 buffer
    ///   如果文件没有足够的字节，read_exact 会返回 Err
    pub fn read_page(&mut self, page: &mut Page) -> io::Result<()> {
        // TODO:
        // 1. 计算偏移量 offset (注意类型转换：PageId 是 u32，偏移量需要 u64)
        // 2. seek 到偏移量位置
        // 3. 用 read_exact 读取数据到 page.data_mut()
        let offset = page.id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(page.data_mut())?;
        Ok(())
    }

    /// 将一个 Page 写入文件。
    ///
    /// 根据 page.id 计算文件偏移量，将 page 的 data 写入文件。
    ///
    /// # 参数
    /// - `page`: 要写入磁盘的 Page
    ///
    /// # 实现步骤
    /// 1. 计算偏移量: `offset = page.id * PAGE_SIZE`
    /// 2. seek 到该偏移量
    /// 3. 将 page.data() 写入文件
    ///
    /// # TODO: 实现这个函数
    /// 提示：
    /// - `self.file.write_all(data)` 将所有字节写入文件
    /// - 思考：写完后要不要调用 `self.file.sync_all()`？
    ///   (sync_all 相当于 Java 的 fd.sync()，强制刷盘)
    ///   在真实数据库中这很重要，但现阶段为了性能可以先不加。
    pub fn write_page(&mut self, page: &Page) -> io::Result<()> {
        // TODO:
        // 1. 计算偏移量
        // 2. seek 到偏移量位置
        // 3. 用 write_all 将 page.data() 写入
        let offset = page.id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page.data())?;
        Ok(())
    }

    /// 分配一个新的 PageId。
    ///
    /// 简单地返回当前的 next_page_id，然后自增。
    ///
    /// # 返回
    /// 新分配的 PageId
    ///
    /// # TODO: 实现这个函数
    /// 这是最简单的一个——两行代码搞定。
    pub fn allocate_page(&mut self) -> PageId {
        // TODO:
        // 1. 保存当前 next_page_id
        // 2. 自增 next_page_id
        // 3. 返回保存的值
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        page_id
    }
}

// ============================================================
// 🧪 单元测试
// ============================================================
// 运行测试: cargo test storage::disk
//
// 注意：这些测试会创建临时文件，测试结束后自动清理。
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// 辅助函数：创建测试用的临时文件路径
    /// 每个测试用不同的文件名避免冲突
    fn test_db_path(name: &str) -> String {
        format!("/tmp/toydb_test_{}.db", name)
    }

    /// 辅助函数：清理测试文件
    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_new_creates_file() {
        let path = test_db_path("new");
        cleanup(&path);

        let dm = DiskManager::new(&path);
        assert!(dm.is_ok());

        // 文件应该被创建
        assert!(Path::new(&path).exists());

        cleanup(&path);
    }

    #[test]
    fn test_allocate_page_sequential() {
        let path = test_db_path("alloc");
        cleanup(&path);

        let mut dm = DiskManager::new(&path).unwrap();

        // 分配的 PageId 应该是递增的
        assert_eq!(dm.allocate_page(), 0);
        assert_eq!(dm.allocate_page(), 1);
        assert_eq!(dm.allocate_page(), 2);

        cleanup(&path);
    }

    #[test]
    fn test_write_and_read_page() {
        let path = test_db_path("rw");
        cleanup(&path);

        let mut dm = DiskManager::new(&path).unwrap();
        let page_id = dm.allocate_page();

        // 写入一个 Page
        let mut page = Page::new(page_id);
        page.write_bytes(0, b"Hello, DiskManager!");
        dm.write_page(&page).unwrap();

        // 读回来应该一致
        let mut loaded = Page::new(page_id);
        dm.read_page(&mut loaded).unwrap();
        assert_eq!(loaded.read_bytes(0, 19), b"Hello, DiskManager!");

        cleanup(&path);
    }

    #[test]
    fn test_write_multiple_pages() {
        let path = test_db_path("multi");
        cleanup(&path);

        let mut dm = DiskManager::new(&path).unwrap();

        // 写入 3 个不同的 Page
        for i in 0..3u32 {
            let page_id = dm.allocate_page();
            let mut page = Page::new(page_id);
            let msg = format!("Page {}", i);
            page.write_bytes(0, msg.as_bytes());
            dm.write_page(&page).unwrap();
        }

        // 按反序读取，验证每个 Page 的独立性
        for i in (0..3u32).rev() {
            let mut page = Page::new(i);
            dm.read_page(&mut page).unwrap();
            let expected = format!("Page {}", i);
            assert_eq!(page.read_bytes(0, expected.len()), expected.as_bytes());
        }

        cleanup(&path);
    }

    #[test]
    fn test_overwrite_page() {
        let path = test_db_path("overwrite");
        cleanup(&path);

        let mut dm = DiskManager::new(&path).unwrap();
        let page_id = dm.allocate_page();

        // 第一次写入
        let mut page = Page::new(page_id);
        page.write_bytes(0, b"Version 1");
        dm.write_page(&page).unwrap();

        // 覆盖写入
        let mut page2 = Page::new(page_id);
        page2.write_bytes(0, b"Version 2");
        dm.write_page(&page2).unwrap();

        // 读回来应该是最新版本
        let mut loaded = Page::new(page_id);
        dm.read_page(&mut loaded).unwrap();
        assert_eq!(loaded.read_bytes(0, 9), b"Version 2");

        cleanup(&path);
    }

    #[test]
    fn test_persistence_across_reopen() {
        let path = test_db_path("persist");
        cleanup(&path);

        // 第一次打开：写入数据
        {
            let mut dm = DiskManager::new(&path).unwrap();
            let page_id = dm.allocate_page();
            let mut page = Page::new(page_id);
            page.write_bytes(0, b"Persistent!");
            dm.write_page(&page).unwrap();
        }
        // DiskManager 被 drop，文件关闭

        // 第二次打开：数据应该还在
        {
            let mut dm = DiskManager::new(&path).unwrap();
            let mut page = Page::new(0);
            dm.read_page(&mut page).unwrap();
            assert_eq!(page.read_bytes(0, 11), b"Persistent!");
        }

        cleanup(&path);
    }

    #[test]
    fn test_reopen_preserves_page_count() {
        let path = test_db_path("count");
        cleanup(&path);

        // 写入 3 个 Page
        {
            let mut dm = DiskManager::new(&path).unwrap();
            for _ in 0..3 {
                let pid = dm.allocate_page();
                let mut p = Page::new(pid);
                p.write_bytes(0, b"data");
                dm.write_page(&p).unwrap();
            }
        }

        // 重新打开后，next_page_id 应该从 3 开始
        {
            let mut dm = DiskManager::new(&path).unwrap();
            assert_eq!(dm.allocate_page(), 3);
        }

        cleanup(&path);
    }
}
