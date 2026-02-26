//! # Page —— 固定大小的字节缓冲区
//!
//! Page 是数据库与磁盘交互的最小单位。
//!
//! ## 核心概念
//! - 每个 Page 大小固定为 `PAGE_SIZE`（4096 字节）
//! - Page 通过 `PageId`（u32）唯一标识
//! - Page 的内容就是一个 `[u8; PAGE_SIZE]` 字节数组
//!
//! ## 你需要实现的功能
//! 1. `Page::new()` — 创建一个全零的新页
//! 2. `Page::read_bytes()` — 从页中读取指定位置的字节
//! 3. `Page::write_bytes()` — 向页中写入字节到指定位置
//! 4. `Page::data()` — 返回页的原始字节引用
//!
//! ## 使用示例
//! ```
//! use toydb::storage::page::{Page, PAGE_SIZE};
//!
//! let mut page = Page::new(0);
//! page.write_bytes(0, b"Hello, ToyDB!");
//! let data = page.read_bytes(0, 13);
//! assert_eq!(data, b"Hello, ToyDB!");
//! ```

/// 页大小：4096 字节 (4KB)
/// 这是数据库与磁盘交互的最小 I/O 单位。
/// 大多数真实数据库（PostgreSQL, SQLite）也使用 4KB 或 8KB。
pub const PAGE_SIZE: usize = 4096;

/// 页 ID 类型。每个页有一个唯一的编号。
/// 用 u32 可以寻址 4GB × 4KB = 16TB 的数据，对我们的 ToyDB 绰绰有余。
pub type PageId = u32;

/// 表示一个无效的页 ID，类似于 null pointer。
/// 在链表等结构中用来表示"没有下一页"。
pub const INVALID_PAGE_ID: PageId = u32::MAX;

/// Page 结构体
///
/// 一个 Page 就是一块固定大小的字节数组，加上一个 ID 和一个脏标记。
///
/// - `id`：这个页在文件中的编号（第几页）
/// - `data`：4096 字节的内容
/// - `dirty`：是否被修改过（修改后需要写回磁盘）
pub struct Page {
    /// 页编号
    pub id: PageId,
    /// 页的原始数据（固定 4096 字节）
    data: [u8; PAGE_SIZE],
    /// 脏标记：如果 data 被修改过，这个要设为 true
    /// 缓冲池在淘汰此页时，如果 dirty=true，必须先写回磁盘
    pub dirty: bool,
}

impl Page {
    /// 创建一个新的 Page，内容全部初始化为 0。
    ///
    /// # 参数
    /// - `id`: 页编号
    ///
    /// # TODO: 实现这个函数
    /// 提示：用 `[0u8; PAGE_SIZE]` 可以创建一个全零字节数组
    pub fn new(id: PageId) -> Self {
        // TODO: 创建并返回一个 Page 实例
        // - data 初始化为全零
        // - dirty 初始化为 false
        Self {
            id,
            data: [0; PAGE_SIZE],
            dirty: false,
        }
    }

    /// 从页中读取一段字节。
    ///
    /// # 参数
    /// - `offset`: 起始位置（从 0 开始）
    /// - `len`: 读取长度
    ///
    /// # 返回
    /// 返回 `&[u8]` 切片，包含从 offset 开始的 len 个字节
    ///
    /// # Panics
    /// 如果 `offset + len > PAGE_SIZE`，应该 panic（越界访问）
    ///
    /// # TODO: 实现这个函数
    /// 提示：Rust 的数组切片语法 `&self.data[start..end]`
    pub fn read_bytes(&self, offset: usize, len: usize) -> &[u8] {
        // TODO: 返回 data[offset..offset+len] 的切片
        // 思考：是否需要手动检查边界？Rust 的切片操作会自动 panic 吗？
        &self.data[offset..offset + len]
    }

    /// 向页中写入一段字节。
    ///
    /// # 参数
    /// - `offset`: 写入的起始位置
    /// - `bytes`: 要写入的字节数据
    ///
    /// # 行为
    /// 1. 将 `bytes` 复制到 `self.data[offset..offset+bytes.len()]`
    /// 2. 将 `self.dirty` 设为 `true`（页已被修改）
    ///
    /// # Panics
    /// 如果 `offset + bytes.len() > PAGE_SIZE`，应该 panic
    ///
    /// # TODO: 实现这个函数
    /// 提示：可以用 `self.data[start..end].copy_from_slice(bytes)` 来复制字节
    pub fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        // TODO:
        // 1. 把 bytes 复制到 self.data 的对应位置
        // 2. 标记 dirty = true
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
        self.dirty = true;
    }

    /// 获取整个页的数据的不可变引用。
    ///
    /// 通常用于将整个页写入磁盘文件。
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        // TODO: 返回 self.data 的引用
        &self.data
    }

    /// 获取整个页的数据的可变引用。
    ///
    /// 用于 DiskManager 从文件读取数据后直接填充到 Page 中。
    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        // TODO: 返回 self.data 的可变引用
        &mut self.data
    }
}

// ============================================================
// 🧪 单元测试
// ============================================================
// 运行测试: cargo test -p toydb storage::page
//
// 这些测试帮助你验证实现是否正确。
// 先阅读测试理解预期行为，再去实现上面的函数。
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page_is_zeroed() {
        let page = Page::new(0);
        assert_eq!(page.id, 0);
        assert!(!page.dirty);
        // 新页的所有字节应该都是 0
        assert!(page.data().iter().all(|&b| b == 0));
    }

    #[test]
    fn test_write_and_read_bytes() {
        let mut page = Page::new(1);

        // 写入 "Hello"
        let hello = b"Hello";
        page.write_bytes(0, hello);

        // 读回来应该一致
        let result = page.read_bytes(0, 5);
        assert_eq!(result, b"Hello");

        // 写入后页应该是脏的
        assert!(page.dirty);
    }

    #[test]
    fn test_write_at_offset() {
        let mut page = Page::new(2);

        // 在 offset=100 处写入数据
        page.write_bytes(100, b"ToyDB");

        // offset 0-99 应该还是 0
        let zeros = page.read_bytes(0, 100);
        assert!(zeros.iter().all(|&b| b == 0));

        // offset 100-104 应该是 "ToyDB"
        let result = page.read_bytes(100, 5);
        assert_eq!(result, b"ToyDB");
    }

    #[test]
    fn test_write_at_end_of_page() {
        let mut page = Page::new(3);

        // 在接近页尾的位置写入
        let data = b"End!";
        let offset = PAGE_SIZE - data.len();
        page.write_bytes(offset, data);

        let result = page.read_bytes(offset, data.len());
        assert_eq!(result, b"End!");
    }

    #[test]
    #[should_panic]
    fn test_write_overflow_panics() {
        let mut page = Page::new(4);
        // 写入超出页边界，应该 panic
        page.write_bytes(PAGE_SIZE - 2, b"Too long!");
    }

    #[test]
    #[should_panic]
    fn test_read_overflow_panics() {
        let page = Page::new(5);
        // 读取超出页边界，应该 panic
        page.read_bytes(PAGE_SIZE - 2, 10);
    }

    #[test]
    fn test_data_mut() {
        let mut page = Page::new(6);
        let data = page.data_mut();
        data[0] = 0xFF;
        data[1] = 0xAB;

        assert_eq!(page.read_bytes(0, 2), &[0xFF, 0xAB]);
    }

    #[test]
    fn test_multiple_writes() {
        let mut page = Page::new(7);

        page.write_bytes(0, b"First");
        page.write_bytes(10, b"Second");
        page.write_bytes(0, b"Over"); // 覆盖写

        // "First" 被 "Over" 部分覆盖 → "Overt"
        assert_eq!(page.read_bytes(0, 5), b"Overt");
        assert_eq!(page.read_bytes(10, 6), b"Second");
    }
}
