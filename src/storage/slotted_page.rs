//! # SlottedPage —— 页内变长记录管理
//!
//! SlottedPage 在一个 4096 字节的 Page 中管理多条变长记录。
//! 它不是一个独立的数据结构，而是对 Page data 的一种"解释方式"。
//!
//! ## 页内布局
//! ```text
//!  低地址                                              高地址
//! ┌────────┬──────────────────┬─────────────┬──────────────┐
//! │ Header │ Slot Array → →   │ Free Space  │ ← ← Records │
//! │ (8B)   │ [off,len] × N   │             │ rec_N...rec_0│
//! └────────┴──────────────────┴─────────────┴──────────────┘
//!          ↑ free_start                 free_end ↑
//! ```
//!
//! ## 使用示例
//! ```no_run
//! use toydb::storage::page::Page;
//! use toydb::storage::slotted_page::SlottedPage;
//!
//! let mut page = Page::new(0);
//! let mut sp = SlottedPage::new(page.data_mut());
//! sp.init(); // 初始化页头
//!
//! // 插入记录
//! let slot_id = sp.insert(b"Hello, SlottedPage!").unwrap();
//!
//! // 读取记录
//! let data = sp.get(slot_id).unwrap();
//! assert_eq!(data, b"Hello, SlottedPage!");
//!
//! // 删除记录
//! sp.delete(slot_id);
//! assert!(sp.get(slot_id).is_none());
//! ```

use std::u16;

use crate::storage::page::PAGE_SIZE;

// ============================================================
// 常量定义
// ============================================================

/// Header 大小：8 字节
/// [num_slots: u16][free_start: u16][free_end: u16][reserved: u16]
const HEADER_SIZE: usize = 8;

/// 每个 Slot 的大小：4 字节
/// [offset: u16][length: u16]
const SLOT_SIZE: usize = 4;

// ============================================================
// 辅助函数：字节级读写
// ============================================================

/// 从字节切片中读取一个 u16（小端序）
///
/// # 类比 Java
/// 就像 `ByteBuffer.order(ByteOrder.LITTLE_ENDIAN).getShort(offset)`
fn read_u16(data: &[u8], offset: usize) -> u16 {
    let bytes = [data[offset], data[offset + 1]];
    u16::from_le_bytes(bytes)
}

/// 向字节切片中写入一个 u16（小端序）
///
/// # 类比 Java
/// 就像 `ByteBuffer.order(ByteOrder.LITTLE_ENDIAN).putShort(offset, value)`
fn write_u16(data: &mut [u8], offset: usize, value: u16) {
    let bytes = value.to_le_bytes();
    data[offset] = bytes[0];
    data[offset + 1] = bytes[1];
}

// ============================================================
// SlottedPage 结构体
// ============================================================

/// SlottedPage 是对 Page data 的一个"视图"。
///
/// 它不拥有数据，而是通过可变引用借用 Page 的 4096 字节数组，
/// 按照 Header + Slot Array + Record Data 的布局来解释和操作这些字节。
///
/// 生命周期 `'a` 表示 SlottedPage 的生命周期不能超过它所借用的 data。
/// 这类似于 Java 中 ByteBuffer 的 wrap()：
/// ```java
/// ByteBuffer buf = ByteBuffer.wrap(page.data); // buf 是 data 的视图
/// ```
pub struct SlottedPage<'a> {
    data: &'a mut [u8],
}

impl<'a> SlottedPage<'a> {
    /// 创建一个 SlottedPage 视图。
    ///
    /// **注意**：这只是创建视图，不会初始化页头。
    /// 如果是全新的 Page，需要调用 `init()` 来初始化。
    pub fn new(data: &'a mut [u8]) -> Self {
        SlottedPage { data }
    }

    // ========================================================
    // Header 读写辅助方法（已实现）
    // ========================================================

    /// 读取 num_slots（当前有多少个 slot，包括已删除的）
    pub fn num_slots(&self) -> u16 {
        read_u16(self.data, 0)
    }

    /// 写入 num_slots
    fn set_num_slots(&mut self, val: u16) {
        write_u16(self.data, 0, val);
    }

    /// 读取 free_start（Slot Array 末尾位置）
    fn free_start(&self) -> u16 {
        read_u16(self.data, 2)
    }

    /// 写入 free_start
    fn set_free_start(&mut self, val: u16) {
        write_u16(self.data, 2, val);
    }

    /// 读取 free_end（Record 区域开头位置）
    fn free_end(&self) -> u16 {
        read_u16(self.data, 4)
    }

    /// 写入 free_end
    fn set_free_end(&mut self, val: u16) {
        write_u16(self.data, 4, val);
    }

    // ========================================================
    // Slot 读写辅助方法（已实现）
    // ========================================================

    /// 读取指定 slot 的 (offset, length)
    ///
    /// Slot 在数据中的位置：HEADER_SIZE + slot_id * SLOT_SIZE
    fn get_slot(&self, slot_id: u16) -> (u16, u16) {
        let pos = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        let offset = read_u16(self.data, pos);
        let length = read_u16(self.data, pos + 2);
        (offset, length)
    }

    /// 写入指定 slot 的 (offset, length)
    fn set_slot(&mut self, slot_id: u16, offset: u16, length: u16) {
        let pos = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        write_u16(self.data, pos, offset);
        write_u16(self.data, pos + 2, length);
    }

    // ========================================================
    // 🎯 以下是你需要实现的 5 个核心方法
    // ========================================================

    /// 初始化一个空白页为 SlottedPage 格式。
    ///
    /// # 行为
    /// 设置 Header 的三个字段：
    /// - `num_slots = 0`（还没有任何 slot）
    /// - `free_start = HEADER_SIZE`（Slot Array 紧接 Header 之后）
    /// - `free_end = PAGE_SIZE`（Record 区域从页尾开始）
    ///
    /// # TODO 1: 实现这个函数
    /// 使用上面提供的 set_num_slots / set_free_start / set_free_end 方法
    pub fn init(&mut self) {
        // TODO: 初始化三个 Header 字段
        // 提示：一共就 3 行代码
        self.set_num_slots(0);
        self.set_free_start(HEADER_SIZE as u16);
        self.set_free_end(PAGE_SIZE as u16);
    }

    /// 插入一条记录，返回 slot_id。
    ///
    /// # 流程
    /// ```text
    /// 1. 计算需要的空间 = record.len() + SLOT_SIZE(4)
    ///    (需要存记录本身 + 新的 Slot 条目)
    ///
    /// 2. 检查 free_end - free_start >= 需要的空间？
    ///    └─ 不够 → 返回 None
    ///
    /// 3. free_end -= record.len()  (记录从页尾向左写)
    ///
    /// 4. 把 record 的字节复制到 data[free_end..free_end+len]
    ///    提示：self.data[start..end].copy_from_slice(record)
    ///
    /// 5. 写入新的 Slot: set_slot(num_slots, free_end, record.len())
    ///
    /// 6. 更新 Header:
    ///    - num_slots += 1
    ///    - free_start += SLOT_SIZE
    ///    - free_end 已经在步骤 3 更新了
    ///
    /// 7. 返回 Some(slot_id)  (slot_id = old num_slots)
    /// ```
    ///
    /// # TODO 2: 实现这个函数（最复杂的一个）
    pub fn insert(&mut self, record: &[u8]) -> Option<u16> {
        // TODO: 按照上面的 7 个步骤实现
        //
        // 提示1：先读取当前的 num_slots, free_start, free_end
        // 提示2：计算 needed_space 时注意包含 SLOT_SIZE
        // 提示3：注意类型转换 —— record.len() 是 usize，Header 里存的是 u16
        //        用 `as u16` 或 `record.len() as u16` 进行转换
        let num_slots = self.num_slots();
        let free_start = self.free_start();
        let free_end = self.free_end();
        let needed_space = record.len() as u16 + SLOT_SIZE as u16;
        if (free_end - free_start) < needed_space {
            return None;
        }
        let new_free_end = free_end - record.len() as u16;
        self.set_free_end(new_free_end);
        // 步骤 4：把 record 数据复制到 data 里！
        self.data[new_free_end as usize..(new_free_end as usize + record.len())]
            .copy_from_slice(record);
        self.set_slot(num_slots, new_free_end, record.len() as u16);
        self.set_num_slots(num_slots + 1);
        self.set_free_start(free_start + SLOT_SIZE as u16);
        Some(num_slots)
    }

    /// 按 slot_id 读取一条记录。
    ///
    /// # 返回
    /// - `Some(&[u8])`: 记录的字节切片
    /// - `None`: slot_id 无效或记录已被删除
    ///
    /// # 流程
    /// ```text
    /// 1. slot_id >= num_slots? → 返回 None
    /// 2. 读取 Slot 的 (offset, length)
    /// 3. offset == 0 且 length == 0? → 已删除，返回 None
    /// 4. 返回 data[offset..offset+length]
    /// ```
    ///
    /// # TODO 3: 实现这个函数
    pub fn get(&self, slot_id: u16) -> Option<&[u8]> {
        // TODO: 按照上面的 4 个步骤实现
        //
        // 提示：返回 &self.data[start..end] 即可
        //       Rust 会自动推导生命周期
        let num_slots = self.num_slots();
        if slot_id >= num_slots {
            return None;
        }
        let (offset, length) = self.get_slot(slot_id);
        if offset == 0 && length == 0 {
            return None;
        }
        Some(&self.data[offset as usize..(offset + length) as usize])
    }

    /// 按 slot_id 删除一条记录（标记删除）。
    ///
    /// # 行为
    /// 将对应 Slot 的 offset 和 length 都设为 0。
    /// **不移动任何数据**——记录留在原地成为碎片。
    ///
    /// # TODO 4: 实现这个函数
    /// 一行就够了，使用 set_slot 方法
    pub fn delete(&mut self, slot_id: u16) {
        // TODO: 标记删除
        // 提示：直接用 set_slot(slot_id, 0, 0)
        self.set_slot(slot_id, 0, 0);
    }

    /// 返回当前可用的空闲空间（字节数）。
    ///
    /// 空闲空间 = free_end - free_start
    ///
    /// 注意：这是**连续可用空间**，不包括删除记录后留下的碎片空间。
    ///
    /// # TODO 5: 实现这个函数
    pub fn free_space(&self) -> usize {
        // TODO: 返回 free_end - free_start
        // 提示：注意 u16 → usize 的转换
        (self.free_end() - self.free_start()) as usize
    }
}

// ============================================================
// 🧪 单元测试
// ============================================================
// 运行测试: cargo test storage::slotted_page
//
// 从简单到复杂，逐步验证你的实现。
// 建议先看测试理解预期行为，再填 TODO。
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    /// 创建一个初始化好的 SlottedPage（测试辅助函数）
    fn make_page() -> [u8; PAGE_SIZE] {
        let mut data = [0u8; PAGE_SIZE];
        let mut sp = SlottedPage::new(&mut data);
        sp.init();
        data
    }

    // ---- 测试 1: 初始化 ----
    #[test]
    fn test_init() {
        let mut data = [0u8; PAGE_SIZE];
        let mut sp = SlottedPage::new(&mut data);
        sp.init();

        assert_eq!(sp.num_slots(), 0);
        assert_eq!(sp.free_start(), HEADER_SIZE as u16);
        assert_eq!(sp.free_end(), PAGE_SIZE as u16);
        assert_eq!(sp.free_space(), PAGE_SIZE - HEADER_SIZE);
    }

    // ---- 测试 2: 插入一条记录并读回 ----
    #[test]
    fn test_insert_and_get() {
        let mut data = make_page();
        let mut sp = SlottedPage::new(&mut data);

        let record = b"Hello, SlottedPage!";
        let slot_id = sp.insert(record).unwrap();
        assert_eq!(slot_id, 0);

        let result = sp.get(slot_id).unwrap();
        assert_eq!(result, record);
    }

    // ---- 测试 3: 连续插入多条记录 ----
    #[test]
    fn test_multiple_inserts() {
        let mut data = make_page();
        let mut sp = SlottedPage::new(&mut data);

        let s0 = sp.insert(b"Alice").unwrap();
        let s1 = sp.insert(b"Bob").unwrap();
        let s2 = sp.insert(b"Charlie").unwrap();

        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);

        assert_eq!(sp.get(s0).unwrap(), b"Alice");
        assert_eq!(sp.get(s1).unwrap(), b"Bob");
        assert_eq!(sp.get(s2).unwrap(), b"Charlie");

        assert_eq!(sp.num_slots(), 3);
    }

    // ---- 测试 4: 删除后 get 返回 None ----
    #[test]
    fn test_delete() {
        let mut data = make_page();
        let mut sp = SlottedPage::new(&mut data);

        let s0 = sp.insert(b"to be deleted").unwrap();
        assert!(sp.get(s0).is_some());

        sp.delete(s0);
        assert!(sp.get(s0).is_none());

        // num_slots 不变（slot 还在，只是标记删除）
        assert_eq!(sp.num_slots(), 1);
    }

    // ---- 测试 5: 删除后可以插入新记录 ----
    #[test]
    fn test_insert_after_delete() {
        let mut data = make_page();
        let mut sp = SlottedPage::new(&mut data);

        let s0 = sp.insert(b"first").unwrap();
        sp.delete(s0);

        // 新记录应该可以插入（虽然有碎片，但还有连续空间）
        let s1 = sp.insert(b"second").unwrap();
        assert_eq!(sp.get(s1).unwrap(), b"second");
    }

    // ---- 测试 6: 页满时返回 None ----
    #[test]
    fn test_page_full() {
        let mut data = make_page();
        let mut sp = SlottedPage::new(&mut data);

        // 不断插入直到满
        let big_record = vec![0xAB_u8; 100]; // 每条 100 字节
        let mut count = 0;
        while sp.insert(&big_record).is_some() {
            count += 1;
        }

        // 应该能插入一些记录，但最终会满
        assert!(count > 0);
        // 满了之后 insert 返回 None
        assert!(sp.insert(&big_record).is_none());
    }

    // ---- 测试 7: free_space 计算正确 ----
    #[test]
    fn test_free_space() {
        let mut data = make_page();
        let mut sp = SlottedPage::new(&mut data);

        let initial_free = sp.free_space();
        assert_eq!(initial_free, PAGE_SIZE - HEADER_SIZE);

        // 插入一条 10 字节的记录，应该减少 10(记录) + 4(slot) = 14 字节
        sp.insert(&[0u8; 10]).unwrap();
        assert_eq!(sp.free_space(), initial_free - 10 - SLOT_SIZE);
    }

    // ---- 测试 8: 越界 slot_id 返回 None ----
    #[test]
    fn test_get_invalid_slot() {
        let mut data = make_page();
        let sp = SlottedPage::new(&mut data);

        assert!(sp.get(0).is_none()); // 没有任何记录
        assert!(sp.get(999).is_none()); // 远超范围
    }
}
