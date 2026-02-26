//! # 存储引擎模块
//!
//! 这是 ToyDB 的最底层模块，负责数据在磁盘上的组织和管理。
//!
//! ## 模块层次（自底向上）
//!
//! ```text
//! heap.rs          ← 堆文件：管理多个 Page
//! slotted_page.rs  ← 槽页：管理页内的多行数据
//! buffer.rs        ← 缓冲池：在内存中缓存 Page
//! disk.rs          ← 磁盘管理器：读写文件中的 Page
//! page.rs          ← 页：固定大小的字节块
//! ```

pub mod buffer;
pub mod disk;
pub mod heap;
pub mod page;
pub mod schema;
pub mod slotted_page;
pub mod tuple;
