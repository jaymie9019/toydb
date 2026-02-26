# ToyDB

一个从零开始构建的教学型关系数据库，使用 Rust 编写。项目的目的是通过逐步实现数据库核心组件来学习数据库内部原理。

## 项目结构

```
src/
├── lib.rs                  # 库入口
├── main.rs                 # 可执行入口
└── storage/                # 存储引擎层
    ├── mod.rs              # 模块声明
    ├── page.rs             # Page：固定 4KB 字节块，磁盘 I/O 最小单位
    ├── disk.rs             # DiskManager：负责 Page 的磁盘读写
    ├── buffer.rs           # BufferPool：内存缓存层，LRU 淘汰策略
    ├── slotted_page.rs     # SlottedPage：页内变长记录管理（Header + Slot Array + Records）
    ├── schema.rs           # Schema：表结构定义（DataType、Column）
    ├── tuple.rs            # Tuple：行数据的序列化/反序列化（NULL Bitmap + 列编码）
    └── heap.rs             # HeapFile：堆文件，管理多个 Page 组成一张表
tutorials/                  # Obsidian 教程笔记（软链接）
.agents/workflows/          # AI 辅助 workflow
```

## 技术栈

- **语言**: Rust (edition 2024)
- **构建工具**: Cargo
- **无外部依赖**，仅使用标准库

## 架构概览

存储引擎模块层次（自底向上）：

```
heap.rs          ← 堆文件：管理多个 Page 组成一张表，提供 insert/get/scan 接口
  tuple.rs       ← 行数据：Tuple ↔ Vec<u8> 序列化/反序列化
  schema.rs      ← 表结构：DataType、Column、Schema 定义
  slotted_page.rs ← 槽页：页内变长记录管理（Header + Slot Array + Records 三明治结构）
buffer.rs        ← 缓冲池：LRU 内存缓存，fetch_page / unpin_page
disk.rs          ← 磁盘管理器：文件级 Page 读写
page.rs          ← Page 定义：4096 字节固定大小块
```

## 关键类型

- `PageId` = `u32`：页编号
- `PAGE_SIZE` = `4096`：页大小（字节）
- `Page`：包含 `id`、`data: [u8; 4096]`、`dirty: bool`
- `DiskManager`：封装文件 `seek` + `read_exact` / `write_all`
- `BufferPool`：`frames` + `page_table` + `pin_counts` + `lru_list`
- `SlottedPage<'a>`：借用 Page data 的视图，Header(8B) + Slot(4B each) + Records
- `DataType`：`Integer | Float | Text | Boolean`
- `Value`：`Integer(i32) | Float(f64) | Text(String) | Boolean(bool) | Null`
- `Schema`：`columns: Vec<Column>`
- `Tuple`：`values: Vec<Value>`，支持 serialize/deserialize（NULL Bitmap + 列编码）
- `RecordId`：`{ page_id: PageId, slot_id: u16 }` 全局行定位符
- `HeapFile`：拥有 BufferPool，提供 insert_record/get_record/scan

## 开发规范

- 代码注释和文档使用**中文**
- 每个模块包含内联单元测试（`#[cfg(test)] mod tests`）
- 测试文件写入 `/tmp/toydb_test_*.db`，测试后清理
- 代码中使用 `TODO` 标记待实现的函数骨架

## 常用命令

```bash
# 运行所有测试
cargo test

# 运行存储层特定模块测试
cargo test storage::page
cargo test storage::disk
cargo test storage::buffer
cargo test storage::slotted_page
cargo test storage::schema
cargo test storage::tuple
cargo test storage::heap

# 编译检查
cargo check

# 运行项目
cargo run
```

## 教程体系

`tutorials/` 目录是一个 Obsidian vault 的软链接，包含分步教程：
1. **1.1** - Page 与磁盘存储基础 (5/10)
2. **1.2** - DiskManager 磁盘读写 (4/10)
3. **1.3** - BufferPool 缓冲池管理 (6/10)
4. **1.4** - SlottedPage 槽页数据组织 (7/10)
5. **1.5** - Tuple & Schema 行与表结构 (8/10)
6. **1.6** - HeapFile 堆文件管理 (8/10)

教程中使用 Obsidian 语法（wikilinks、callouts、Excalidraw 图表）。可以使用 obsidian 相关 skill 对仓库进行操作
