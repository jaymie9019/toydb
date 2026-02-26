# ToyDB 🗃️

一个从零开始构建的教学型关系数据库，使用 Rust 编写。

通过逐步实现数据库核心组件来学习数据库内部原理——从磁盘 I/O 到 SQL 执行，每一层都亲手搭建。

## 特点

- **纯 Rust 实现**，零外部依赖，仅使用标准库
- **自底向上**的学习路线，从字节到 SQL
- **配套教程**，每个模块都有详细的中文教程和测验

## 架构

```
┌──────────────────────────────────────────────┐
│         HeapFile（堆文件）                      │
│  insert_record() / get_record() / scan()     │
│                                              │
│  ┌──────────────┐  ┌───────────────────┐     │
│  │ Tuple         │  │ SlottedPage       │     │
│  │ serialize()   │  │ insert/get/delete │     │
│  │ deserialize() │  │                   │     │
│  └──────────────┘  └───────────────────┘     │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │         BufferPool（缓冲池）             │  │
│  │  fetch_page() / unpin_page() / LRU     │  │
│  │                                        │  │
│  │  ┌──────────────────────────────────┐  │  │
│  │  │     DiskManager（磁盘管理器）      │  │  │
│  │  │  read_page() / write_page()      │  │  │
│  │  │                                  │  │  │
│  │  │  ┌────────────────────────────┐  │  │  │
│  │  │  │   Page（4KB 页）            │  │  │  │
│  │  │  │   [u8; 4096] + dirty flag  │  │  │  │
│  │  │  └────────────────────────────┘  │  │  │
│  │  └──────────────────────────────────┘  │  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

## 项目结构

```
src/
├── lib.rs                  # 库入口
├── main.rs                 # 可执行入口
└── storage/                # 存储引擎层
    ├── mod.rs              # 模块声明
    ├── page.rs             # Page：固定 4KB 字节块
    ├── disk.rs             # DiskManager：磁盘读写
    ├── buffer.rs           # BufferPool：LRU 内存缓存
    ├── slotted_page.rs     # SlottedPage：页内变长记录管理
    ├── schema.rs           # Schema：表结构定义
    ├── tuple.rs            # Tuple：行数据序列化/反序列化
    └── heap.rs             # HeapFile：堆文件，多页表管理
```

## 学习路线

### Phase 1：存储引擎 ✅

| 课程 | 主题 | 核心概念 |
|------|------|---------|
| 1.1 | Page 与磁盘存储基础 | 固定大小页、字节数组 |
| 1.2 | DiskManager 磁盘读写 | 文件 I/O、页寻址 |
| 1.3 | BufferPool 缓冲池 | 内存缓存、LRU 淘汰、Pin/Unpin |
| 1.4 | SlottedPage 槽页 | 变长记录、Header + Slot Array + Records |
| 1.5 | Tuple & Schema | 行数据编解码、NULL Bitmap、表结构 |
| 1.6 | HeapFile 堆文件 | 多页管理、全表扫描、RecordId |

### Phase 2：SQL 解析与执行（计划中）
### Phase 3：查询优化（计划中）
### Phase 4：索引（计划中）

## 快速开始

```bash
# 编译
cargo build

# 运行所有测试
cargo test

# 运行特定模块测试
cargo test storage::page
cargo test storage::disk
cargo test storage::buffer
cargo test storage::slotted_page
cargo test storage::tuple
cargo test storage::heap
```

## 技术细节

- **页大小**: 4096 字节（4KB）
- **页编号**: `u32`，最大支持 16TB 数据
- **缓冲池**: LRU 淘汰策略，支持脏页回写
- **槽页格式**: Header(8B) + Slot Array(4B each) + Records，双向增长
- **行编码**: NULL Bitmap + 列值顺序编码，支持 Integer/Float/Text/Boolean/Null

## License

MIT
