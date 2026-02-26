//! # Schema —— 表结构定义
//!
//! Schema 描述了一张表的结构：有哪些列、每列是什么类型。
//! 它是数据序列化/反序列化的"说明书"。
//!
//! ## 使用示例
//! ```
//! use toydb::storage::schema::{Schema, Column, DataType};
//!
//! let schema = Schema::new(vec![
//!     Column::new("id", DataType::Integer, false),
//!     Column::new("name", DataType::Text, false),
//!     Column::new("age", DataType::Integer, true),
//! ]);
//!
//! assert_eq!(schema.num_columns(), 3);
//! assert_eq!(schema.columns[0].name, "id");
//! ```

/// 数据库支持的数据类型
///
/// # 类比 Java
/// 类似 `java.sql.Types` 里的类型常量：
/// - Integer ≈ Types.INTEGER
/// - Float ≈ Types.DOUBLE
/// - Text ≈ Types.VARCHAR
/// - Boolean ≈ Types.BOOLEAN
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// 32 位有符号整数，固定 4 字节
    Integer,
    /// 64 位浮点数，固定 8 字节
    Float,
    /// 变长字符串（UTF-8），存储时带 2 字节长度前缀
    Text,
    /// 布尔值，固定 1 字节
    Boolean,
}

/// 列定义
///
/// 描述表中的一列：名字、类型、是否可以为 NULL。
///
/// # 类比 Java
/// ```java
/// // 就像 JPA 的 @Column 注解
/// @Column(name = "age", nullable = true)
/// private Integer age;
/// ```
#[derive(Debug, Clone)]
pub struct Column {
    /// 列名
    pub name: String,
    /// 列的数据类型
    pub data_type: DataType,
    /// 是否允许 NULL
    pub nullable: bool,
}

impl Column {
    /// 创建一个新的列定义
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Column {
            name: name.to_string(),
            data_type,
            nullable,
        }
    }
}

/// 表结构（Schema）
///
/// 一个有序的列定义列表，描述表中每一列的属性。
///
/// # 类比 Java
/// ```java
/// // 类似 JDBC 的 ResultSetMetaData
/// ResultSetMetaData meta = rs.getMetaData();
/// int count = meta.getColumnCount();
/// String name = meta.getColumnName(1);
/// int type = meta.getColumnType(1);
/// ```
#[derive(Debug, Clone)]
pub struct Schema {
    /// 列定义列表
    pub columns: Vec<Column>,
}

impl Schema {
    /// 创建一个新的 Schema
    pub fn new(columns: Vec<Column>) -> Self {
        Schema { columns }
    }

    /// 返回列的数量
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// 按名字查找列的索引
    ///
    /// # 返回
    /// - `Some(index)`: 找到了，返回列的下标
    /// - `None`: 没找到
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

// ============================================================
// 🧪 单元测试
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Text, false),
            Column::new("age", DataType::Integer, true),
            Column::new("active", DataType::Boolean, false),
        ])
    }

    #[test]
    fn test_schema_creation() {
        let schema = test_schema();
        assert_eq!(schema.num_columns(), 4);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, DataType::Integer);
        assert!(!schema.columns[0].nullable);
        assert!(schema.columns[2].nullable);
    }

    #[test]
    fn test_find_column() {
        let schema = test_schema();
        assert_eq!(schema.find_column("id"), Some(0));
        assert_eq!(schema.find_column("name"), Some(1));
        assert_eq!(schema.find_column("age"), Some(2));
        assert_eq!(schema.find_column("nonexistent"), None);
    }
}
