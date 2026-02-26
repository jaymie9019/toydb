//! # Tuple —— 行数据与序列化
//!
//! Tuple 表示数据库中的一行数据，由一组 Value 组成。
//! 它需要能被序列化为 `Vec<u8>` 存入 SlottedPage，
//! 也需要能从 `&[u8]` 反序列化回来。
//!
//! ## 序列化格式
//! ```text
//! ┌──────────┬──────────────────────────────────────────┐
//! │ NULL     │         Column Values                    │
//! │ Bitmap   │ col_0 bytes | col_1 bytes | ...          │
//! │ (N bits) │                                          │
//! └──────────┴──────────────────────────────────────────┘
//! ```
//!
//! ## 使用示例
//! ```
//! use toydb::storage::tuple::{Tuple, Value};
//! use toydb::storage::schema::{Schema, Column, DataType};
//!
//! let schema = Schema::new(vec![
//!     Column::new("id", DataType::Integer, false),
//!     Column::new("name", DataType::Text, false),
//! ]);
//!
//! let tuple = Tuple::new(vec![
//!     Value::Integer(1),
//!     Value::Text("Alice".to_string()),
//! ]);
//!
//! // 序列化
//! let bytes = tuple.serialize();
//!
//! // 反序列化
//! let recovered = Tuple::deserialize(&bytes, &schema).unwrap();
//! assert_eq!(recovered.values[0], Value::Integer(1));
//! ```

use crate::storage::schema::{DataType, Schema};

/// 一个字段的值
///
/// # 类比 Java
/// 类似于 `Object`，但类型安全：
/// ```java
/// Object val = rs.getObject(1); // 可能是 Integer, String, Double...
/// ```
///
/// 在 Rust 中用 enum 替代了 Java 的类型擦除，编译期就能检查类型。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// 32 位有符号整数
    Integer(i32),
    /// 64 位浮点数
    Float(f64),
    /// 变长字符串
    Text(String),
    /// 布尔值
    Boolean(bool),
    /// 空值
    Null,
}

/// 一行数据
///
/// `Tuple` 是一组 `Value` 的有序列表，对应 Schema 中的列定义。
///
/// # 类比 Java
/// ```java
/// // 就像 JDBC ResultSet 的一行
/// int id = rs.getInt("id");
/// String name = rs.getString("name");
/// ```
pub struct Tuple {
    /// 各列的值，顺序对应 Schema 中的列定义
    pub values: Vec<Value>,
}

impl Tuple {
    /// 创建一个新的 Tuple
    pub fn new(values: Vec<Value>) -> Self {
        Tuple { values }
    }

    /// 将 Tuple 序列化为字节数组。
    ///
    /// # 格式
    /// ```text
    /// [NULL bitmap] [col_0 bytes] [col_1 bytes] ...
    /// ```
    ///
    /// ## NULL Bitmap
    /// - 占 `ceil(num_columns / 8)` 个字节
    /// - 第 i 位为 1 表示第 i 列是 NULL
    /// - 第 i 位为 0 表示第 i 列有值
    ///
    /// ## 各类型编码
    /// - Integer: 4 字节，`i32::to_le_bytes()` 小端序
    /// - Float: 8 字节，`f64::to_le_bytes()` 小端序
    /// - Boolean: 1 字节，0x00=false 0x01=true
    /// - Text: 2 字节长度前缀(u16) + UTF-8 字节
    /// - Null: 不写入任何数据（由 bitmap 标记）
    ///
    /// # TODO 1: 实现这个函数（最核心的）
    ///
    /// ```rust,ignore
    /// // 大致步骤：
    /// // 1. 计算 bitmap 大小 = ceil(values.len() / 8)
    /// //    提示：(values.len() + 7) / 8
    /// // 2. 创建 bitmap 数组，初始全 0
    /// // 3. 遍历 values，如果是 Null，在 bitmap 对应位置 1
    /// //    bit 操作：bitmap[i / 8] |= 1 << (i % 8)
    /// // 4. 把 bitmap 写入 buf
    /// // 5. 遍历 values，对非 Null 值按类型编码并追加到 buf
    /// //    - Integer: buf.extend_from_slice(&i.to_le_bytes())
    /// //    - Float: buf.extend_from_slice(&f.to_le_bytes())
    /// //    - Boolean: buf.push(if *b { 1 } else { 0 })
    /// //    - Text: 先写 len as u16，再写 bytes
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        // --- Step 1: 计算并写入 NULL bitmap ---
        // TODO: 计算 bitmap_size = (self.values.len() + 7) / 8
        // TODO: 创建 bitmap: vec![0u8; bitmap_size]
        // TODO: 遍历 values，标记 Null 的位
        // TODO: buf.extend_from_slice(&bitmap)

        // 一句话：整数除法会丢掉余数，+ (除数-1) 就是把"有余数"的情况往上进一位。 想象成需要几个盒子，每个盒子可以装 8 个鸡蛋
        let bitmap_size = (self.values.len() + 7) / 8;
        let mut bitmap = vec![0u8; bitmap_size];
        for (i, value) in self.values.iter().enumerate() {
            if let Value::Null = value {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        buf.extend_from_slice(&bitmap);

        // --- Step 2: 编码每个非 Null 值 ---
        // TODO: 遍历 self.values，对每个值：
        //   match value {
        //       Value::Integer(i) => ...
        //       Value::Float(f) => ...
        //       Value::Boolean(b) => ...
        //       Value::Text(s) => ...
        //       Value::Null => {} // 跳过，不写入数据
        //   }

        for value in &self.values {
            match value {
                Value::Integer(i) => {
                    buf.extend_from_slice(&i.to_le_bytes());
                }
                Value::Float(f) => {
                    buf.extend_from_slice(&f.to_le_bytes());
                }
                Value::Boolean(b) => {
                    buf.push(if *b { 1 } else { 0 });
                }
                Value::Text(s) => {
                    let len = s.len() as u16;
                    buf.extend_from_slice(&len.to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                Value::Null => {}
            }
        }

        buf
    }

    /// 从字节切片反序列化为 Tuple。
    ///
    /// 需要 Schema 来知道每列的类型，才能正确解析字节。
    ///
    /// # 参数
    /// - `data`: 序列化后的字节
    /// - `schema`: 表结构（告诉我们每列是什么类型）
    ///
    /// # 返回
    /// - `Some(Tuple)`: 成功反序列化
    /// - `None`: 数据格式错误
    ///
    /// # TODO 2: 实现这个函数
    ///
    /// ```rust,ignore
    /// // 大致步骤：
    /// // 1. 读取 NULL bitmap (前 bitmap_size 个字节)
    /// // 2. 维护一个 offset 指针，从 bitmap 之后开始
    /// // 3. 遍历 schema.columns，对每列：
    /// //    a. 检查 bitmap 对应位：是 Null 就 push Value::Null
    /// //    b. 否则按类型从 data[offset..] 读取值：
    /// //       - Integer: 读 4 字节 → i32::from_le_bytes(...)
    /// //       - Float: 读 8 字节 → f64::from_le_bytes(...)
    /// //       - Boolean: 读 1 字节 → data[offset] != 0
    /// //       - Text: 先读 2 字节 u16 长度，再读对应长度的 UTF-8 字符串
    /// //    c. offset += 消耗的字节数
    /// ```
    pub fn deserialize(data: &[u8], schema: &Schema) -> Option<Self> {
        let mut values = Vec::new();

        // --- Step 1: 读取 NULL bitmap ---
        // TODO: 计算 bitmap_size
        // TODO: 读取 bitmap 字节
        let bitmap_size = (schema.columns.len() + 7) / 8;
        if data.len() < bitmap_size {
            return None; // 数据不足以容纳 bitmap
        }
        let bitmap = &data[..bitmap_size];

        // --- Step 2: 按 Schema 逐列解码 ---
        // TODO: 维护 offset 指针
        // TODO: 遍历 schema.columns，按类型解码每列
        let mut offset = bitmap_size;
        for (i, column) in schema.columns.iter().enumerate() {
            if bitmap[i / 8] & (1 << (i % 8)) != 0 {
                values.push(Value::Null);
            } else {
                match column.data_type {
                    DataType::Integer => {
                        if offset + 4 > data.len() {
                            return None;
                        }
                        let bytes = &data[offset..offset + 4];
                        offset += 4;
                        values.push(Value::Integer(i32::from_le_bytes(
                            bytes.try_into().unwrap(),
                        )));
                    }
                    DataType::Float => {
                        if offset + 8 > data.len() {
                            return None;
                        }
                        let bytes = &data[offset..offset + 8];
                        offset += 8;
                        values.push(Value::Float(f64::from_le_bytes(bytes.try_into().unwrap())));
                    }
                    DataType::Text => {
                        if offset + 2 > data.len() {
                            return None;
                        }
                        let len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                        offset += 2;
                        if offset + len as usize > data.len() {
                            return None;
                        }
                        let bytes = &data[offset..offset + len as usize];
                        offset += len as usize;
                        // 严格校验 UTF-8，损坏的数据应该报错而不是静默替换
                        let text = String::from_utf8(bytes.to_vec()).ok()?;
                        values.push(Value::Text(text));
                    }
                    DataType::Boolean => {
                        if offset + 1 > data.len() {
                            return None;
                        }
                        let byte = data[offset];
                        offset += 1;
                        values.push(Value::Boolean(byte != 0));
                    }
                }
            }
        }

        Some(Tuple { values })
    }

    /// 格式化打印一行数据（便于调试）。
    ///
    /// 输出格式: `(1, 'Alice', 25)`
    ///
    /// # TODO 3: 实现这个函数
    ///
    /// ```rust,ignore
    /// // 遍历 values，对每个值格式化：
    /// // Integer(i) → i.to_string()
    /// // Float(f) → f.to_string()
    /// // Text(s) → format!("'{}'", s)
    /// // Boolean(b) → b.to_string()
    /// // Null → "NULL".to_string()
    /// // 用逗号连接，外面包一层括号
    /// ```
    pub fn display(&self) -> String {
        // TODO: 格式化输出
        // 提示：可以用 Vec<String> 收集每个值的字符串表示，
        //       然后用 join(", ") 连接
        let mut result = String::new();
        result.push_str("(");
        for (i, value) in self.values.iter().enumerate() {
            if i > 0 {
                result.push_str(", ");
            }
            match value {
                Value::Integer(i) => result.push_str(&i.to_string()),
                Value::Float(f) => result.push_str(&f.to_string()),
                Value::Text(s) => result.push_str(&format!("'{}'", s)),
                Value::Boolean(b) => result.push_str(&b.to_string()),
                Value::Null => result.push_str("NULL"),
            }
        }
        result.push_str(")");
        result
    }
}

// ============================================================
// 🧪 单元测试
// ============================================================
// 运行测试: cargo test storage::tuple
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::schema::{Column, DataType, Schema};

    /// 测试用的 Schema: (id: Integer, name: Text, age: Integer)
    fn test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Text, false),
            Column::new("age", DataType::Integer, true),
        ])
    }

    // ---- 测试 1: 基本序列化和反序列化 ----
    #[test]
    fn test_serialize_deserialize() {
        let schema = test_schema();
        let tuple = Tuple::new(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Integer(25),
        ]);

        let bytes = tuple.serialize();
        let recovered = Tuple::deserialize(&bytes, &schema).unwrap();

        assert_eq!(recovered.values.len(), 3);
        assert_eq!(recovered.values[0], Value::Integer(1));
        assert_eq!(recovered.values[1], Value::Text("Alice".to_string()));
        assert_eq!(recovered.values[2], Value::Integer(25));
    }

    // ---- 测试 2: 带 NULL 的序列化 ----
    #[test]
    fn test_serialize_with_null() {
        let schema = test_schema();
        let tuple = Tuple::new(vec![
            Value::Integer(42),
            Value::Text("Bob".to_string()),
            Value::Null, // age 为 NULL
        ]);

        let bytes = tuple.serialize();
        let recovered = Tuple::deserialize(&bytes, &schema).unwrap();

        assert_eq!(recovered.values[0], Value::Integer(42));
        assert_eq!(recovered.values[1], Value::Text("Bob".to_string()));
        assert_eq!(recovered.values[2], Value::Null);
    }

    // ---- 测试 3: 所有数据类型 ----
    #[test]
    fn test_all_types() {
        let schema = Schema::new(vec![
            Column::new("a", DataType::Integer, false),
            Column::new("b", DataType::Float, false),
            Column::new("c", DataType::Text, false),
            Column::new("d", DataType::Boolean, false),
        ]);

        let tuple = Tuple::new(vec![
            Value::Integer(-100),
            Value::Float(3.14),
            Value::Text("Hello 世界".to_string()),
            Value::Boolean(true),
        ]);

        let bytes = tuple.serialize();
        let recovered = Tuple::deserialize(&bytes, &schema).unwrap();

        assert_eq!(recovered.values[0], Value::Integer(-100));
        assert_eq!(recovered.values[1], Value::Float(3.14));
        assert_eq!(recovered.values[2], Value::Text("Hello 世界".to_string()));
        assert_eq!(recovered.values[3], Value::Boolean(true));
    }

    // ---- 测试 4: 空字符串 ----
    #[test]
    fn test_empty_text() {
        let schema = Schema::new(vec![Column::new("x", DataType::Text, false)]);

        let tuple = Tuple::new(vec![Value::Text(String::new())]);

        let bytes = tuple.serialize();
        let recovered = Tuple::deserialize(&bytes, &schema).unwrap();
        assert_eq!(recovered.values[0], Value::Text(String::new()));
    }

    // ---- 测试 5: display 格式化 ----
    #[test]
    fn test_display() {
        let tuple = Tuple::new(vec![
            Value::Integer(1),
            Value::Text("Alice".to_string()),
            Value::Null,
        ]);

        let output = tuple.display();
        assert_eq!(output, "(1, 'Alice', NULL)");
    }

    // ---- 测试 6: 序列化后的字节大小 ----
    #[test]
    fn test_serialized_size() {
        let tuple = Tuple::new(vec![
            Value::Integer(1),             // 4 bytes
            Value::Text("Hi".to_string()), // 2 (len) + 2 (data) = 4 bytes
            Value::Integer(25),            // 4 bytes
        ]);

        let bytes = tuple.serialize();
        // bitmap: ceil(3/8) = 1 byte
        // total: 1 + 4 + 4 + 4 = 13 bytes
        assert_eq!(bytes.len(), 13);
    }

    // ---- 测试 7: Boolean 值 ----
    #[test]
    fn test_boolean_values() {
        let schema = Schema::new(vec![
            Column::new("a", DataType::Boolean, false),
            Column::new("b", DataType::Boolean, false),
        ]);

        let tuple = Tuple::new(vec![Value::Boolean(true), Value::Boolean(false)]);

        let bytes = tuple.serialize();
        let recovered = Tuple::deserialize(&bytes, &schema).unwrap();
        assert_eq!(recovered.values[0], Value::Boolean(true));
        assert_eq!(recovered.values[1], Value::Boolean(false));
    }
}
