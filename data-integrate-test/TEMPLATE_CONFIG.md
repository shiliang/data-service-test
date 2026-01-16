# 测试模板配置说明

## 表名和资产名配置

### 功能说明

现在可以在模板中自定义表名和资产名，而不是使用自动生成的名称。

### 配置方式

在 `schema` 部分添加以下可选字段：

```yaml
schema:
  field_count: 8
  field_types: ["int", "varchar", "text", "decimal", "datetime"]
  max_field_size: 1024
  table_name: "my_custom_table"    # 可选，自定义表名
  asset_name: "my_custom_asset"    # 可选，自定义资产名
```

### 命名规则

1. **如果指定了 `table_name`**:
   - 最终表名 = `{namespace}_{table_name}`
   - 例如：`test_mysql_1m_8fields_1234567890_abc123_user_data`

2. **如果没有指定 `table_name`**:
   - 使用默认值 `"test_table"`
   - 最终表名 = `{namespace}_test_table`

3. **如果指定了 `asset_name`**:
   - 最终资产名 = `{namespace}_{asset_name}`
   - 例如：`test_mysql_1m_8fields_1234567890_abc123_user_data_asset`

4. **如果没有指定 `asset_name`**:
   - 使用模板名称
   - 最终资产名 = `{namespace}_{template_name}`

### 示例

#### 示例 1: 使用默认名称

```yaml
name: "mysql_1m_8fields"
schema:
  field_count: 8
  # 不指定 table_name 和 asset_name
```

结果：
- 表名: `test_mysql_1m_8fields_1234567890_abc123_test_table`
- 资产名: `test_mysql_1m_8fields_1234567890_abc123_mysql_1m_8fields`

#### 示例 2: 自定义名称

```yaml
name: "mysql_1m_8fields_custom"
schema:
  field_count: 8
  table_name: "user_data"
  asset_name: "user_data_asset"
```

结果：
- 表名: `test_mysql_1m_8fields_custom_1234567890_abc123_user_data`
- 资产名: `test_mysql_1m_8fields_custom_1234567890_abc123_user_data_asset`

### 注意事项

1. **命名空间前缀**: 即使指定了自定义名称，仍然会加上命名空间前缀，确保：
   - 不同测试用例之间的隔离
   - 并行测试时不会冲突
   - 唯一性保证

2. **命名规范**: 
   - 表名和资产名应该遵循数据库命名规范
   - 避免使用特殊字符
   - 建议使用小写字母、数字和下划线

3. **向后兼容**: 
   - 如果不指定 `table_name` 和 `asset_name`，行为与之前完全一致
   - 现有模板无需修改即可继续使用

### 完整模板示例

```yaml
name: "mysql_1m_8fields_custom"
description: "MySQL 1M行 8字段测试（自定义表名和资产名）"

database:
  type: "mysql"
  name: "mysql_test"

schema:
  field_count: 8
  field_types: ["int", "varchar", "text", "decimal", "datetime"]
  max_field_size: 1024
  table_name: "user_data"      # 自定义表名
  asset_name: "user_data_asset" # 自定义资产名

data:
  row_count: 1000000
  use_snapshot: true

tests:
  - type: "read"
    expected: 1000000
    tolerance: 0.1
  
  - type: "write"
    expected: 1000000
    tolerance: 0.1
```
