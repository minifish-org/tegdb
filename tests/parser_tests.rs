use tegdb::parser::{parse_sql, Statement, SqlValue, DataType, ColumnConstraint, ComparisonOperator, ArithmeticOperator, Expression, Condition, OrderDirection};

#[test]
fn test_parse_create_table_basic() {
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50))";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::CreateTable(create_table)) = result {
        assert_eq!(create_table.table, "users");
        assert_eq!(create_table.columns.len(), 2);
        
        assert_eq!(create_table.columns[0].name, "id");
        assert_eq!(create_table.columns[0].data_type, DataType::Integer);
        assert!(create_table.columns[0].constraints.contains(&ColumnConstraint::PrimaryKey));
        
        assert_eq!(create_table.columns[1].name, "name");
        assert_eq!(create_table.columns[1].data_type, DataType::Text(Some(50)));
    } else {
        panic!("Expected CreateTable statement");
    }
}

#[test]
fn test_parse_create_table_multiline() {
    let sql = "CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT(50),
        age INTEGER
    )";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::CreateTable(create_table)) = result {
        assert_eq!(create_table.table, "users");
        assert_eq!(create_table.columns.len(), 3);
    } else {
        panic!("Expected CreateTable statement");
    }
}

#[test]
fn test_parse_create_table_with_constraints() {
    let sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT(100) NOT NULL, price REAL UNIQUE)";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::CreateTable(create_table)) = result {
        assert_eq!(create_table.columns.len(), 3);
        
        // Check constraints
        assert!(create_table.columns[0].constraints.contains(&ColumnConstraint::PrimaryKey));
        assert!(create_table.columns[1].constraints.contains(&ColumnConstraint::NotNull));
        assert!(create_table.columns[2].constraints.contains(&ColumnConstraint::Unique));
    } else {
        panic!("Expected CreateTable statement");
    }
}

#[test]
fn test_parse_create_table_vector_type() {
    let sql = "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vector VECTOR(384))";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::CreateTable(create_table)) = result {
        assert_eq!(create_table.columns[1].data_type, DataType::Vector(Some(384)));
    } else {
        panic!("Expected CreateTable statement");
    }
}

#[test]
fn test_parse_insert_basic() {
    let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.table, "users");
        assert_eq!(insert.columns, vec!["id", "name"]);
        assert_eq!(insert.values.len(), 1);
        assert_eq!(insert.values[0], vec![SqlValue::Integer(1), SqlValue::Text("Alice".to_string())]);
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_insert_multiline() {
    let sql = "INSERT INTO users (
        id,
        name,
        age
    ) VALUES (
        1,
        'Alice',
        25
    )";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.table, "users");
        assert_eq!(insert.columns, vec!["id", "name", "age"]);
        assert_eq!(insert.values[0], vec![
            SqlValue::Integer(1),
            SqlValue::Text("Alice".to_string()),
            SqlValue::Integer(25)
        ]);
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_insert_without_columns() {
    let sql = "INSERT INTO users VALUES (1, 'Alice', 25)";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.table, "users");
        assert!(insert.columns.is_empty());
        assert_eq!(insert.values[0], vec![
            SqlValue::Integer(1),
            SqlValue::Text("Alice".to_string()),
            SqlValue::Integer(25)
        ]);
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_insert_multiple_values() {
    let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.values.len(), 2);
        assert_eq!(insert.values[0], vec![SqlValue::Integer(1), SqlValue::Text("Alice".to_string())]);
        assert_eq!(insert.values[1], vec![SqlValue::Integer(2), SqlValue::Text("Bob".to_string())]);
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_insert_with_escaped_strings() {
    let sql = "INSERT INTO users (id, name) VALUES (1, 'Charlie\\'s Name')";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.values[0][1], SqlValue::Text("Charlie's Name".to_string()));
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_insert_with_null() {
    let sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', NULL)";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.values[0][2], SqlValue::Null);
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_select_basic() {
    let sql = "SELECT * FROM users";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert_eq!(select.table, "users");
        assert_eq!(select.columns.len(), 1);
        assert!(matches!(&select.columns[0], Expression::Column(name) if name == "*"));
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_select_specific_columns() {
    let sql = "SELECT id, name FROM users";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert_eq!(select.columns.len(), 2);
        assert!(matches!(&select.columns[0], Expression::Column(name) if name == "id"));
        assert!(matches!(&select.columns[1], Expression::Column(name) if name == "name"));
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_select_with_where() {
    let sql = "SELECT * FROM users WHERE id = 1";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert!(select.where_clause.is_some());
        if let Some(where_clause) = select.where_clause {
            assert!(matches!(where_clause.condition, Condition::Comparison { operator: ComparisonOperator::Equal, .. }));
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_select_with_order_by() {
    let sql = "SELECT * FROM users ORDER BY name ASC";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert!(select.order_by.is_some());
        if let Some(order_by) = select.order_by {
            assert_eq!(order_by.items.len(), 1);
            assert_eq!(order_by.items[0].direction, OrderDirection::Asc);
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_select_with_limit() {
    let sql = "SELECT * FROM users LIMIT 10";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert_eq!(select.limit, Some(10));
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_select_complex() {
    let sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name DESC LIMIT 5";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert_eq!(select.columns.len(), 2);
        assert!(select.where_clause.is_some());
        assert!(select.order_by.is_some());
        assert_eq!(select.limit, Some(5));
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_update_basic() {
    let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Update(update)) = result {
        assert_eq!(update.table, "users");
        assert_eq!(update.assignments.len(), 1);
        assert_eq!(update.assignments[0].column, "name");
        assert!(update.where_clause.is_some());
    } else {
        panic!("Expected Update statement");
    }
}

#[test]
fn test_parse_delete_basic() {
    let sql = "DELETE FROM users WHERE id = 1";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Delete(delete)) = result {
        assert_eq!(delete.table, "users");
        assert!(delete.where_clause.is_some());
    } else {
        panic!("Expected Delete statement");
    }
}

#[test]
fn test_parse_arithmetic_expressions() {
    let sql = "SELECT id + 1, price * 1.1 FROM products";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert_eq!(select.columns.len(), 2);
        // Check that we have arithmetic expressions
        assert!(matches!(select.columns[0], Expression::BinaryOp { operator: ArithmeticOperator::Add, .. }));
        assert!(matches!(select.columns[1], Expression::BinaryOp { operator: ArithmeticOperator::Multiply, .. }));
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_aggregate_functions() {
    let sql = "SELECT COUNT(*), SUM(price), AVG(price) FROM products";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert_eq!(select.columns.len(), 3);
        assert!(matches!(&select.columns[0], Expression::AggregateFunction { name, .. } if name == "COUNT"));
        assert!(matches!(&select.columns[1], Expression::AggregateFunction { name, .. } if name == "SUM"));
        assert!(matches!(&select.columns[2], Expression::AggregateFunction { name, .. } if name == "AVG"));
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_where_conditions() {
    let sql = "SELECT * FROM users WHERE id = 1 AND name LIKE 'A%' OR age BETWEEN 18 AND 65";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        assert!(select.where_clause.is_some());
        // The condition should be an OR with AND on the left side
        if let Some(where_clause) = select.where_clause {
            assert!(matches!(where_clause.condition, Condition::Or(..)));
        }
    } else {
        panic!("Expected Select statement");
    }
}

#[test]
fn test_parse_create_index() {
    let sql = "CREATE INDEX idx_name ON users (name)";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::CreateIndex(create_index)) = result {
        assert_eq!(create_index.index_name, "idx_name");
        assert_eq!(create_index.table_name, "users");
        assert_eq!(create_index.column_name, "name");
        assert!(!create_index.unique);
    } else {
        panic!("Expected CreateIndex statement");
    }
}

#[test]
fn test_parse_create_unique_index() {
    let sql = "CREATE INDEX idx_email ON users (email) UNIQUE";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::CreateIndex(create_index)) = result {
        assert!(create_index.unique);
    } else {
        panic!("Expected CreateIndex statement");
    }
}

#[test]
fn test_parse_drop_table() {
    let sql = "DROP TABLE users";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::DropTable(drop_table)) = result {
        assert_eq!(drop_table.table, "users");
        assert!(!drop_table.if_exists);
    } else {
        panic!("Expected DropTable statement");
    }
}

#[test]
fn test_parse_drop_table_if_exists() {
    let sql = "DROP TABLE IF EXISTS users";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::DropTable(drop_table)) = result {
        assert!(drop_table.if_exists);
    } else {
        panic!("Expected DropTable statement");
    }
}

#[test]
fn test_parse_transaction_commands() {
    let begin_sql = "BEGIN TRANSACTION";
    let commit_sql = "COMMIT";
    let rollback_sql = "ROLLBACK";
    
    assert!(matches!(parse_sql(begin_sql), Ok(Statement::Begin)));
    assert!(matches!(parse_sql(commit_sql), Ok(Statement::Commit)));
    assert!(matches!(parse_sql(rollback_sql), Ok(Statement::Rollback)));
}

#[test]
fn test_parse_with_semicolon() {
    let sql = "SELECT * FROM users;";
    let result = parse_sql(sql);
    assert!(result.is_ok());
}

#[test]
fn test_parse_error_handling() {
    let invalid_sql = "INVALID SQL STATEMENT";
    let result = parse_sql(invalid_sql);
    assert!(result.is_err());
}

#[test]
fn test_parse_string_escaping() {
    let sql = "INSERT INTO test (id, text) VALUES (1, 'Line 1\\nLine 2\\tTabbed')";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.values[0][1], SqlValue::Text("Line 1\nLine 2\tTabbed".to_string()));
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_vector_literals() {
    let sql = "INSERT INTO embeddings (id, vector) VALUES (1, [1.0, 2.0, 3.0])";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Insert(insert)) = result {
        assert_eq!(insert.values[0][1], SqlValue::Vector(vec![1.0, 2.0, 3.0]));
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_parameter_placeholders() {
    let sql = "SELECT * FROM users WHERE id = ?1 AND name = ?2";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    
    if let Ok(Statement::Select(select)) = result {
        if let Some(where_clause) = select.where_clause {
            if let Condition::Comparison { right, .. } = where_clause.condition {
                assert!(matches!(right, SqlValue::Parameter(0))); // ?1 becomes index 0
            }
        }
    } else {
        panic!("Expected Select statement");
    }
}
