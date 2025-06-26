//! Test showing the query planner working with actual SQL parsing and plan generation
//! 
//! Run with: cargo test --test planner_integration_test --features dev

#[cfg(test)]
mod tests {
    use tegdb::{
        Database, Result,
        parser::{parse_sql, DataType, ColumnConstraint},
        executor::{TableSchema, ColumnInfo},
        planner::{QueryPlanner, ExecutionPlan},
    };
    use std::collections::HashMap;

    fn create_test_schema() -> HashMap<String, TableSchema> {
        let mut schemas = HashMap::new();
        
        schemas.insert("users".to_string(), TableSchema {
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![ColumnConstraint::PrimaryKey],
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![ColumnConstraint::NotNull],
                },
                ColumnInfo {
                    name: "email".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![ColumnConstraint::Unique],
                },
            ],
        });
        
        schemas
    }

    #[test]
    fn test_planner_with_pk_query() -> Result<()> {
        let schemas = create_test_schema();
        let planner = QueryPlanner::new(schemas);
        
        // Parse a query that should use primary key optimization
        let sql = "SELECT name, email FROM users WHERE id = 42";
        let (_, statement) = parse_sql(sql)
            .map_err(|e| tegdb::Error::Other(format!("Parse error: {:?}", e)))?;
        
        // Generate execution plan
        let plan = planner.plan(statement)?;
        
        // Verify it's a primary key lookup plan
        match plan {
            ExecutionPlan::PrimaryKeyLookup { table, pk_values, selected_columns, .. } => {
                assert_eq!(table, "users");
                assert_eq!(pk_values.len(), 1);
                assert!(pk_values.contains_key("id"));
                assert_eq!(selected_columns, vec!["name", "email"]);
                println!("✓ Primary key optimization detected correctly");
            }
            _ => panic!("Expected PrimaryKeyLookup plan"),
        }
        
        Ok(())
    }

    #[test]
    fn test_planner_with_scan_query() -> Result<()> {
        let schemas = create_test_schema();
        let planner = QueryPlanner::new(schemas);
        
        // Parse a query that should use table scan
        let sql = "SELECT * FROM users WHERE name = 'John' LIMIT 10";
        let (_, statement) = parse_sql(sql)
            .map_err(|e| tegdb::Error::Other(format!("Parse error: {:?}", e)))?;
        
        // Generate execution plan
        let plan = planner.plan(statement)?;
        
        // Verify it's a table scan plan with optimizations
        match plan {
            ExecutionPlan::TableScan { table, selected_columns, filter, limit, early_termination } => {
                assert_eq!(table, "users");
                // Column order should be sorted: email, id, name
                assert_eq!(selected_columns, vec!["email", "id", "name"]); 
                assert!(filter.is_some());
                assert_eq!(limit, Some(10));
                assert!(early_termination); // Should enable early termination for simple filters
                println!("✓ Table scan with optimizations detected correctly");
            }
            _ => panic!("Expected TableScan plan, got: {}", plan.describe()),
        }
        
        Ok(())
    }

    #[test]
    fn test_planner_with_insert() -> Result<()> {
        let schemas = create_test_schema();
        let planner = QueryPlanner::new(schemas);
        
        // Parse an INSERT statement
        let sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";
        let (_, statement) = parse_sql(sql)
            .map_err(|e| tegdb::Error::Other(format!("Parse error: {:?}", e)))?;
        
        // Generate execution plan
        let plan = planner.plan(statement)?;
        
        // Verify it's an insert plan
        match plan {
            ExecutionPlan::Insert { table, rows, .. } => {
                assert_eq!(table, "users");
                assert_eq!(rows.len(), 1);
                assert!(rows[0].contains_key("id"));
                assert!(rows[0].contains_key("name"));
                assert!(rows[0].contains_key("email"));
                println!("✓ Insert plan generated correctly");
            }
            _ => panic!("Expected Insert plan"),
        }
        
        Ok(())
    }

    #[test]
    fn test_plan_descriptions() -> Result<()> {
        let schemas = create_test_schema();
        let planner = QueryPlanner::new(schemas);
        
        let test_cases = vec![
            ("SELECT * FROM users WHERE id = 1", "Primary Key Lookup"),
            ("SELECT * FROM users WHERE name = 'John'", "Table Scan"),
            ("INSERT INTO users (id, name) VALUES (1, 'Test')", "Insert into users"),
            ("UPDATE users SET name = 'Updated' WHERE id = 1", "Update users"),
            ("DELETE FROM users WHERE name = 'Test'", "Delete from users"),
        ];
        
        for (sql, expected_description_part) in test_cases {
            let (_, statement) = parse_sql(sql)
                .map_err(|e| tegdb::Error::Other(format!("Parse error: {:?}", e)))?;
            
            let plan = planner.plan(statement)?;
            let description = plan.describe();
            
            assert!(description.contains(expected_description_part), 
                "Plan description '{}' should contain '{}'", description, expected_description_part);
            
            println!("✓ {}: {}", sql, description);
        }
        
        Ok(())
    }

    #[test]
    fn test_end_to_end_with_database() -> Result<()> {
        // Test with actual database to ensure integration works
        let mut db = Database::open("test_planner.db")?;
        
        // Setup
        db.execute("DROP TABLE IF EXISTS test_users")?;
        db.execute("CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT)")?;
        db.execute("INSERT INTO test_users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")?;
        
        // Test queries
        let result = db.query("SELECT * FROM test_users WHERE id = 1")?;
        assert_eq!(result.rows().len(), 1);
        
        let result = db.query("SELECT * FROM test_users LIMIT 1")?;
        assert_eq!(result.rows().len(), 1);
        
        println!("✓ End-to-end database integration works");
        
        Ok(())
    }
}
