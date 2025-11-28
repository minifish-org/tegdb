mod vector_storage_format_tests {
    use std::collections::HashMap;
    use tegdb::parser::{ColumnConstraint, DataType, SqlValue};
    use tegdb::query_processor::{ColumnInfo, TableSchema};
    use tegdb::storage_format::StorageFormat;

    fn make_vector_schema(dim: usize) -> TableSchema {
        TableSchema {
            name: "vectors".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![ColumnConstraint::PrimaryKey],
                    storage_offset: 0,
                    storage_size: 0,
                    storage_type_code: 0,
                },
                ColumnInfo {
                    name: "embedding".to_string(),
                    data_type: DataType::Vector(Some(dim)),
                    constraints: vec![],
                    storage_offset: 0,
                    storage_size: 0,
                    storage_type_code: 0,
                },
            ],
            indexes: vec![], // Initialize indexes as empty
        }
    }

    #[test]
    fn test_vector_serialize_deserialize_round_trip() {
        let dim = 5;
        let mut schema = make_vector_schema(dim);
        tegdb::catalog::Catalog::compute_table_metadata(&mut schema).unwrap();
        let storage = StorageFormat::new();

        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(42));
        row.insert(
            "embedding".to_string(),
            SqlValue::Vector(vec![0.1, 0.2, 0.3, 0.4, 0.5]),
        );

        let bytes = storage.serialize_row(&row, &schema).unwrap();
        let deserialized = storage.deserialize_row_full(&bytes, &schema).unwrap();
        assert_eq!(deserialized["id"], SqlValue::Integer(42));
        assert_eq!(
            deserialized["embedding"],
            SqlValue::Vector(vec![0.1, 0.2, 0.3, 0.4, 0.5])
        );
    }

    #[test]
    fn test_vector_wrong_dimension_fails() {
        let dim = 4;
        let mut schema = make_vector_schema(dim);
        tegdb::catalog::Catalog::compute_table_metadata(&mut schema).unwrap();
        let storage = StorageFormat::new();

        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(1));
        // Insert a vector with the wrong dimension (should be 4, but is 3)
        row.insert(
            "embedding".to_string(),
            SqlValue::Vector(vec![1.0, 2.0, 3.0]),
        );

        let result = storage.serialize_row(&row, &schema);
        assert!(result.is_err(), "Should fail due to wrong vector dimension");
    }

    #[test]
    fn test_vector_zero_dimension() {
        let dim = 0;
        let mut schema = make_vector_schema(dim);
        tegdb::catalog::Catalog::compute_table_metadata(&mut schema).unwrap();
        let storage = StorageFormat::new();

        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(2));
        row.insert("embedding".to_string(), SqlValue::Vector(vec![]));

        let bytes = storage.serialize_row(&row, &schema).unwrap();
        let deserialized = storage.deserialize_row_full(&bytes, &schema).unwrap();
        assert_eq!(deserialized["embedding"], SqlValue::Vector(vec![]));
    }
}
