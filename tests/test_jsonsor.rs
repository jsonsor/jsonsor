use std::{collections::HashMap, io::Cursor, sync::Arc};

use jsonsor::{jsonsor::Jsonsor, schema::JsonsorFieldType, stream::JsonsorConfig};

#[test]
fn test_process_stream_case1() {
    let input = r#"
    {"name": "Alice", "age": 30}
    {"name": "Bob", "age": "25"}
    {"name": "Charlie", "age": 35}
    "#;

    let mut output = Vec::new();
    let init_schema = HashMap::new();
    let config = JsonsorConfig {
        field_name_processors: vec![],
        heterogeneous_array_strategy: jsonsor::stream::HeterogeneousArrayStrategy::KeepAsIs,
        exclude_null_fields: false,
        input_buffer_size: 1024,
        output_buffer_size: 1024,
    };

    let schema = Jsonsor::new(init_schema, config).process_stream(
        &mut Cursor::new(input.as_bytes()), 
        &mut output, 
    ).expect("Stream processing failed");

    let output_str = String::from_utf8(output).expect("Output is not valid UTF-8");
    let expected_output = r#"
    {"name":"Alice", "age":30}
    {"name":"Bob", "age__str":"25"}
    {"name":"Charlie", "age":35}
    "#;
    assert_eq!(output_str, expected_output);

    let mut expected_schema = HashMap::new();
    expected_schema.insert("name".as_bytes().to_vec(), JsonsorFieldType::String);
    expected_schema.insert("age".as_bytes().to_vec(), JsonsorFieldType::Number);
    expected_schema.insert("age__str".as_bytes().to_vec(), JsonsorFieldType::String);

    assert_eq!(schema, Arc::new(expected_schema));
}
