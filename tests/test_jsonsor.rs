use std::{collections::HashMap, fs::File, io::Cursor};

use jsonsor::{jsonsor::{Jsonsor, JsonsorParallelismConfig}, stream::{JsonsorConfig, JsonsorFieldType}};

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
        input_buffer_size: 1024,
        output_buffer_size: 1024,
    };

    let schema = Jsonsor::process_stream(
        &mut Cursor::new(input.as_bytes()), 
        &mut output, 
        init_schema, 
        config
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

    assert_eq!(schema, expected_schema);
}

#[test]
fn test_process_stream_par_case1() {
    let input = File::open("tests/data/test_jsonsor_1.ndjson.gz").expect("Failed to open input file");

    let output = Vec::new();
    let init_schema = HashMap::new();
    let config = JsonsorConfig {
        field_name_processors: vec![],
        heterogeneous_array_strategy: jsonsor::stream::HeterogeneousArrayStrategy::KeepAsIs,
        input_buffer_size: 1024,
        output_buffer_size: 1024,
    };

    let _ = Jsonsor::process_stream_par(
        input, 
        output, 
        init_schema, 
        config,
        JsonsorParallelismConfig::default(),
    ).expect("Parallel stream processing failed");
}
