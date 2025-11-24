use std::{collections::HashMap, fs::File, sync::{Arc, Mutex}};

use jsonsor::{jsonsor_par::{JsonsorPar, JsonsorParallelismConfig}, stream::{JsonsorConfig, JsonsorFieldType}};


#[test]
fn test_process_stream_par_case1() {
    let input = File::open("tests/data/test_jsonsor_1.ndjson.gz").expect("Failed to open input file");

    let output = Arc::new(Mutex::new(Vec::new()));
    let init_schema = HashMap::new();
    let config = JsonsorConfig {
        field_name_processors: vec![],
        heterogeneous_array_strategy: jsonsor::stream::HeterogeneousArrayStrategy::KeepAsIs,
        exclude_null_fields: false,
        input_buffer_size: 1024,
        output_buffer_size: 1024,
    };

    let mut jsonsor = JsonsorPar::new(init_schema, config, JsonsorParallelismConfig::default());
    let schema = jsonsor.process_stream(
        input, 
        output.clone(), 
    ).expect("Parallel stream processing failed");

    
    let mut expected_schema = HashMap::new();
    expected_schema.insert("id".as_bytes().to_vec(), JsonsorFieldType::Number);
    expected_schema.insert("id__str".as_bytes().to_vec(), JsonsorFieldType::String);
    expected_schema.insert("value".as_bytes().to_vec(), JsonsorFieldType::Number);
    expected_schema.insert("value__str".as_bytes().to_vec(), JsonsorFieldType::String);

    let schema_locked = schema.lock().unwrap();
    assert_eq!(*schema_locked, expected_schema);

    let expected_output = "
    {\"id\":1,\"value\":10.5}
    {\"id__str\":\"2\",\"value__str\":\"20.0\"}
    {\"id\":3,\"value\":30.25}
    ";

    let output_locked = output.lock().unwrap();
    let output_bytes = std::str::from_utf8(&output_locked).expect("Output is not valid UTF-8");
    assert_eq!(output_bytes, expected_output);
}
