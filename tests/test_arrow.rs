use std::io::Cursor;

use arrow::datatypes::{Field, Schema};
use jsonsor::{arrow::process_to_arrow, stream::JsonsorConfig};

#[test]
fn test_case_1() {
    let input = b"{\"key1\": \"value1\", \"key2\": 42}\n{\"key1\": \"value2\", \"key2\": 43}\n";
    let mut output = Vec::new();
    let fields: Vec<Field> = vec![];
    let init_schema = Schema::new(fields);
    let config = JsonsorConfig {
        field_name_processors: vec![],
        heterogeneous_array_strategy: jsonsor::stream::HeterogeneousArrayStrategy::WrapInObject,
        exclude_null_fields: false,
        input_buffer_size: 8 * 1024,
        output_buffer_size: 8 * 1024,
    };

    let inferred_schema = process_to_arrow(&mut Cursor::new(input), &mut output, init_schema, config).expect("Processing failed");
    assert_eq!(inferred_schema.fields().len(), 2);

    assert!(inferred_schema.field_with_name("key1").is_ok());
    assert_eq!(inferred_schema.field_with_name("key1").unwrap().data_type(), &arrow::datatypes::DataType::Utf8);

    assert!(inferred_schema.field_with_name("key2").is_ok());
    assert_eq!(inferred_schema.field_with_name("key2").unwrap().data_type(), &arrow::datatypes::DataType::Float64);

    let expected_output = b"{\"key1\":\"value1\", \"key2\":42}\n{\"key1\":\"value2\", \"key2\":43}\n";
    assert_eq!(String::from_utf8(output), String::from_utf8(expected_output.to_vec()));
}
