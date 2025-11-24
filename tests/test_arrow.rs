use std::io::Cursor;

use arrow::{datatypes::{Field, Schema}, ffi_stream::ArrowArrayStreamReader, ipc::reader::StreamReader};
use jsonsor::{arrow::{process_to_arrow_ffi, process_to_arrow_ipc}, stream::JsonsorConfig};

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

    let inferred_schema = process_to_arrow_ipc(Cursor::new(input), &mut output, init_schema, config).expect("Processing failed");

    assert_eq!(inferred_schema.fields().len(), 2);

    assert!(inferred_schema.field_with_name("key1").is_ok());
    assert_eq!(inferred_schema.field_with_name("key1").unwrap().data_type(), &arrow::datatypes::DataType::Utf8);

    assert!(inferred_schema.field_with_name("key2").is_ok());
    assert_eq!(inferred_schema.field_with_name("key2").unwrap().data_type(), &arrow::datatypes::DataType::Float64);

    let reader = StreamReader::try_new(Cursor::new(&output), None)
        .expect("Output is not valid Arrow IPC stream");

    for batch in reader {
        let batch = batch.expect("Failed to read record batch");
        assert_eq!(batch.num_rows(), 2);

        let key1_array = batch
            .column(batch.schema().index_of("key1").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Failed to downcast key1 column");
        let key2_array = batch
            .column(batch.schema().index_of("key2").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("Failed to downcast key2 column");
        assert_eq!(key1_array.value(0), "value1");
        assert_eq!(key1_array.value(1), "value2");
        assert_eq!(key2_array.value(0), 42.0);
        assert_eq!(key2_array.value(1), 43.0);
    }
}

#[test]
fn test_ffi_case_1() {
    let input = b"{\"key1\": \"value1\", \"key2\": 42}\n{\"key1\": \"value2\", \"key2\": 43}\n";
    let input_ptr = input.as_ptr();
    let input_len = input.len();
    let fields: Vec<Field> = vec![];
    let input_schema = Schema::new(fields);
    let input_schema_ptr = Box::into_raw(Box::new(arrow::ffi::FFI_ArrowSchema::try_from(&input_schema).unwrap()));
    let output_schema_ptr = Box::into_raw(Box::new(arrow::ffi::FFI_ArrowSchema::empty()));

    let output_stream_ptr = process_to_arrow_ffi(input_ptr, input_len, input_schema_ptr, output_schema_ptr);
    let output_schema_ffi = unsafe { &*output_schema_ptr };
    let output_schema = Schema::try_from(output_schema_ffi).expect("Failed to convert output schema from FFI");

    assert_eq!(output_schema.fields().len(), 2);
    assert!(output_schema.field_with_name("key1").is_ok());
    assert_eq!(output_schema.field_with_name("key1").unwrap().data_type(), &arrow::datatypes::DataType::Utf8);
    assert!(output_schema.field_with_name("key2").is_ok());
    assert_eq!(output_schema.field_with_name("key2").unwrap().data_type(), &arrow::datatypes::DataType::Float64);

    let output_stream = unsafe { *Box::from_raw(output_stream_ptr) };
    let reader = ArrowArrayStreamReader::try_new(output_stream)
        .expect("Failed to create ArrowArrayStreamReader");

    let mut num_rows_read = 0;
    for batch_result in reader {
        let batch = batch_result.expect("Failed to read record batch");
        assert_eq!(batch.num_rows(), 2);
        num_rows_read += batch.num_rows();

        let key1_array = batch
            .column(batch.schema().index_of("key1").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Failed to downcast key1 column");
        assert_eq!(key1_array.value(0), "value1");
        assert_eq!(key1_array.value(1), "value2");

        let key2_array = batch
            .column(batch.schema().index_of("key2").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("Failed to downcast key2 column");
        assert_eq!(key2_array.value(0), 42.0);
        assert_eq!(key2_array.value(1), 43.0);

        assert_eq!(num_rows_read, 2);
    }
}
