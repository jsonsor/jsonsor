use std::{collections::HashMap, io::{Read, Write}, sync::Arc, thread::spawn};

use arrow::{array::{RecordBatchReader}, ipc::writer::StreamWriter, json::ReaderBuilder};
use io_pipe::pipe;

use crate::{jsonsor::Jsonsor, schema::JsonsorFieldType, stream::JsonsorConfig};

#[cfg(feature = "ffi")]
use arrow::{ffi::FFI_ArrowSchema, ffi_stream::FFI_ArrowArrayStream};


pub fn jsonsor_schema_to_arrow_schema(
    jsonsor_schema: Arc<HashMap<Vec<u8>, JsonsorFieldType>>,
) -> arrow::datatypes::Schema {

    fn map_jsonsor_type_to_arrow_type(
        jsonsor_type: &JsonsorFieldType,
    ) -> arrow::datatypes::DataType {
        match jsonsor_type {
            JsonsorFieldType::Null => arrow::datatypes::DataType::Null,
            JsonsorFieldType::Boolean => arrow::datatypes::DataType::Boolean,
            JsonsorFieldType::Number => arrow::datatypes::DataType::Float64,
            JsonsorFieldType::String => arrow::datatypes::DataType::Utf8,
            JsonsorFieldType::Array {item_type} => {
                let item_arrow_type = map_jsonsor_type_to_arrow_type(item_type);
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    item_arrow_type,
                    true,
                )))
            }
            JsonsorFieldType::Object { schema } => {
                let inner_schema = jsonsor_schema_to_arrow_schema(schema.clone());
                arrow::datatypes::DataType::Struct(inner_schema.fields().clone())
            }
            JsonsorFieldType::Mixed { .. } => {
                // For mixed types, we can choose to represent them as Utf8 (string) in Arrow
                arrow::datatypes::DataType::Utf8
            }
        }
    }

    let fields = jsonsor_schema
        .iter()
        .map(|(key, jsonsor_type)| {
            let arrow_type = map_jsonsor_type_to_arrow_type(jsonsor_type);
            let field_name = String::from_utf8_lossy(key).to_string();
            arrow::datatypes::Field::new(&field_name, arrow_type, true)
        })
        .collect::<Vec<arrow::datatypes::Field>>();

    arrow::datatypes::Schema::new(fields)
}

pub fn arrow_schema_to_jsonsor_schema(
    arrow_schema: &arrow::datatypes::Schema,
) -> HashMap<Vec<u8>, JsonsorFieldType> {

    fn map_arrow_type_to_jsonsor_type(
        arrow_type: &arrow::datatypes::DataType,
    ) -> JsonsorFieldType {
        match arrow_type {
            arrow::datatypes::DataType::Null => JsonsorFieldType::Null,
            arrow::datatypes::DataType::Boolean => JsonsorFieldType::Boolean,
            arrow::datatypes::DataType::Float64 => JsonsorFieldType::Number,
            arrow::datatypes::DataType::Utf8 => JsonsorFieldType::String,
            arrow::datatypes::DataType::List(field) => {
                let item_jsonsor_type = map_arrow_type_to_jsonsor_type(&field.data_type());
                JsonsorFieldType::Array {
                    item_type: Box::new(item_jsonsor_type),
                }
            }
            arrow::datatypes::DataType::Struct(fields) => {
                let inner_schema = arrow_schema_to_jsonsor_schema(&arrow::datatypes::Schema::new(
                    fields.clone(),
                ));
                JsonsorFieldType::Object {
                    schema: Arc::new(inner_schema),
                }
            }
            _ => JsonsorFieldType::Null, // Fallback for unsupported types
        }
    }

    let mut schema_map = HashMap::new();
    for field in arrow_schema.fields() {
        let jsonsor_type = map_arrow_type_to_jsonsor_type(&field.data_type());
        schema_map.insert(field.name().as_bytes().to_vec(), jsonsor_type);
    }

    schema_map
}

/// Processes an input NDJSON byte stream using Jsonsor into Arrow RecordBatches.
///
/// input - A reader for the input NDJSON stream.
/// init_schema - The initial Arrow schema to start with.
/// config - Configuration for Jsonsor processing.
///
/// Returns a tuple containing:
/// - A RecordBatchReader for the resulting Arrow RecordBatches.
/// - The reconciled Arrow schema after processing.
pub fn process_to_arrow<R>(
        mut input: R,
        init_schema: arrow::datatypes::Schema,
        config: JsonsorConfig,
    ) -> Result<(Box<dyn RecordBatchReader + Send + 'static>, Arc<arrow::datatypes::Schema>), std::io::Error>
    where
        R: Read + Send + 'static,
    {

    let jsonsor_init_schema = arrow_schema_to_jsonsor_schema(&init_schema);
    let (mut reconciled_ndjson_writer, reconciled_ndjson_reader) = pipe();

    let reconciliating_thread = spawn(move || {
        let mut jsonsor = Jsonsor::new(jsonsor_init_schema, config);
        jsonsor.process_stream(&mut input, &mut reconciled_ndjson_writer)
    });

    let reconciled_schema = Arc::new(
        reconciliating_thread
                .join()
                .expect("Reconciliating thread panicked")
                .map(|jsonsor_schema| {
                    jsonsor_schema_to_arrow_schema(jsonsor_schema)
                })
                .expect("Failed to convert schema")
    );

    let reader_to_arrow = ReaderBuilder::new(reconciled_schema.clone())
        .build(reconciled_ndjson_reader)
        .expect("Failed to create Arrow JSON reader");

    Ok((Box::new(reader_to_arrow), reconciled_schema))
}

/// Processes an input NDJSON byte stream using Jsonsor into Arrow IPC.
///
/// input - A reader for the input NDJSON stream.
/// output - A writer for the output Arrow IPC stream.
/// init_schema - The initial Arrow schema to start with.
/// config - Configuration for Jsonsor processing.
/// Returns the reconciled Arrow schema after processing.
pub fn process_to_arrow_ipc<R, W>(
        input: R,
        output: &mut W,
        init_schema: arrow::datatypes::Schema,
        config: JsonsorConfig,
    ) -> Result<arrow::datatypes::Schema, std::io::Error>
    where
        R: Read + Send + 'static,
        W: Write
    {
    let (reader_to_arrow, reconciled_schema) = process_to_arrow(
        input,
        init_schema,
        config,
    ).expect("Failed to process to Arrow batches");

    let mut arrow_ipc_writer = StreamWriter::try_new(output, &reconciled_schema).expect("Failed to create Arrow IPC writer");

    for batch in reader_to_arrow {
        let batch = batch.expect("Failed to read record batch from JSON");
        arrow_ipc_writer
            .write(&batch)
            .expect("Failed to write record batch to Arrow IPC");
    }

    arrow_ipc_writer.finish().expect("Failed to finish Arrow IPC writing");

    Ok(reconciled_schema.as_ref().clone())
}

#[cfg(feature = "ffi")]
#[no_mangle]
pub extern "C" fn process_to_arrow_ffi(
    input_ptr: *const u8,
    input_len: usize,
    input_schema_ptr: *mut FFI_ArrowSchema,
    output_schema_ptr: *mut FFI_ArrowSchema,
) -> *mut FFI_ArrowArrayStream {
    let input_slice = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
    let input_reader = std::io::Cursor::new(input_slice);

    let init_schema = unsafe {
        arrow::datatypes::Schema::try_from(&*input_schema_ptr).expect("Failed to convert FFI Arrow schema")
    };

    let config = JsonsorConfig {
        field_name_processors: vec![],
        heterogeneous_array_strategy: crate::stream::HeterogeneousArrayStrategy::WrapInObject,
        exclude_null_fields: true,
        input_buffer_size: 8192,
        output_buffer_size: 8192,
    };

    match process_to_arrow(
        input_reader,
        init_schema,
        config,
    ) {
        Ok((arrow_reader, reconciled_schema)) => {
            let ffi_stream = FFI_ArrowArrayStream::new(arrow_reader);
            let ffi_output_schema = FFI_ArrowSchema::try_from(&*reconciled_schema)
                .expect("Failed to convert reconciled schema to FFI Arrow schema");

            unsafe {
                std::ptr::write(output_schema_ptr, ffi_output_schema);
            }

            return Box::into_raw(Box::new(ffi_stream));
        }
        Err(e) => {
            panic!("Error processing to Arrow: {}", e);
        }
    }
}


#[cfg(feature = "ffi")]
#[no_mangle]
pub extern "C" fn free_ffi_arrow_array_stream(
    stream_ptr: *mut FFI_ArrowArrayStream,
) {
    if !stream_ptr.is_null() {
        unsafe {
            drop(Box::from_raw(stream_ptr));
        }
    }
}
