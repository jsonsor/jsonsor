use std::{collections::HashMap, io::Write, io::Read, sync::Arc};

use crate::{jsonsor::Jsonsor, stream::{JsonsorConfig, JsonsorFieldType}};


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

pub fn process_to_arrow<R: Read, W: Write>(
        input: &mut R,
        output: &mut W,
        init_schema: arrow::datatypes::Schema,
        config: JsonsorConfig,
    ) -> Result<arrow::datatypes::Schema, std::io::Error> {
    // TODO: write to the output in Arrow format
    let jsonsor_init_schema = arrow_schema_to_jsonsor_schema(&init_schema);

    // TODO: Inject into output the pipe inside of the pipe produce RecordBatches and stream them
    // into IPC stream
    // use pipe::pipe;
    // use std::io::{Read, Write, Result};
    // use std::thread;

    // fn process_stream<R: Read, W: Write>(mut reader: R, mut writer: W) -> Result<()> {
    //     // Example: copy data
    //     std::io::copy(&mut reader, &mut writer)?;
    //     Ok(())
    // }

    // fn proxy_stream<R: Read, W: Write>(mut input: R, mut output: W) -> Result<()> {
    //     let (mut pipe_reader, mut pipe_writer) = pipe();

    //     // Spawn a thread to run the original function
    //     let handle = thread::spawn(move || {
    //         process_stream(input, pipe_writer).unwrap();
    //     });

    //     // In the proxy, read from pipe_reader, process, and write to output
    //     let mut buf = [0u8; 4096];
    //     loop {
    //         let n = pipe_reader.read(&mut buf)?;
    //         if n == 0 { break; }
    //         // Process buf[..n] as needed
    //         output.write_all(&buf[..n])?;
    //     }

    //     handle.join().unwrap();
    //     Ok(())
    // }

    match Jsonsor::process_stream(input, output, jsonsor_init_schema, config) {
        Ok(jsonsor_final_schema) => {
            // Convert final Jsonsor schema to Arrow schema
            let arrow_final_schema = jsonsor_schema_to_arrow_schema(jsonsor_final_schema);
            Ok(arrow_final_schema)
        }
        Err(e) => Err(e),
    }
}


#[cfg(feature = "ffi")]
#[no_mangle]
pub extern "C" fn process_to_arrow_ffi() {}
