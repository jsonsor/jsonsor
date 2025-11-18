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
    ) -> Result<Arc<arrow::datatypes::Schema>, std::io::Error> {
    // TODO: write to the output in Arrow format
    let jsonsor_schema = arrow_schema_to_jsonsor_schema(&init_schema);
    let final_schema = Jsonsor::process_stream(input, output, jsonsor_schema, config).expect("Processing failed");
    Ok(Arc::new(jsonsor_schema_to_arrow_schema(final_schema)))
}
