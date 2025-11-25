use std::collections::HashMap;
use std::io::Write;
use std::mem::discriminant;
use std::sync::{Arc};

use crate::chunk::JsonsorChunk;
use crate::schema::JsonsorFieldType;



#[derive(Debug, PartialEq, Eq)]
pub enum JsonsorStreamStatus {
    SeekingObjectStart,
    SeekingArrayStart,
    SeekingFieldName,
    ParsingFieldName,
    SeekingColon,
    SeekingFieldValue,
    InferringFieldValueType,
    PassingFieldValue { dtype: JsonsorFieldType },
    ReachedObjectEnd,
}

#[derive(Clone, PartialEq, Eq)]
pub enum HeterogeneousArrayStrategy {
    WrapInObject,
    KeepAsIs, // Must collect information about the types inside of the array
}

pub trait FieldNameProcessor: Send + Sync {
    fn process_unicode(&self, field_name: &String) -> String;
    fn process_ascii(&self, field_name: &Vec<u8>) -> Vec<u8>;
}

#[derive(Clone)]
pub struct JsonsorConfig {
    pub field_name_processors: Vec<Arc<dyn FieldNameProcessor>>,
    pub heterogeneous_array_strategy: HeterogeneousArrayStrategy,
    pub exclude_null_fields: bool,
    pub input_buffer_size: usize,
    pub output_buffer_size: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum JsonsorNestedStreamSpecificsFlag {
    None,
    ArrayValueWrapper,
    ArrayForWrapInObject,
}

pub struct JsonsorStream {
    config: Arc<JsonsorConfig>,
    nested_level: usize,
    stack: Vec<JsonsorStream>,
    schema: Arc<HashMap<Vec<u8>, JsonsorFieldType>>,
    current_field_buf: Vec<u8>,
    current_field_name_buf: Vec<u8>,
    current_status: JsonsorStreamStatus,
    specifics_flag: JsonsorNestedStreamSpecificsFlag,
    previous_char: u8,
    has_previous_field: bool,
}

impl JsonsorStream {
    pub fn new(
        schema: HashMap<Vec<u8>, JsonsorFieldType>,
        config: Arc<JsonsorConfig>,
    ) -> Self {
        Self {
            config: config,
            nested_level: 0,
            stack: Vec::new(),
            schema: Arc::new(schema),
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingObjectStart,
            specifics_flag: JsonsorNestedStreamSpecificsFlag::None,
            previous_char: b'\0',
            has_previous_field: false,
        }
    }

    pub fn nest_obj(&self,
        schema: Arc<HashMap<Vec<u8>, JsonsorFieldType>>,
        inherited_specifics_flag: JsonsorNestedStreamSpecificsFlag,
    ) -> JsonsorStream {
        JsonsorStream {
            config: self.config.clone(),
            nested_level: self.nested_level + 1,
            stack: Vec::new(),
            schema,
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingObjectStart,
            specifics_flag: inherited_specifics_flag,
            previous_char: b'\0',
            has_previous_field: false,
        }
    }

    pub fn nest_arr(&self,
        schema: Arc<HashMap<Vec<u8>, JsonsorFieldType>>,
    ) -> JsonsorStream {
        JsonsorStream {
            config: self.config.clone(),
            nested_level: self.nested_level + 1,
            stack: Vec::new(),
            schema,
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingArrayStart,
            specifics_flag: match self.config.heterogeneous_array_strategy {
                HeterogeneousArrayStrategy::WrapInObject => JsonsorNestedStreamSpecificsFlag::ArrayForWrapInObject,
                HeterogeneousArrayStrategy::KeepAsIs => JsonsorNestedStreamSpecificsFlag::None,
            },
            previous_char: b'\0',
            has_previous_field: false,
        }
    }

    pub fn config(&self) -> &JsonsorConfig {
        &self.config
    }

    pub fn schema(&self) -> &Arc<HashMap<Vec<u8>, JsonsorFieldType>> {
        &self.schema
    }

    pub fn write_raw(&mut self, chunk: &[u8]) -> (Vec<u8>, bool, usize) {
        let mut jsonsor_chunk = JsonsorChunk::new(chunk);
        let mut output_buf: Vec<u8> = Vec::new();

        let (is_complete_obj, processed_bytes_num) = self.write(&mut jsonsor_chunk, &mut output_buf);
        (output_buf, is_complete_obj, processed_bytes_num)
    }

    pub fn write<W: Write>(&mut self, chunk: &JsonsorChunk, out: &mut W) -> (bool, usize) {
        // TODO: Reduce number of clones
        // TODO: Use logger????

        // TODO: think how to handle cases of invalid JSON

        // clear output buffer for the new chunk
        let (is_complete_obj, processed_bytes_num ) = self.process_with_stacked_streams(chunk, out);
        if !is_complete_obj {
            return (false, processed_bytes_num);
        }

        let (is_finally_completed, current_stream_processed_bytes_num) = self.process(&mut chunk.starting_from(processed_bytes_num), out);
        (is_finally_completed, processed_bytes_num + current_stream_processed_bytes_num)
    }

    fn process_with_stacked_streams<W: Write>(&mut self, chunk: &JsonsorChunk, out: &mut W) -> (bool, usize) {
        let mut cursor = 0;
        while let Some(mut nested_stream) = self.stack.pop() {
            let (is_obj_complete, cursor_shift) = nested_stream.write(&chunk.starting_from(cursor), out);
            println!(
                "{}[{}][{}] Nested stream completed with status {:?}/{}",
                "\t".repeat(self.nested_level),
                chunk.len(),
                cursor_shift,
                nested_stream.current_status,
                is_obj_complete
            );
            cursor += cursor_shift;
            self.update_current_field_schema(&nested_stream.schema);
            if !is_obj_complete {
                // Nested object is not complete yet, push it back to the stack
                self.stack.push(nested_stream);
                return (false, cursor);
            }
        }
        (true, cursor)
    }

    fn process<W: Write>(&mut self, chunk: &mut JsonsorChunk, out: &mut W) -> (bool, usize) {
        let obj_type = JsonsorFieldType::Object {
            schema: Arc::new(HashMap::new()),
        };
        let arr_obj_type = JsonsorFieldType::Array {
            item_type: Box::new(JsonsorFieldType::Object {
                schema: Arc::new(HashMap::new()),
            }),
        };
        let mut cursor = 0;

        while cursor < chunk.len() {
            let byte = &chunk.get_byte_by_index(cursor).expect("Cursor out of bounds");
            println!(
                "{}[{}] {:?} '{}'",
                "\t".repeat(self.nested_level),
                cursor,
                self.current_status,
                *byte as char
            );

            match &self.current_status {
                JsonsorStreamStatus::SeekingObjectStart => {
                    if *byte == b'{' {
                        self.current_status = JsonsorStreamStatus::SeekingFieldName;
                    }
                    out.write_all(&[*byte]).expect("Failed to write to output");
                }
                JsonsorStreamStatus::SeekingArrayStart => {
                    if *byte == b'[' {
                        self.current_status = JsonsorStreamStatus::SeekingFieldValue;
                    }
                    out.write_all(&[*byte]).expect("Failed to write to output");
                }
                JsonsorStreamStatus::SeekingFieldName => {
                    if *byte == b'"' {
                        self.current_status = JsonsorStreamStatus::ParsingFieldName;
                    } else if *byte == b'}' { // happens if object is empty
                        self.current_status = JsonsorStreamStatus::PassingFieldValue { dtype: JsonsorFieldType::Null };
                        continue;
                    }
                    self.current_field_buf.push(*byte);
                }
                JsonsorStreamStatus::ParsingFieldName => {
                    if *byte == b'"' {
                        self.current_status = JsonsorStreamStatus::SeekingColon;
                        if !self.current_field_name_buf.is_empty() && !self.config.field_name_processors.is_empty() {
                            if self.current_field_name_buf.is_ascii() {
                                for processor in &self.config.field_name_processors {
                                    self.current_field_name_buf = processor.process_ascii(&self.current_field_name_buf);
                                }
                            } else {
                                let mut field_name_str = String::from_utf8(
                                    self.current_field_name_buf.clone(),
                                ).expect("Invalid UTF-8 in field name");

                                for processor in &self.config.field_name_processors {
                                    field_name_str = processor.process_unicode(&field_name_str);
                                }

                                self.current_field_name_buf = field_name_str.into_bytes();
                            }
                        }
                        // TODO: Case-sensitivity problem
                        // Needs a default solution:
                        // 1. Case insensitive. Preserve the first occurance of the field name.
                        //    Keep the Case
                        // 2. Case sensitive. Rename fields with different cases.
                        self.current_field_buf.extend(&self.current_field_name_buf);
                    } else {
                        self.current_field_name_buf.push(*byte);
                    }
                }
                JsonsorStreamStatus::SeekingColon => {
                    if *byte == b':' {
                        self.current_status = JsonsorStreamStatus::SeekingFieldValue;
                    }
                }
                JsonsorStreamStatus::SeekingFieldValue => {
                    // TODO: are all space chars covered?
                    if self.is_space_byte(*byte) {
                        // skip spaces
                    } else if *byte == b']' { // happens if array is empty
                        if self.config.heterogeneous_array_strategy == HeterogeneousArrayStrategy::WrapInObject {
                            self.current_status = JsonsorStreamStatus::PassingFieldValue { dtype: JsonsorFieldType::Object { schema: Arc::new(HashMap::new()) } };
                        } else {
                            self.current_status = JsonsorStreamStatus::PassingFieldValue { dtype: JsonsorFieldType::Null };
                        }
                        continue;
                    } else {
                        self.current_status = JsonsorStreamStatus::InferringFieldValueType;

                        if self.specifics_flag == JsonsorNestedStreamSpecificsFlag::ArrayForWrapInObject {
                            // TODO: No need to value_prefix_buf. Replace by flag???
                            chunk.inject_chunk(cursor, b"{\"value\":");
                        }

                        continue; // reprocess this byte in the next state
                    }
                }
                JsonsorStreamStatus::InferringFieldValueType => {
                    let (dtype, cursor_shift, is_type_modified_during_type_inference) = match *byte {
                        b'"' => (JsonsorFieldType::String, 0, false),
                        b'n' => (JsonsorFieldType::Null, 0, false),
                        b't' | b'f' => (JsonsorFieldType::Boolean, 0, false),
                        b'0'..=b'9' | b'-' => (JsonsorFieldType::Number, 0, false),
                        b'{' => {
                            self.commit_field_name(&obj_type, out);

                            let expected_field_type = self.schema.get(&self.current_field_name_buf);

                            println!(
                                "{}[{}] Nesting object for field '{}' of type {}",
                                "\t".repeat(self.nested_level),
                                cursor,
                                String::from_utf8_lossy(&self.current_field_name_buf),
                                expected_field_type.unwrap_or(&JsonsorFieldType::Null)
                            );

                            let nested_stream_init_schema =
                                if let Some(JsonsorFieldType::Object { schema }) = expected_field_type {
                                     schema.clone()
                                } else {
                                    Arc::new(HashMap::new())
                                };

                            let mut nested_stream = self.nest_obj(
                                nested_stream_init_schema,
                                if self.specifics_flag == JsonsorNestedStreamSpecificsFlag::ArrayForWrapInObject {
                                    JsonsorNestedStreamSpecificsFlag::ArrayValueWrapper
                                } else {
                                    JsonsorNestedStreamSpecificsFlag::None
                                },
                            );


                            let (is_complete_obj, processed_bytes_num) =
                                nested_stream.write(&chunk.starting_from(cursor), out);
                            let inferred_type = JsonsorFieldType::Object {
                                schema: nested_stream.schema.clone(),
                            };

                            let is_type_modified_during_type_inference = expected_field_type != Some(&inferred_type);

                            if !is_complete_obj {
                                self.stack.push(nested_stream);
                            }

                            // TODO: decrement by 1 because the output shift is rather a number of
                            // processed bytes, not the cursor shift
                            (inferred_type, processed_bytes_num - 1, is_type_modified_during_type_inference)
                        }
                        b'[' => {
                            self.commit_field_name(&arr_obj_type, out);

                            let expected_field_type = self.schema.get(&self.current_field_name_buf);

                            println!(
                                "{}[{}] Nesting array for field '{}' of type {}",
                                "\t".repeat(self.nested_level),
                                cursor,
                                String::from_utf8_lossy(&self.current_field_name_buf),
                                expected_field_type.unwrap_or(&JsonsorFieldType::Null)
                            );

                            let nested_stream_init_schema: Arc<HashMap<Vec<u8>, JsonsorFieldType>> =
                                if let Some(JsonsorFieldType::Array { item_type }) = expected_field_type {
                                    let mut array_schema_with_item = HashMap::new();
                                    array_schema_with_item.insert(
                                        vec![],
                                        (**item_type).clone(),
                                    );
                                    Arc::new(array_schema_with_item)
                            } else {
                                Arc::new(HashMap::new())
                            };

                            let mut nested_stream = self.nest_arr(
                                nested_stream_init_schema,
                            );

                            let (is_complete_array, processed_bytes_num) =
                                nested_stream.write(&chunk.starting_from(cursor), out);

                            let inferred_item_type =
                                    nested_stream
                                        .schema
                                        .get(&vec![])
                                        .cloned()
                                        .unwrap_or(JsonsorFieldType::Null);

                            let inferred_type = JsonsorFieldType::Array {
                                item_type: Box::new(inferred_item_type)
                            };
                            let is_type_modified_during_type_inference = expected_field_type != Some(&inferred_type);

                            if !is_complete_array {
                                self.stack.push(nested_stream);
                            }
                            (inferred_type, processed_bytes_num - 1, is_type_modified_during_type_inference)
                        }
                        _ => panic!(
                            "Unexpected byte while inferring field value type: '{}'",
                            *byte as char
                        ),
                    };

                    cursor += cursor_shift;
                    let should_schema_be_modified = if dtype.is_primitive() {
                        let is_primitive_type_updated = self.commit_field_name(&dtype, out);
                        if !(self.config.exclude_null_fields && dtype == JsonsorFieldType::Null) {
                            out.write_all(&[*byte]).expect("Failed to write to output");
                        }
                        is_primitive_type_updated
                    } else {
                        is_type_modified_during_type_inference
                    };

                    // Reset buffers and status
                    self.current_field_buf.clear();
                    self.current_status = JsonsorStreamStatus::PassingFieldValue {
                        dtype: dtype.clone(),
                    };

                    if dtype != JsonsorFieldType::Null && should_schema_be_modified {
                        Arc::make_mut(&mut self.schema)
                            .insert(self.current_field_name_buf.clone(), dtype.clone());
                        println!(
                            "{}[{}] Field '{}' type {} differs from the schema. Schema is updated",
                            "\t".repeat(self.nested_level),
                            cursor,
                            String::from_utf8_lossy(&self.current_field_name_buf),
                            dtype
                        );
                    } else {
                        println!(
                            "{}[{}] Field '{}' type {} matches the schema",
                            "\t".repeat(self.nested_level),
                            cursor,
                            String::from_utf8_lossy(&self.current_field_name_buf),
                            dtype
                        );
                    }
                }
                JsonsorStreamStatus::PassingFieldValue { dtype } => {
                    let is_field_excluded = self.config.exclude_null_fields && *dtype == JsonsorFieldType::Null;

                    match dtype {
                        JsonsorFieldType::String => {
                            // Seek the closing quote, but ignore escaped quotes if there is not
                            // only the current symbol in the output buffer
                            if *byte == b'"' && self.previous_char != b'\\' {
                                // reiterate PassingFieldValue with another type to handle
                                // correctly comma or closing brace after string value
                                self.current_status = JsonsorStreamStatus::PassingFieldValue {
                                    dtype: JsonsorFieldType::Number, // dummy type to process
                                                                     // ending of the regulat
                                                                     // value
                                };
                            }
                        }
                        _ => {
                            match *byte {
                                b',' => {
                                    if !is_field_excluded {
                                        self.has_previous_field = true;
                                    }

                                    if self.specifics_flag == JsonsorNestedStreamSpecificsFlag::ArrayValueWrapper {
                                        out.write_all(b"}").expect("Failed to write to output");
                                        println!(
                                            "{}[{}] Wrapping completed with status {:?}",
                                            "\t".repeat(self.nested_level),
                                            cursor,
                                            self.current_status,
                                        );
                                        self.current_status = JsonsorStreamStatus::SeekingObjectStart;

                                        // Return here to make sure that the parent will reprocess
                                        // current byte. cursor is unsigned to subtract 1 safely
                                        return (true, cursor); // move past the closing brace
                                    }

                                    if self.current_field_name_buf.is_empty() {
                                        self.current_status =
                                            JsonsorStreamStatus::SeekingFieldValue;
                                    } else {
                                        self.current_status = JsonsorStreamStatus::SeekingFieldName;
                                        self.current_field_name_buf.clear();
                                    }
                                    cursor += 1; // move to the next byte
                                    self.previous_char = *byte;
                                    continue;
                                }
                                b'}' | b']' => {
                                    if self.specifics_flag == JsonsorNestedStreamSpecificsFlag::ArrayValueWrapper {
                                        out.write_all(b"}").expect("Failed to write to output");
                                        println!(
                                            "{}[{}] Wrapping completed with status {:?}",
                                            "\t".repeat(self.nested_level),
                                            cursor,
                                            self.current_status,
                                        );
                                        self.current_status = JsonsorStreamStatus::SeekingObjectStart;

                                        // root stream has no early exit on object end due to NDJSON structure
                                        return (true, cursor); // move past the closing brace
                                    }

                                    self.current_field_name_buf.clear();
                                    self.current_status = JsonsorStreamStatus::ReachedObjectEnd;
                                    out.write_all(&[*byte]).expect("Failed to write to output");
                                    cursor -= chunk.injected_bytes;
                                    continue; // reprocess this byte in the next state
                                }
                                _ => {}
                            }
                        }
                    }
                    if !is_field_excluded {
                        out.write_all(&[*byte]).expect("Failed to write to output");
                    }
                }
                JsonsorStreamStatus::ReachedObjectEnd => {
                    // TODO: increment by 1 to make the final cursor equal to the chunk length.
                    // Why?
                    self.has_previous_field = false;
                    self.current_status = JsonsorStreamStatus::SeekingObjectStart;

                    // root stream has no early exit on object end due to NDJSON structure
                    if self.nested_level > 0 {
                        return (true, cursor + 1); // move past the closing brace
                    }
                }
            }

            cursor += 1; // move to the next byte
            self.previous_char = *byte;
        }

        println!(
            "{}[{}] Exiting with status {:?}",
            "\t".repeat(self.nested_level),
            cursor,
            self.current_status,
        );

        // Chunk is over, but object not complete yet
        return (self.current_status == JsonsorStreamStatus::SeekingObjectStart, cursor);
    }

    // returns true if the schema is logically updated (field name was suffixed or new field)
    fn commit_field_name<W: Write>(&mut self, target_type: &JsonsorFieldType, out: &mut W) -> bool {
        if !(self.config.exclude_null_fields && *target_type == JsonsorFieldType::Null) {
            // TODO: Get via args
            let expected_field_type = self.schema.get(&self.current_field_name_buf);

            let mut should_be_suffixed = self.config.heterogeneous_array_strategy == HeterogeneousArrayStrategy::WrapInObject &&
                matches!(target_type, JsonsorFieldType::Array { item_type: _ });

            if should_be_suffixed {
                println!(
                    "{} Field '{}' should be suffixed due to array strategy",
                    "\t".repeat(self.nested_level),
                    String::from_utf8_lossy(&self.current_field_name_buf)
                );
            }

            if !should_be_suffixed {
                if let Some(expected_type) = expected_field_type {
                    if self.types_differ(target_type, expected_type) {
                        println!(
                            "{} Field '{}' type {} differs from the schema type {}",
                            "\t".repeat(self.nested_level),
                            String::from_utf8_lossy(&self.current_field_name_buf),
                            target_type,
                            expected_type
                        );
                        should_be_suffixed = true;
                    }
                }
            }

            if should_be_suffixed {

                println!(
                    "{} Suffixing field '{}' of type {}",
                    "\t".repeat(self.nested_level),
                    String::from_utf8_lossy(&self.current_field_name_buf),
                    target_type
                );

                let suffix = self.type_suffix(target_type);
                if !self.current_field_name_buf.is_empty() {
                    self.current_field_buf.extend_from_slice(suffix.as_bytes());
                    self.current_field_name_buf
                        .extend_from_slice(suffix.as_bytes());
                }
            }

            if self.has_previous_field {
                out.write_all(b",").expect("Failed to write to output");
                self.has_previous_field = false;
            }

            out.write_all(&self.current_field_buf).expect("Failed to write to output");
            if !self.current_field_name_buf.is_empty() {
                out.write_all(b"\":").expect("Failed to write to output");
            }

            return should_be_suffixed || expected_field_type.is_none();
        }

        self.current_field_buf.clear();

        return false;
    }

    fn update_current_field_schema(&mut self, updated_schema: &Arc<HashMap<Vec<u8>, JsonsorFieldType>>) {
        let updated_nested_type = match self.schema.get(&self.current_field_name_buf) {
            Some(JsonsorFieldType::Object { schema: _ }) => JsonsorFieldType::Object {
                schema: updated_schema.clone(),
            },
            Some(JsonsorFieldType::Array { item_type: _ }) => JsonsorFieldType::Array {
                item_type: Box::new(updated_schema
                               .get(&vec![])
                               .cloned()
                               .unwrap_or(JsonsorFieldType::Null)),
            },
            None => panic!("Field for the nested stream not found in the schema"),
            _ => panic!("Unsupported field type for nested object"),
        };
        Arc::make_mut(&mut self.schema).insert(
            self.current_field_name_buf.clone(),
            updated_nested_type,
        );
    }

    fn type_suffix(&self, dtype: &JsonsorFieldType) -> String {
        match dtype {
            JsonsorFieldType::Number => String::from("__num"),
            JsonsorFieldType::String => String::from("__str"),
            JsonsorFieldType::Boolean => String::from("__bool"),
            JsonsorFieldType::Null => String::from(""), // nulls are compatible with any type
            JsonsorFieldType::Object { schema: _ } => String::from("__obj"),
            JsonsorFieldType::Array { item_type } => {
                String::from("__arr") + &self.type_suffix(&*item_type)
            }
        }
    }

    fn types_differ(&self, dtype1: &JsonsorFieldType, dtype2: &JsonsorFieldType) -> bool {
        discriminant(dtype1) != discriminant(&dtype2)
            || match (dtype1, dtype2) {
                (
                    JsonsorFieldType::Array {
                        item_type: expected_item_type,
                    },
                    JsonsorFieldType::Array {
                        item_type: dtype_item_type,
                    },
                ) => self.types_differ(&*expected_item_type, &*dtype_item_type),
                _ => false,
            }
    }

    fn is_space_byte(&self, byte: u8) -> bool {
        byte == b' ' || byte == b'\n' || byte == b'\r' || byte == b'\t'
    }

}
