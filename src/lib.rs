use std::collections::HashMap;
use std::mem::discriminant;

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum JsonsorFieldType {
    Number,
    String,
    Boolean,
    Null,
    // TODO: Make it better visualizable in debug prints
    Object {
        schema: HashMap<Vec<u8>, JsonsorFieldType>,
    },
    Array {
        item_type: Box<JsonsorFieldType>,
    },
}

#[derive(Debug)]
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

pub struct ReconciliatingStream {
    nested_level: usize,
    stack: Vec<ReconciliatingStream>,
    // TODO :is it ok to expose struct fields like this for the lib?
    pub schema: HashMap<Vec<u8>, JsonsorFieldType>,
    pub output_buf: Vec<u8>,
    current_field_buf: Vec<u8>,
    current_field_name_buf: Vec<u8>,
    current_status: JsonsorStreamStatus,
}

impl ReconciliatingStream {
    pub fn new(
        nested_level: usize,
        status: JsonsorStreamStatus,
        schema: HashMap<Vec<u8>, JsonsorFieldType>,
    ) -> Self {
        Self {
            nested_level,
            stack: Vec::new(),
            schema,
            output_buf: Vec::new(),
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: status,
        }
    }

    pub fn reconcile_object(&mut self, chunk: &[u8]) -> (bool, usize) {
        // TODO: Reduce number of clones
        // TODO: Use logger????
        // TODO: Heterogeneous arrays
        // TODO: add support of encypted data

        // TODO: return output buffer
        // TODO: think how to handle cases of invalid JSON

        // clear output buffer for the new chunk
        self.output_buf.clear();
        let mut cursor = 0;

        while let Some(mut nested_stream) = self.stack.pop() {
            let (is_obj_complete, cursor_shift) = nested_stream.reconcile_object(chunk);
            self.output_buf.extend(nested_stream.output_buf.clone());
            cursor += cursor_shift;
            if !is_obj_complete {
                // Nested object is not complete yet, push it back to the stack
                // TODO: schema needs to be updated anyway
                self.stack.push(nested_stream);
                return (false, cursor);
            } else {
                // nested streams guarantee that the schema is updated and reconciled
                let updated_nested_type = match self.schema.get(&self.current_field_name_buf) {
                    Some(JsonsorFieldType::Object { schema: _ }) => JsonsorFieldType::Object {
                        schema: nested_stream.schema.clone(),
                    },
                    Some(JsonsorFieldType::Array { item_type: _ }) => JsonsorFieldType::Array {
                        item_type: Box::new(nested_stream
                            .schema
                            .get(&vec![])
                            .cloned()
                            .unwrap_or(JsonsorFieldType::Null)),
                    },
                    None => panic!("Field not found in schema when processing nested object"),
                    _ => panic!("Unsupported field type for nested object"),
                };
                self.schema.insert(
                    self.current_field_name_buf.clone(),
                    updated_nested_type,
                );
            }
        }

        while cursor < chunk.len() {
            let byte = &chunk[cursor];
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
                    self.output_buf.push(*byte);
                }
                JsonsorStreamStatus::SeekingArrayStart => {
                    if *byte == b'[' {
                        self.current_status = JsonsorStreamStatus::SeekingFieldValue;
                    }
                    self.output_buf.push(*byte);
                }
                //TODO: support escaping, unicode, etc.
                JsonsorStreamStatus::SeekingFieldName => {
                    if *byte == b'"' {
                        self.current_status = JsonsorStreamStatus::ParsingFieldName;
                    }
                    self.current_field_buf.push(*byte);
                }
                JsonsorStreamStatus::ParsingFieldName => {
                    if *byte == b'"' {
                        self.current_status = JsonsorStreamStatus::SeekingColon;
                        // TODO: What to do?
                    } else {
                        // TODO: Replace unwanted symbols
                        // TODO: Case-sensitivity problem
                        self.current_field_buf.push(*byte);
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
                    } else {
                        self.current_status = JsonsorStreamStatus::InferringFieldValueType;
                        continue; // reprocess this byte in the next state
                    }
                }
                JsonsorStreamStatus::InferringFieldValueType => {
                    let expected_field_type = self.schema.get(&self.current_field_name_buf);
                    let (dtype, cursor_shift, content) = match *byte {
                        b'"' => (JsonsorFieldType::String, 0, vec![*byte]),
                        b'n' => (JsonsorFieldType::Null, 0, vec![*byte]),
                        b't' | b'f' => (JsonsorFieldType::Boolean, 0, vec![*byte]),
                        b'0'..=b'9' | b'-' => (JsonsorFieldType::Number, 0, vec![*byte]),
                        b'{' => {
                            // Process the nested object with the nested stream
                            // To be able to process reconciliation on partial chunks later
                            let nested_stream_init_schema =
                                if let Some(JsonsorFieldType::Object { schema }) =
                                    expected_field_type
                                {
                                    schema.clone()
                                } else {
                                    HashMap::new()
                                };

                            let mut nested_stream = ReconciliatingStream::new(
                                self.nested_level + 1,
                                JsonsorStreamStatus::SeekingObjectStart,
                                nested_stream_init_schema,
                            );
                            let (is_complete_obj, processed_bytes_num) =
                                nested_stream.reconcile_object(&chunk[cursor..]);
                            let inferred_type = JsonsorFieldType::Object {
                                schema: nested_stream.schema.clone(),
                            };
                            let content = nested_stream.output_buf.clone();

                            if !is_complete_obj {
                                self.stack.push(nested_stream);
                            }

                            // TODO: decrement by 1 because the output shift is rather a number of
                            // processed bytes, not the cursor shift
                            (inferred_type, processed_bytes_num - 1, content)
                        }
                        b'[' => {
                            let mut nested_stream = ReconciliatingStream::new(
                                self.nested_level + 1,
                                JsonsorStreamStatus::SeekingArrayStart,
                                HashMap::new(),
                            );
                            let (is_complete_array, processed_bytes_num) =
                                nested_stream.reconcile_object(&chunk[cursor..]);
                            let inferred_type = JsonsorFieldType::Array {
                                // Array item is a field without a name. Valid JSON cannot have an
                                // empty field name.
                                item_type: Box::new(
                                    nested_stream
                                        .schema
                                        .get(&vec![])
                                        .cloned()
                                        .unwrap_or(JsonsorFieldType::Null),
                                ),
                            };

                            let content = nested_stream.output_buf.clone();
                            if !is_complete_array {
                                self.stack.push(nested_stream);
                            }
                            (inferred_type, processed_bytes_num - 1, content)
                        }
                        _ => panic!(
                            "Unexpected byte while inferring field value type: '{}'",
                            *byte as char
                        ),
                    };

                    // Do column renaming if type is not expected
                    if let Some(expected_type) = expected_field_type {
                        // TODO: WHat to do if expected_type = Null?
                        if self.types_differ(&dtype, expected_type) {
                            let suffix = self.type_suffix(&dtype);
                            self.current_field_buf.extend_from_slice(suffix.as_bytes());
                            self.current_field_name_buf
                                .extend_from_slice(suffix.as_bytes());
                            // TODO: Should we exclude current_field_buf if it is null?
                        }
                    }

                    cursor += cursor_shift;

                    self.output_buf.extend(self.current_field_buf.clone());
                    // Add colon only if field name is not empty (for arrays)
                    if !self.current_field_name_buf.is_empty() {
                        self.output_buf.extend_from_slice(b"\":");
                    }
                    self.output_buf.extend(content);

                    // Reset buffers and status
                    self.current_field_buf.clear();
                    self.current_status = JsonsorStreamStatus::PassingFieldValue {
                        dtype: dtype.clone(),
                    };

                    if self.schema.contains_key(&self.current_field_name_buf)
                        && dtype == JsonsorFieldType::Null
                    {
                        println!("Field '{}' is already in schema, and new value is null. Keeping existing type.", String::from_utf8_lossy(&self.current_field_buf));
                    } else {
                        self.schema
                            .insert(self.current_field_name_buf.clone(), dtype.clone());
                    }
                }
                JsonsorStreamStatus::PassingFieldValue { dtype } => {
                    self.output_buf.push(*byte);

                    match dtype {
                        JsonsorFieldType::String => {
                            if *byte == b'"' {
                                // TODO: Not handling escaped quotes yet
                                // reiterate PassingFieldValue with another type to handle
                                // correctly comma or closing brace after string value
                                self.current_status = JsonsorStreamStatus::PassingFieldValue {
                                    dtype: JsonsorFieldType::Null,
                                };
                            }
                        }
                        _ => {
                            // TODO: For non-string types, we assume the value ends with a comma or closing brace
                            match *byte {
                                b',' => {
                                    if self.current_field_name_buf.is_empty() {
                                        self.current_status =
                                            JsonsorStreamStatus::SeekingFieldValue;
                                    } else {
                                        self.current_status = JsonsorStreamStatus::SeekingFieldName;
                                        self.current_field_name_buf.clear();
                                    }
                                }
                                b'}' | b']' => {
                                    self.current_field_name_buf.clear();
                                    self.current_status = JsonsorStreamStatus::ReachedObjectEnd;
                                    continue; // reprocess this byte in the next state
                                }
                                _ => {}
                            }
                        }
                    }
                }
                JsonsorStreamStatus::ReachedObjectEnd => {
                    // TODO: increment by 1 to make the final cursor equal to the chunk length.
                    // Why?
                    self.current_status = JsonsorStreamStatus::SeekingObjectStart;
                    return (true, cursor + 1); // move past the closing brace
                }
            }

            cursor += 1; // move to the next byte
        }

        println!("Exiting reconcile_object with cursor at {}", cursor);
        // Chunk is over, but object not complete yet
        return (false, cursor);
    }

    fn type_suffix(&self, dtype: &JsonsorFieldType) -> String {
        match dtype {
            JsonsorFieldType::Number => String::from("__num"),
            JsonsorFieldType::String => String::from("__str"),
            JsonsorFieldType::Boolean => String::from("__bool"),
            JsonsorFieldType::Null => String::from(""), // nulls are compatible with
            // any type
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
