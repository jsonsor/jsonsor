use std::collections::HashMap;
use std::mem::discriminant;
use std::sync::Arc;

struct JsonsorChunk<'a> {
    data: Vec<&'a [u8]>,
    size: usize,
    injected_bytes: usize,
}

impl <'a> JsonsorChunk<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data: vec![data],
            size: data.len(),
            injected_bytes: 0,
        }
    }

    pub fn add_chunk(&mut self, chunk: &'a [u8]) {
        self.data.push(chunk);
        self.size += chunk.len();
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.size = 0;
    }

    pub fn inject_chunk(&mut self, index: usize, chunk: &'a [u8]) {
        let before = self.ending_by(index);
        let after = self.starting_from(index);

        self.clear();
        self.injected_bytes += chunk.len();
        for c in before.data {
            self.add_chunk(c);
        }

        self.add_chunk(chunk);

        for c in after.data {
            self.add_chunk(c);
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn get_byte_by_index(&self, index: usize) -> Option<u8> {
        if index >= self.size {
            return None;
        }

        let mut accumulated_size = 0;
        for chunk in &self.data {
            if index < accumulated_size + chunk.len() {
                return Some(chunk[index - accumulated_size]);
            }
            accumulated_size += chunk.len();
        }

        None
    }

    pub fn ending_by(&self, end_index: usize) -> JsonsorChunk<'a> {
        let mut new_chunk = JsonsorChunk {
            data: Vec::new(),
            size: 0,
            injected_bytes: 0,
        };

        let mut accumulated_size = 0;
        for chunk in &self.data {
            if end_index <= accumulated_size + chunk.len() {
                let slice_end = end_index - accumulated_size;
                new_chunk.add_chunk(&chunk[..slice_end]);
                break;
            } else {
                new_chunk.add_chunk(chunk);
            }
            accumulated_size += chunk.len();
        }

        new_chunk
    }

    pub fn starting_from(&self, start_index: usize) -> JsonsorChunk<'a> {
        let mut new_chunk = JsonsorChunk {
            data: Vec::new(),
            size: 0,
            injected_bytes: 0,
        };

        let mut accumulated_size = 0;
        for chunk in &self.data {
            if start_index < accumulated_size + chunk.len() {
                let slice_start = if start_index > accumulated_size {
                    start_index - accumulated_size
                } else {
                    0
                };

                new_chunk.add_chunk(&chunk[slice_start..]);
            }
            accumulated_size += chunk.len();
        }

        new_chunk
    }
}

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

#[derive(Debug, PartialEq)]
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

pub fn lowercase_field_name(field_name: &String) -> String {
    field_name.to_lowercase()
}

pub fn replace_chars_processor(unwanted_chars: &str, replacement: &str) -> Arc<dyn Fn(&String) -> String> {
    let unwanted_chars = unwanted_chars.to_string();
    let replacement = replacement.to_string();
    Arc::new(move |field_name: &String| {
        let mut processed_name = field_name.clone();
        for ch in unwanted_chars.chars() {
            processed_name = processed_name.replace(ch, &replacement);
        }
        processed_name
    })
}

#[derive(Clone)]
pub enum HeterogeneousArrayStrategy {
    WrapInObject,
    KeepAsIs, // Must collect information about the types inside of the array
}

#[derive(Clone)]
pub struct JsonsorConfig {
    pub field_name_processors: Vec<Arc<dyn Fn(&String) -> String>>,
    pub heterogeneous_array_strategy: HeterogeneousArrayStrategy,
}

pub struct JsonsorStream {
    config: Arc<JsonsorConfig>,
    nested_level: usize,
    stack: Vec<JsonsorStream>,
    // TODO :is it ok to expose struct fields like this for the lib?
    pub schema: HashMap<Vec<u8>, JsonsorFieldType>,
    pub output_buf: Vec<u8>,
    current_field_buf: Vec<u8>,
    current_field_name_buf: Vec<u8>,
    current_status: JsonsorStreamStatus,
    value_prefix_buf: Vec<u8>,
    is_field_value_wrapper: bool
}

impl JsonsorStream {
    pub fn new(
        schema: HashMap<Vec<u8>, JsonsorFieldType>,
        config: JsonsorConfig,
    ) -> Self {
        Self {
            config: Arc::new(config),
            nested_level: 0,
            stack: Vec::new(),
            schema,
            output_buf: Vec::new(),
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingObjectStart,
            value_prefix_buf: Vec::new(),
            is_field_value_wrapper: false,
        }
    }

    pub fn nest_obj(&self,
        schema: HashMap<Vec<u8>, JsonsorFieldType>,
        is_field_value_wrapper: bool,
    ) -> JsonsorStream {
        JsonsorStream {
            config: self.config.clone(), // TODO: rethink later
            nested_level: self.nested_level + 1,
            stack: Vec::new(),
            schema,
            output_buf: Vec::new(),
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingObjectStart,
            value_prefix_buf: Vec::new(),
            is_field_value_wrapper,
        }
    }

    pub fn nest_arr(&self,
        schema: HashMap<Vec<u8>, JsonsorFieldType>,
    ) -> JsonsorStream {
        JsonsorStream {
            config: self.config.clone(), // TODO: rethink later
            nested_level: self.nested_level + 1,
            stack: Vec::new(),
            schema,
            output_buf: Vec::new(),
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingArrayStart,
            value_prefix_buf: match self.config.heterogeneous_array_strategy {
                HeterogeneousArrayStrategy::WrapInObject => "{\"value\":".as_bytes().to_vec(),
                HeterogeneousArrayStrategy::KeepAsIs => Vec::new(),
            },
            is_field_value_wrapper: false,
        }
    }

    pub fn reconcile_object(&mut self, chunk: &[u8]) -> (bool, usize) {
        let jsonsor_chunk = JsonsorChunk::new(chunk);
        self.write_all(&jsonsor_chunk)
    }

    fn write_all(&mut self, chunk: &JsonsorChunk) -> (bool, usize) {
        // TODO: Reduce number of clones
        // TODO: Use logger????
        // TODO: add support of encypted data

        // TODO: return output buffer
        // TODO: think how to handle cases of invalid JSON

        // clear output buffer for the new chunk
        self.output_buf.clear();

        let (is_complete_obj, processed_bytes_num ) = self.process_with_stacked_streams(chunk);
        if !is_complete_obj {
            return (false, processed_bytes_num);
        }

        let (is_finally_completed, current_stream_processed_bytes_num) = self.process_with_current_stream(&mut chunk.starting_from(processed_bytes_num));
        (is_finally_completed, processed_bytes_num + current_stream_processed_bytes_num)
    }

    fn process_with_stacked_streams(&mut self, chunk: &JsonsorChunk) -> (bool, usize) {
        let mut cursor = 0;
        while let Some(mut nested_stream) = self.stack.pop() {
            let (is_obj_complete, cursor_shift) = nested_stream.write_all(&chunk.starting_from(cursor));
            self.output_buf.extend(&nested_stream.output_buf);
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

    fn process_with_current_stream(&mut self, chunk: &mut JsonsorChunk) -> (bool, usize) {
        let mut cursor = 0;

        while cursor < chunk.len() {
            let byte = &chunk.get_byte_by_index(cursor).expect("Cursor out of bounds");
            println!(
                "{}[{}][{}] {:?} '{}'",
                "\t".repeat(self.nested_level),
                chunk.len(),
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
                JsonsorStreamStatus::SeekingFieldName => {
                    if *byte == b'"' {
                        self.current_status = JsonsorStreamStatus::ParsingFieldName;
                    }
                    self.current_field_buf.push(*byte);
                }
                JsonsorStreamStatus::ParsingFieldName => {
                    if *byte == b'"' {
                        self.current_status = JsonsorStreamStatus::SeekingColon;
                        if !self.current_field_name_buf.is_empty() && !self.config.field_name_processors.is_empty() {
                            let field_name_str = String::from_utf8(
                                self.current_field_name_buf.clone(),
                            ).expect("Invalid UTF-8 in field name");

                            for processor in &self.config.field_name_processors {
                                self.current_field_name_buf = processor(&field_name_str).into_bytes();
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
                    } else {
                        self.current_status = JsonsorStreamStatus::InferringFieldValueType;

                        if !self.value_prefix_buf.is_empty() {
                            chunk.inject_chunk(cursor, "{\"value\":".as_bytes());
                        }

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
                            let nested_stream_init_schema =
                                if let Some(JsonsorFieldType::Object { schema }) = expected_field_type {
                                     schema.clone()
                                } else {
                                    HashMap::new()
                                };

                            let mut nested_stream = self.nest_obj(
                                nested_stream_init_schema,
                                !self.value_prefix_buf.is_empty(),
                            );
                            let (is_complete_obj, processed_bytes_num) =
                                nested_stream.write_all(&chunk.starting_from(cursor));
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
                            let mut nested_stream = self.nest_arr(
                                HashMap::new(),
                            );
                            let (is_complete_array, processed_bytes_num) =
                                nested_stream.write_all(&chunk.starting_from(cursor));
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
                            if !self.current_field_name_buf.is_empty() {
                                self.current_field_buf.extend_from_slice(suffix.as_bytes());
                                self.current_field_name_buf
                                .extend_from_slice(suffix.as_bytes());
                            }
                            // TODO: Should we exclude current_field_buf if it is null?
                        }
                    }

                    cursor += cursor_shift;

                    self.output_buf.extend(&self.current_field_buf);
                    if !self.current_field_name_buf.is_empty() {
                        self.output_buf.extend_from_slice(b"\":");
                    }
                    self.output_buf.extend(content);

                    // Reset buffers and status
                    self.current_field_buf.clear();
                    self.current_status = JsonsorStreamStatus::PassingFieldValue {
                        dtype: dtype.clone(),
                    };

                    if self.schema.contains_key(&self.current_field_name_buf) && dtype == JsonsorFieldType::Null {
                        println!("Field '{}' is already in schema, and new value is null. Keeping existing type.", String::from_utf8_lossy(&self.current_field_buf));
                    } else {
                        self.schema
                            .insert(self.current_field_name_buf.clone(), dtype.clone());
                    }
                }
                JsonsorStreamStatus::PassingFieldValue { dtype } => {
                    match dtype {
                        JsonsorFieldType::String => {
                            // Seek the closing quote, but ignore escaped quotes if there is not
                            // only the current symbol in the output buffer
                            if *byte == b'"' && (self.output_buf.is_empty() || self.output_buf[self.output_buf.len() - 1] != b'\\') {
                                // reiterate PassingFieldValue with another type to handle
                                // correctly comma or closing brace after string value
                                self.current_status = JsonsorStreamStatus::PassingFieldValue {
                                    dtype: JsonsorFieldType::Null,
                                };
                            }
                        }
                        _ => {
                            match *byte {
                                b',' => {
                                    if self.is_field_value_wrapper {
                                        self.output_buf.extend(b"}");
                                        println!(
                                            "{}[{}] Wrapping completed with status {:?}",
                                            "\t".repeat(self.nested_level),
                                            cursor,
                                            self.current_status,
                                        );
                                        cursor -= 1; // can be negative if the comma is the first
                                                     // byte after the object
                                                     // TODO: investigate what to do with unsigned
                                                     // type
                                        self.current_status = JsonsorStreamStatus::ReachedObjectEnd;
                                        continue;
                                    }

                                    if self.current_field_name_buf.is_empty() {
                                        self.current_status =
                                            JsonsorStreamStatus::SeekingFieldValue;
                                    } else {
                                        self.current_status = JsonsorStreamStatus::SeekingFieldName;
                                        self.current_field_name_buf.clear();
                                    }
                                }
                                b'}' | b']' => {
                                    if self.is_field_value_wrapper {
                                        self.output_buf.extend(b"}");
                                        println!(
                                            "{}[{}] Wrapping completed with status {:?}",
                                            "\t".repeat(self.nested_level),
                                            cursor,
                                            self.current_status,
                                        );
                                        self.current_status = JsonsorStreamStatus::ReachedObjectEnd;
                                        cursor -= 1; // make sure that the closing brace is
                                                     // reprocessed in the parent stream
                                                     // it can be negative if the closing brace is
                                                     // the first
                                                     // TODO: investigate what to do with unsigned
                                                     // type
                                        continue;
                                    }

                                    self.current_field_name_buf.clear();
                                    self.current_status = JsonsorStreamStatus::ReachedObjectEnd;
                                    self.output_buf.push(*byte);
                                    cursor -= chunk.injected_bytes;
                                    continue; // reprocess this byte in the next state
                                }
                                _ => {}
                            }
                        }
                    }
                    self.output_buf.push(*byte);
                }
                JsonsorStreamStatus::ReachedObjectEnd => {
                    // TODO: increment by 1 to make the final cursor equal to the chunk length.
                    // Why?
                    self.current_status = JsonsorStreamStatus::SeekingObjectStart;

                    // root stream has no early exit on object end due to NDJSON structure
                    if self.nested_level > 0 {
                        return (true, cursor + 1); // move past the closing brace
                    }
                }
            }

            cursor += 1; // move to the next byte
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

    fn update_current_field_schema(&mut self, updated_schema: &HashMap<Vec<u8>, JsonsorFieldType>) {
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
        self.schema.insert(
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
