use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
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

    pub fn inject_chunk(&mut self, index: usize, chunk: &'a [u8]) {
        let before = self.ending_by(index);
        let after = self.starting_from(index);

        self.data = before.data.into_iter().chain(std::iter::once(chunk)).chain(after.data.into_iter()).collect();
        self.size = self.data.iter().map(|c| c.len()).sum();
        self.injected_bytes += chunk.len();
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
    pub input_buffer_size: usize,
    pub output_buffer_size: usize,
}

pub struct Jsonsor;
impl Jsonsor {
    pub fn process_stream<R: Read, W: Write>(
        input: &mut R,
        output: &mut W,
        init_schemaa: HashMap<Vec<u8>, JsonsorFieldType>,
        config: JsonsorConfig,
    ) -> Result<HashMap<Vec<u8>, JsonsorFieldType>, std::io::Error> {
        let mut buf_input = BufReader::new(input);
        let mut input_reader: Box<dyn Read> = if Self::is_gzipped(&mut buf_input) {
            Box::new(flate2::read::GzDecoder::new(buf_input))
        } else {
            Box::new(buf_input)
        };

        let input_buffer_size = config.input_buffer_size.clone();
        let mut jsonsor_stream = JsonsorStream::new(init_schemaa, config);
        let mut buffer = vec![0; input_buffer_size];

        loop {
            let bytes_read = input_reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break; // EOF
            }

            let chunk = JsonsorChunk::new(&buffer[..bytes_read]);
            jsonsor_stream.write_all(&chunk, output);
        }

        Ok(jsonsor_stream.schema)
    }

    pub fn process_file(
        input_path: &str,
        output_path: &str,
        init_schemaa: HashMap<Vec<u8>, JsonsorFieldType>,
        config: JsonsorConfig,
    ) -> Result<HashMap<Vec<u8>, JsonsorFieldType>, std::io::Error> {
        let mut input_file = std::fs::File::open(input_path)?;
        let mut output_file = std::fs::File::create(output_path)?;

        Self::process_stream(&mut input_file, &mut output_file, init_schemaa, config)
    }

    fn is_gzipped<R: Read>(reader: &mut BufReader<R>) -> bool {
        match reader.fill_buf() {
            Ok(buf) => buf.len() >= 2 && buf[0] == 0x1f && buf[1] == 0x8b,
            Err(_) => false,
        }
    }
}

pub struct JsonsorStream {
    config: Arc<JsonsorConfig>,
    nested_level: usize,
    stack: Vec<JsonsorStream>,
    // TODO :is it ok to expose struct fields like this for the lib?
    pub schema: HashMap<Vec<u8>, JsonsorFieldType>,
    current_field_buf: Vec<u8>,
    current_field_name_buf: Vec<u8>,
    current_status: JsonsorStreamStatus,
    value_prefix_buf: Vec<u8>,
    is_field_value_wrapper: bool,
    previous_char: u8,
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
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingObjectStart,
            value_prefix_buf: Vec::new(),
            is_field_value_wrapper: false,
            previous_char: b'\0',
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
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingObjectStart,
            value_prefix_buf: Vec::new(),
            is_field_value_wrapper,
            previous_char: b'\0',
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
            current_field_buf: Vec::new(),
            current_field_name_buf: Vec::new(),
            current_status: JsonsorStreamStatus::SeekingArrayStart,
            value_prefix_buf: match self.config.heterogeneous_array_strategy {
                HeterogeneousArrayStrategy::WrapInObject => "{\"value\":".as_bytes().to_vec(),
                HeterogeneousArrayStrategy::KeepAsIs => Vec::new(),
            },
            is_field_value_wrapper: false,
            previous_char: b'\0',
        }
    }

    pub fn reconcile_object(&mut self, chunk: &[u8]) -> (Vec<u8>, bool, usize) {
        let mut jsonsor_chunk = JsonsorChunk::new(chunk);
        let mut output_buf: Vec<u8> = Vec::new();

        let (is_complete_obj, processed_bytes_num) = self.write_all(&mut jsonsor_chunk, &mut output_buf);
        (output_buf, is_complete_obj, processed_bytes_num)
    }

    fn write_all<W: Write>(&mut self, chunk: &JsonsorChunk, out: &mut W) -> (bool, usize) {
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
            let (is_obj_complete, cursor_shift) = nested_stream.write_all(&chunk.starting_from(cursor), out);
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
            schema: HashMap::new(),
        };
        let arr_obj_type = JsonsorFieldType::Array {
            item_type: Box::new(JsonsorFieldType::Object {
                schema: HashMap::new(),
            }),
        };
        let mut output_buf: Vec<u8> = Vec::new();
        let mut cursor = 0;

        while cursor < chunk.len() {
            let byte = &chunk.get_byte_by_index(cursor).expect("Cursor out of bounds");
            println!(
                "{}[{}][{}] {:?} '{}'",
                "\t".repeat(self.nested_level),
                if self.is_field_value_wrapper { "wrapped" } else { "normal" },
                cursor,
                self.current_status,
                *byte as char
            );

            match &self.current_status {
                JsonsorStreamStatus::SeekingObjectStart => {
                    if *byte == b'{' {
                        self.current_status = JsonsorStreamStatus::SeekingFieldName;
                    }
                    output_buf.push(*byte);
                }
                JsonsorStreamStatus::SeekingArrayStart => {
                    if *byte == b'[' {
                        self.current_status = JsonsorStreamStatus::SeekingFieldValue;
                    }
                    output_buf.push(*byte);
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
                            // TODO: No need to value_prefix_buf. Replace by flag???
                            chunk.inject_chunk(cursor, b"{\"value\":");
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

                            self.handle_type_conflict(&obj_type, &mut output_buf);
                            self.flush_output_buf(&mut output_buf, out);

                            let (is_complete_obj, processed_bytes_num) =
                                nested_stream.write_all(&chunk.starting_from(cursor), out);
                            let inferred_type = JsonsorFieldType::Object {
                                schema: nested_stream.schema.clone(),
                            };

                            if !is_complete_obj {
                                self.stack.push(nested_stream);
                            }

                            // TODO: decrement by 1 because the output shift is rather a number of
                            // processed bytes, not the cursor shift
                            (inferred_type, processed_bytes_num - 1, vec![])
                        }
                        b'[' => {
                            let mut nested_stream = self.nest_arr(
                                HashMap::new(), // TODO: init array schema
                            );

                            self.handle_type_conflict(&arr_obj_type, &mut output_buf);
                            self.flush_output_buf(&mut output_buf, out);

                            let (is_complete_array, processed_bytes_num) =
                                nested_stream.write_all(&chunk.starting_from(cursor), out);
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

                            if !is_complete_array {
                                self.stack.push(nested_stream);
                            }
                            (inferred_type, processed_bytes_num - 1, vec![])
                        }
                        _ => panic!(
                            "Unexpected byte while inferring field value type: '{}'",
                            *byte as char
                        ),
                    };

                    cursor += cursor_shift;
                    if !content.is_empty() {
                        self.handle_type_conflict(&dtype, &mut output_buf);
                        output_buf.extend(content);
                    }

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
                            if *byte == b'"' && self.previous_char != b'\\' {
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
                                        output_buf.push(b'}');
                                        println!(
                                            "{}[{}] Wrapping completed with status {:?}",
                                            "\t".repeat(self.nested_level),
                                            cursor,
                                            self.current_status,
                                        );
                                        self.current_status = JsonsorStreamStatus::SeekingObjectStart;
                                        self.flush_output_buf(&mut output_buf, out);

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
                                }
                                b'}' | b']' => {
                                    if self.is_field_value_wrapper {
                                        output_buf.push(b'}');
                                        println!(
                                            "{}[{}] Wrapping completed with status {:?}",
                                            "\t".repeat(self.nested_level),
                                            cursor,
                                            self.current_status,
                                        );
                                        self.current_status = JsonsorStreamStatus::SeekingObjectStart;
                                        self.flush_output_buf(&mut output_buf, out);

                                        // root stream has no early exit on object end due to NDJSON structure
                                        return (true, cursor); // move past the closing brace
                                    }

                                    self.current_field_name_buf.clear();
                                    self.current_status = JsonsorStreamStatus::ReachedObjectEnd;
                                    output_buf.push(*byte);
                                    cursor -= chunk.injected_bytes;
                                    continue; // reprocess this byte in the next state
                                }
                                _ => {}
                            }
                        }
                    }
                    // self.output_buf.push(*byte);
                    output_buf.push(*byte);
                }
                JsonsorStreamStatus::ReachedObjectEnd => {
                    // TODO: increment by 1 to make the final cursor equal to the chunk length.
                    // Why?
                    self.current_status = JsonsorStreamStatus::SeekingObjectStart;

                    // root stream has no early exit on object end due to NDJSON structure
                    if self.nested_level > 0 {
                        self.flush_output_buf(&mut output_buf, out);
                        return (true, cursor + 1); // move past the closing brace
                    }
                }
            }

            cursor += 1; // move to the next byte
            self.previous_char = *byte;

            if output_buf.len() >= self.config.output_buffer_size {
                self.flush_output_buf(&mut output_buf, out);
            }
        }

        println!(
            "{}[{}] Exiting with status {:?}",
            "\t".repeat(self.nested_level),
            cursor,
            self.current_status,
        );

        if !output_buf.is_empty() {
            self.flush_output_buf(&mut output_buf, out);
        }

        // Chunk is over, but object not complete yet
        return (self.current_status == JsonsorStreamStatus::SeekingObjectStart, cursor);
    }

    fn flush_output_buf<W: Write>(&self, output_buf: &mut Vec<u8>, out: &mut W) {
        out.write_all(output_buf).expect("Failed to write to output");
        output_buf.clear();
    }

    fn handle_type_conflict<W: Write>(&mut self, target_type: &JsonsorFieldType, out: &mut W) {
        // TODO: Get via args
        let expected_field_type = self.schema.get(&self.current_field_name_buf);
        if let Some(expected_type) = expected_field_type {
            if self.types_differ(target_type, expected_type) {
                // TODO: Exclude null if needed
                let suffix = self.type_suffix(target_type);
                if !self.current_field_name_buf.is_empty() {
                    self.current_field_buf.extend_from_slice(suffix.as_bytes());
                    self.current_field_name_buf
                    .extend_from_slice(suffix.as_bytes());
                }
            }
        }

        out.write_all(&self.current_field_buf).expect("Failed to write to output");
        if !self.current_field_name_buf.is_empty() {
            out.write_all(b"\":").expect("Failed to write to output");
        }

        self.current_field_buf.clear();
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
