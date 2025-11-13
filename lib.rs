use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, Write};
use std::mem::discriminant;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;
use arrow_json::reader::infer_json_schema;
use flate2::read::GzDecoder;

use parquet::arrow::ArrowWriter;
use arrow_json::ReaderBuilder;

pub struct JsonsorChunk<'a> {
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

        self.data = before.data.into_iter()
            .chain(std::iter::once(chunk))
            .chain(after.data.into_iter())
            .collect();
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
    KeepAsIs, // TODO: Must collect information about the types inside of the array
}

#[derive(Clone)]
pub struct JsonsorConfig {
    pub field_name_processors: Vec<Arc<dyn Fn(&String) -> String>>,
    pub heterogeneous_array_strategy: HeterogeneousArrayStrategy,
}

pub struct Jsonsor;
impl Jsonsor {
    pub fn reconcile_stream<R: Read, W: Write>(
        input_stream: &mut R,
        output_stream: &mut W,
        init_schema: HashMap<Vec<u8>, JsonsorFieldType>,
        config: JsonsorConfig,
    ) -> Result<HashMap<Vec<u8>, JsonsorFieldType>, std::io::Error> {
        let mut jsonsor_stream = JsonsorStream::new(init_schema, config);
        let mut raw_reader = BufReader::new(input_stream);
        let mut target_reader: Box<dyn Read> = if Self::is_gzipped(&mut raw_reader) {
            Box::new(GzDecoder::new(raw_reader))
        } else {
            Box::new(raw_reader)
        };

        let mut buffer = [0u8; 4096];
        loop {
            let n = target_reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            // TODO: Count some useful stats like num of objects
            jsonsor_stream.write(&mut JsonsorChunk::new(&buffer[..n]), output_stream);
        }
        Ok(jsonsor_stream.schema)
    }

    pub fn reconcile_file(
        input_path: &str,
        output_path: &str,
        init_schema: HashMap<Vec<u8>, JsonsorFieldType>,
        in_parallel: bool,
        config: JsonsorConfig,
    ) -> Result<HashMap<Vec<u8>, JsonsorFieldType>, std::io::Error> {
        let input_file = std::fs::File::open(input_path)?;
        let mut output_file = std::fs::File::create(output_path)?;

        fn process_line(worker_jsonsor_stream: &mut JsonsorStream, line: Vec<u8>) -> Vec<u8> {
            let mut out = Vec::new();
            let (_, _) = worker_jsonsor_stream.write(&mut JsonsorChunk::new(&line), &mut out);
            out
        }

        if output_path.ends_with(".parquet") {
            let reconciled_data: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

            let started_at = std::time::Instant::now();

            if in_parallel {
                println!("Starting gzipped file processing with threadpool...");
                Self::process_file_in_parallel(
                    input_file,
                    &reconciled_data,
                    process_line,
                    8,
                    100,
                    1000,
                    100/*MB of decompressed data*/ * 1024 * 1024,
                ).expect("Failed to process gzipped lines");
            } else {
                println!("Starting gzipped file processing as a stream...");

                Self::reconcile_stream(
                    &mut BufReader::new(GzDecoder::new(input_file)),
                    &mut *reconciled_data.lock().unwrap(),
                    init_schema.clone(),
                    config,
                ).expect("Failed to reconcile JSON stream");
            }

            let ndjson = reconciled_data.lock().unwrap();

            let duration = started_at.elapsed();

            println!("Reconciliation of the file {} completed in {:.2?}", input_path, duration);

            println!("Converting reconciled NDJSON ({} bytes) to Parquet...", ndjson.len());

            Self::ndjson_to_parquet(&mut Cursor::new(&*ndjson), output_file).expect("Failed to convert NDJSON to Parquet");
        } else {
            if in_parallel {
                println!("Starting gzipped file processing with threadpool...");
                Self::process_file_in_parallel(
                    input_file,
                    &Arc::new(Mutex::new(output_file)),
                    process_line,
                    10,
                    100,
                    6,
                    100/*MB of decompressed data*/ * 1024 * 1024,
                ).expect("Failed to process gzipped lines");
            } else {
                println!("Starting gzipped file processing as a stream...");

                Self::reconcile_stream(
                    &mut BufReader::new(GzDecoder::new(input_file)),
                    &mut output_file,
                    init_schema.clone(),
                    config,
                ).expect("Failed to reconcile JSON stream");
            }

            println!("Finished processing file {}", input_path);
        }
        Ok(init_schema)
    }

    fn process_file_in_parallel<F, W>(
        input: File,
        output: &Arc<Mutex<W>>,
        process_fn: F,
        num_workers: usize,
        worker_capacity: usize,
        lines_in_chunk: usize,
        flush_limit: usize,
    ) -> std::io::Result<()>
    where
        F: Fn(&mut JsonsorStream, Vec<u8>) -> Vec<u8> + Send + Sync + 'static + Copy,
        W: Write + Send + 'static,
    {
        let decoder = GzDecoder::new(input);
        let mut reader = BufReader::new(decoder);

        // Channel for distributing work to the dispatcher
        let (tx, rx) = mpsc::sync_channel::<(usize, Vec<u8>)>(num_workers);
        println!("[MAIN] Created a dispatcher channel with capacity {}", num_workers);

        // Per-worker channels for actual work
        let mut worker_senders = Vec::new();
        let mut worker_handles = Vec::new();

        // Channel for collecting results
        let (result_tx, result_rx) = mpsc::channel::<(usize, Vec<u8>)>();
        println!("[MAIN] Created a results channel");

        let latest_committed = Arc::new(AtomicUsize::new(0));
        for i in 0..num_workers {
            println!("[MAIN] Spawning worker thread {}", i);
            // the worker opens its own channel
            let (worker_tx, worker_rx) = mpsc::sync_channel::<(usize, Vec<u8>)>(worker_capacity);
            println!("[MAIN] Created a worker channel");

            // clone sender to results channel for the worker
            let result_tx = result_tx.clone();

            // keep worker's sender for dispatcher
            worker_senders.push(worker_tx); 
            println!("[MAIN] Saved worker sender to worker's channel in the main thread");

            let latest_committed_cloned = latest_committed.clone();
            // spawn the worker thread and save its handle
            worker_handles.push(thread::spawn(move || {
                println!("[WORKER {}] Started processing data", i);
                let mut jsonsor_stream = JsonsorStream::new(HashMap::new(), JsonsorConfig {
                    field_name_processors: vec![],
                    heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
                });

                while let Ok((idx, chunk)) = worker_rx.recv() {
                    println!("[WORKER {}] Received chunk {}", i, idx);
                    let result = process_fn(&mut jsonsor_stream, chunk);
                    let is_new_field_found = false; // TODO: should be returned by the process_fn

                    // TODO: Legit to update schema only if the current chunk is the next chunk to commit
                    if is_new_field_found {
                        println!("[WORKER {}] New field found in chunk {}", i, idx);
                        if latest_committed_cloned.load(std::sync::atomic::Ordering::SeqCst) == idx - 1 {
                            println!("[WORKER {}] Adding a new field into schema after processing chunk {}", i, idx);
                        } else {
                            // wait until previous chunks are committed
                            println!("[WORKER {}] Waiting to commit new field from chunk {}", i, idx);
                        }
                    }
                    println!("[WORKER {}] Processed chunk {}", i, idx);
                    result_tx.send((idx, result)).unwrap();
                    println!("[WORKER {}] Sent result for chunk {}", i, idx);
                }
                println!("[WORKER {}] Worker finished", i);
            }));
            println!("[MAIN] Spawned worker thread and saved its handle in the main thread");
        }

        // sender to results channel is used only in workers. no need to keep it in main thread
        drop(result_tx);

        // Dispatcher thread: receives from rx and distributes to workers round-robin
        println!("[MAIN] Starting the dispatcher thread");
        let dispatcher = {
            let worker_senders = worker_senders.clone();
            thread::spawn(move || {
                let mut next_worker = 0;
                println!("[DISPATCHER] Dispatcher started distributing work to {} workers", worker_senders.len());
                while let Ok((idx, chunk)) = rx.recv() {
                    println!("[DISPATCHER] Dispatching  chunk {} to worker {}", idx, next_worker);
                    worker_senders[next_worker].send((idx, chunk)).unwrap();
                    next_worker = (next_worker + 1) % worker_senders.len();
                }
                println!("[DISPATCHER] Dispatcher finished");
            })
        };
        println!("[MAIN] Spawned dispatcher thread");

        println!("[MAIN] Starting the result collector thread");
        let output = output.clone();
        let collector = thread::spawn(move || {
            println!("[COLLECTOR] Listening for results");
            let mut next_idx = 0;
            let mut buffer = BTreeMap::new();
            let mut output_buffer = Vec::with_capacity(flush_limit);
            let mut out = output.lock().unwrap();

            while let Ok((idx, result)) = result_rx.recv() {
                println!("[COLLECTOR] Received result for chunk {}", idx);
                buffer.insert(idx, result);
                while buffer.contains_key(&next_idx) {
                    let result = buffer.remove(&next_idx).unwrap();
                    output_buffer.extend_from_slice(&result);
                    next_idx += 1;
                    if output_buffer.len() >= flush_limit {
                        println!("[COLLECTOR] Flushing interim output buffer with {} bytes", output_buffer.len());
                        out.write_all(&output_buffer).unwrap();
                        output_buffer.clear();
                    }
                }
            }

            if !output_buffer.is_empty() {
                println!("[COLLECTOR] Flushing final output buffer with {} bytes", output_buffer
                    .len());
                out.write_all(&output_buffer).unwrap();
            }
            out.flush().unwrap();
            println!("[COLLECTOR] Collector finished");
        });
        println!("[MAIN] Collector thread spawned");

        println!("[MAIN] Processing the input...");
        // Main thread: read lines as bytes and send to dispatcher
        let mut idx = 0;
        // HINT: create a buffer for the line outside of the loop to avoid reallocations
        let mut line_buf = Vec::new();
        let mut chunk = Vec::new();
        loop {
            let mut lines_count = 0;
            let mut bytes_read = 0;
            // HINT: line_buf.clear() keeps the allocated capacity for the next line
            chunk.clear();

            // HINT: not read_line to not convert bytes to String
            loop {
                bytes_read += reader.read_until(b'\n', &mut line_buf)?;
                chunk.extend_from_slice(&line_buf);
                line_buf.clear();

                lines_count += 1;
                if lines_count >= lines_in_chunk || bytes_read == 0 {
                    break;
                }
            }

            if bytes_read == 0 {
                println!("[MAIN] End of input reached");
                break;
            }

            println!("[MAIN] Got a chunk {} with {} line / {} bytes", idx, lines_count, bytes_read);
            tx.send((idx, chunk.clone())).unwrap();
            println!("[MAIN] Send a chunk {} to dispatcher channel", idx);
            idx += 1;
        }
        println!("[MAIN] All data sent to dispatcher");

        drop(tx); // Close dispatcher channel

        println!("[MAIN] Waiting for dispatcher and workers to finish");
        dispatcher.join().unwrap();
        println!("[MAIN] Dispatcher is closed");

        println!("[MAIN] Closing worker channels");
        for worker_tx in worker_senders {
            drop(worker_tx);
        }
        println!("[MAIN] Worker channels are closed");

        println!("[MAIN] Waiting for worker threads to finish");
        for handle in worker_handles {
            handle.join().unwrap();
        }
        println!("[MAIN] All worker threads are closed");

        println!("[MAIN] Waiting for collector thread to finish");
        collector.join().unwrap();
        println!("[MAIN] Collector thread is closed");

        println!("[MAIN] All done!");
        Ok(())
    }

    pub fn ndjson_to_parquet<R: Read+Seek, W: Write+Send>(ndjson_in: &mut R, parquet_out: W) -> parquet::errors::Result<()> {
        // Wrap input in BufReader for efficient line reading
        let mut ndjson_reader = BufReader::new(ndjson_in);

        let started_at = std::time::Instant::now();
        let (schema, _) = infer_json_schema(&mut ndjson_reader, None).expect("Failed to infer schema from NDJSON");

        let duration = started_at.elapsed();
        println!("Inferred schema from NDJSON in {:.2?}", duration);

        // TODO: Seek 0 in ndjson_reader
        ndjson_reader.get_mut().seek(std::io::SeekFrom::Start(0)).expect("Failed to seek to start of NDJSON input");

        // Infer schema from first 100 rows and build Arrow JSON reader
        let json_reader = ReaderBuilder::new(Arc::new(schema.clone()))
            .build(ndjson_reader)
            .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;

        // Create ArrowWriter for Parquet output
        let mut arrow_writer = ArrowWriter::try_new(parquet_out, Arc::new(schema), None)?;

        // Write each RecordBatch to Parquet
        for batch in json_reader {
            let batch = batch.map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
            arrow_writer.write(&batch)?;
        }

        arrow_writer.flush()?;
        arrow_writer.close()?;
        Ok(())
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
                HeterogeneousArrayStrategy::WrapInObject => b"{\"value\":".to_vec(),
                HeterogeneousArrayStrategy::KeepAsIs => Vec::new(),
            },
            is_field_value_wrapper: false,
        }
    }

    pub fn write<W: Write>(&mut self, chunk: &mut JsonsorChunk, out: &mut W) -> (bool, usize) {
        // TODO: Refactor HeterogeneousArrayStrategy::WrapInObject
        // TODO: Reduce number of clones
        // TODO: Use logger????

        // TODO: think how to handle cases of invalid JSON
        
        let (is_complete, cursor_shift) = self.write_with_stacked_streams(chunk, out);
        if !is_complete {
            return (false, cursor_shift);
        }

        let (is_complete, additional_cursor_shift) = self.write_with_current(&mut chunk.starting_from(cursor_shift), out);
        (is_complete, cursor_shift + additional_cursor_shift)
    }

    fn write_with_current<W: Write>(&mut self, chunk: &mut JsonsorChunk, out: &mut W) -> (bool, usize) {
        let mut cursor = 0;
        let mut output_buf: Vec<u8> = Vec::new();
        let obj_type = JsonsorFieldType::Object { schema: HashMap::new() };
        let array_obj_type = JsonsorFieldType::Array {
            item_type: Box::new(JsonsorFieldType::Object { schema: HashMap::new() }),
        };

        let mut previous_char = b'\0';
        while cursor < chunk.len() {
            let byte = &chunk.get_byte_by_index(cursor).expect("Cursor out of bounds");
            // println!(
            //     "{}[{}] {:?} '{}'",
            //     "\t".repeat(self.nested_level),
            //     cursor,
            //     self.current_status,
            //     *byte as char
            // );

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
                            chunk.inject_chunk(cursor, b"{\"value\":");
                        }

                        continue; // reprocess this byte in the next state
                    }
                }
                JsonsorStreamStatus::InferringFieldValueType => {
                    let expected_field_type = self.schema.get(&mut self.current_field_name_buf);

                    let (dtype, cursor_shift, content) = match *byte {
                        b'"' => (JsonsorFieldType::String, 0, vec![*byte]),
                        b'n' => (JsonsorFieldType::Null, 0, vec![*byte]),
                        b't' | b'f' => (JsonsorFieldType::Boolean, 0, vec![*byte]),
                        b'0'..=b'9' | b'-' => (JsonsorFieldType::Number, 0, vec![*byte]),
                        b'{' => {
                            // Process the nested object with the nested stream
                            // To be able to process reconciliation on partial chunks later
                            // Too ugly in case of arrays
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
                            // TODO: avoid buffering of the nested output if possible
                            let content = vec![];

                            self.correct_field_type_and_flush(&obj_type, &mut output_buf);
                            out.write_all(&output_buf).expect("Failed to write to output");
                            output_buf.clear();

                            let (is_complete_obj, processed_bytes_num) = nested_stream.write(&mut chunk.starting_from(cursor), out);
                            let inferred_type = JsonsorFieldType::Object {
                                schema: nested_stream.schema.clone(),
                            };

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
                            let content = vec![];
                            self.correct_field_type_and_flush(&array_obj_type, &mut output_buf);
                            out.write_all(&output_buf).expect("Failed to write to output");
                            output_buf.clear();
                            let (is_complete_array, processed_bytes_num) = nested_stream.write(&mut chunk.starting_from(cursor), out);
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

                            // let content = nested_stream.output_buf.clone();
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

                    cursor += cursor_shift; // advance cursor if nested object/array was processed

                    if !content.is_empty() {
                        // Do column renaming if type is not expected
                        self.correct_field_type_and_flush(&dtype, &mut output_buf);
                        output_buf.extend(content);
                    }

                    // Reset buffers and status
                    self.current_field_buf.clear();
                    self.current_status = JsonsorStreamStatus::PassingFieldValue {
                        dtype: dtype.clone(),
                    };

                    if self.schema.contains_key(&self.current_field_name_buf) && dtype == JsonsorFieldType::Null {
                        // println!("Field '{}' is already in schema, and new value is null. Keeping existing type.", String::from_utf8_lossy(&self.current_field_buf));
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
                            // TODO: Avoid use of output_buf
                            if *byte == b'"' && previous_char != b'\\' {
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
                                        self.current_status = JsonsorStreamStatus::SeekingObjectStart;
                                        out.write_all(&output_buf).expect("Failed to write to output");
                                        return (true, cursor);
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
                                        self.current_status = JsonsorStreamStatus::SeekingObjectStart;
                                        out.write_all(&output_buf).expect("Failed to write to output");
                                        return (true, cursor);
                                    }

                                    self.current_field_name_buf.clear();
                                    self.current_status = JsonsorStreamStatus::ReachedObjectEnd;
                                    output_buf.push(*byte);
                                    cursor -= chunk.injected_bytes; // adjust cursor for injected
                                                                    // bytes
                                    continue; // reprocess this byte in the next state
                                }
                                _ => {}
                            }
                        }
                    }
                    output_buf.push(*byte);
                }
                JsonsorStreamStatus::ReachedObjectEnd => {
                    // TODO: increment by 1 to make the final cursor equal to the chunk length.
                    // Why?
                    self.current_status = JsonsorStreamStatus::SeekingObjectStart;

                    // root stream should proceed till the end of the chunk
                    if self.nested_level > 0 {
                        out.write_all(&output_buf).expect("Failed to write to output");
                        return (true, cursor + 1);
                    }
                }
            }

            cursor += 1; // move to the next byte
            previous_char = *byte;

            if output_buf.len() >= 16384 {
                out.write_all(&output_buf).expect("Failed to write to output");
                output_buf.clear();
            }
        }

        if !output_buf.is_empty() {
            out.write_all(&output_buf).expect("Failed to write to output");
        }

        // println!("Exiting reconcile_object with cursor at {}", cursor);
        // SeekingObjectStart means that there is no incomplete object left to process
        return (self.current_status == JsonsorStreamStatus::SeekingObjectStart, cursor);
    }

    fn write_with_stacked_streams<W: Write>(&mut self, chunk: &mut JsonsorChunk, out: &mut W) -> (bool, usize) {
        let mut cursor = 0;
        while let Some(mut nested_stream) = self.stack.pop() {
            let (is_obj_complete, cursor_shift) = nested_stream.write(chunk, out);
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
            None => panic!("Field not found in schema when processing nested object"),
            _ => panic!("Unsupported field type for nested object"),
        };
        self.schema.insert(
            self.current_field_name_buf.clone(),
            updated_nested_type,
        );
    }

    fn correct_field_type_and_flush<W: Write>(&mut self, dtype: &JsonsorFieldType, out: &mut W) -> Vec<u8> {
        let expected_field_type = self.schema.get(&self.current_field_name_buf);
        // Do column renaming if type is not expected
        if let Some(expected_type) = expected_field_type {
            // TODO: WHat to do if expected_type = Null?
            if self.types_differ(dtype, expected_type) {
                let suffix = self.type_suffix(dtype);
                if !self.current_field_name_buf.is_empty() {
                    self.current_field_buf.extend_from_slice(suffix.as_bytes());
                    self.current_field_name_buf
                        .extend_from_slice(suffix.as_bytes());
                }
                // TODO: Should we exclude current_field_buf if it is null?
            }
        }

        out.write_all(&self.current_field_buf).expect("Failed to write to output");
        if !self.current_field_name_buf.is_empty() {
            out.write_all(b"\":").expect("Failed to write to output");
        }

        // Reset buffers and status
        self.current_field_buf.clear();
        return vec![];
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
        match byte {
            b' ' | b'\n' | b'\r' | b'\t' => true,
            _ => false,
        }
    }

}
