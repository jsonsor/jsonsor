use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, mpsc};
use std::thread;

use flate2::read::GzDecoder;

use crate::chunk::JsonsorChunk;
use crate::stream::{HeterogeneousArrayStrategy, JsonsorConfig, JsonsorFieldType, JsonsorStream};


pub struct JsonsorParallelismConfig {
    pub num_workers: usize,
    pub worker_capacity: usize,
    pub lines_in_chunk: usize,
    pub flush_limit: usize,
}

impl Default for JsonsorParallelismConfig {
    fn default() -> Self {
        JsonsorParallelismConfig {
            num_workers: 4,
            worker_capacity: 8,
            lines_in_chunk: 1000,
            flush_limit: 10 /*MB of uncompressed data*/ * 1024 * 1024,
        }
    }
}

pub struct Jsonsor;
impl Jsonsor {
    pub fn process_stream<R: Read, W: Write>(
        input: &mut R,
        output: &mut W,
        init_schema: HashMap<Vec<u8>, JsonsorFieldType>,
        config: JsonsorConfig,
    ) -> Result<Arc<HashMap<Vec<u8>, JsonsorFieldType>>, std::io::Error> {
        let mut buf_input = BufReader::new(input);
        let mut buf_output = BufWriter::with_capacity(config.input_buffer_size, output);
        let mut input_reader: Box<dyn Read> = if Self::is_gzipped(&mut buf_input) {
            Box::new(flate2::read::GzDecoder::new(buf_input))
        } else {
            Box::new(buf_input)
        };

        let input_buffer_size = config.input_buffer_size.clone();
        let mut jsonsor_stream = JsonsorStream::new(init_schema, config);
        let mut buffer = vec![0; input_buffer_size];

        loop {
            let bytes_read = input_reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break; // EOF
            }

            let chunk = JsonsorChunk::new(&buffer[..bytes_read]);
            jsonsor_stream.write(&chunk, &mut buf_output);
        }

        Ok(jsonsor_stream.schema)
    }

    pub fn process_stream_par<R: Read + 'static, W: Write + Send + 'static>(
        input: R,
        output: W,
        init_schema: HashMap<Vec<u8>, JsonsorFieldType>,
        config: JsonsorConfig,
        parallelism_config: JsonsorParallelismConfig,
    ) -> Result<HashMap<Vec<u8>, JsonsorFieldType>, std::io::Error> {

        fn process_chunk(worker_jsonsor_stream: &mut JsonsorStream, chunk: Vec<u8>) -> Vec<u8> {
            let mut out = BufWriter::with_capacity(8192, Vec::with_capacity(chunk.len() + 1024));
            let (_, _) = worker_jsonsor_stream.write(&mut JsonsorChunk::new(&chunk), &mut out);
            let out_data = out.into_inner().expect("Failed to get inner buffer from BufWriter");
            out_data
        }

        Self::process_file_in_parallel(
            input,
            output,
            process_chunk,
            parallelism_config.num_workers,
            parallelism_config.worker_capacity,
            parallelism_config.lines_in_chunk,
            parallelism_config.flush_limit,
        ).expect("Failed to process gzipped lines");

        Ok(init_schema)
    }

    fn process_file_in_parallel<R, F, W>(
        input: R,
        mut output: W,
        process_fn: F,
        num_workers: usize,
        worker_capacity: usize,
        lines_in_chunk: usize,
        flush_limit: usize,
    ) -> std::io::Result<()>
    where
        R: Read + 'static,
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
                    input_buffer_size: 8192,
                    output_buffer_size: 8192,
                    exclude_null_fields: false,
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
        let collector = thread::spawn(move || {
            println!("[COLLECTOR] Listening for results");
            let mut next_idx = 0;
            let mut buffer = BTreeMap::new();
            let mut output_buffer = Vec::with_capacity(flush_limit);

            while let Ok((idx, result)) = result_rx.recv() {
                println!("[COLLECTOR] Received result for chunk {}", idx);
                buffer.insert(idx, result);
                while buffer.contains_key(&next_idx) {
                    let result = buffer.remove(&next_idx).unwrap();
                    output_buffer.extend_from_slice(&result);
                    next_idx += 1;
                    if output_buffer.len() >= flush_limit {
                        println!("[COLLECTOR] Flushing interim output buffer with {} bytes", output_buffer.len());
                        output.write_all(&output_buffer).unwrap();
                        output_buffer.clear();
                    }
                }
            }

            if !output_buffer.is_empty() {
                println!("[COLLECTOR] Flushing final output buffer with {} bytes", output_buffer
                    .len());
                output.write_all(&output_buffer).unwrap();
            }
            output.flush().unwrap();
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

    fn is_gzipped<R: Read>(reader: &mut BufReader<R>) -> bool {
        match reader.fill_buf() {
            Ok(buf) => buf.len() >= 2 && buf[0] == 0x1f && buf[1] == 0x8b,
            Err(_) => false,
        }
    }
}
