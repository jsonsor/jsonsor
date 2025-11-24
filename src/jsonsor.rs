use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::sync::Arc;

use flate2::read::GzDecoder;

use crate::chunk::JsonsorChunk;
use crate::stream::{JsonsorConfig, JsonsorFieldType, JsonsorStream};



pub struct Jsonsor {
    root_stream: JsonsorStream,
}
impl Jsonsor {
    pub fn new(init_schema: HashMap<Vec<u8>, JsonsorFieldType>, config: JsonsorConfig) -> Self {
        Jsonsor {
            root_stream: JsonsorStream::new(init_schema, Arc::new(config)),
        }
    }

    pub fn schema(&self) -> Arc<HashMap<Vec<u8>, JsonsorFieldType>> {
        self.root_stream.schema().clone()
    }

    pub fn process_stream<R: Read, W: Write>(
        &mut self,
        input: &mut R,
        output: &mut W,
    ) -> Result<Arc<HashMap<Vec<u8>, JsonsorFieldType>>, std::io::Error> {
        let mut buf_input = BufReader::new(input);
        let mut buf_output = BufWriter::with_capacity(self.root_stream.config().input_buffer_size, output);
        let mut input_reader: Box<dyn Read> = if Self::is_gzipped(&mut buf_input) {
            Box::new(GzDecoder::new(buf_input))
        } else {
            Box::new(buf_input)
        };

        let input_buffer_size = self.root_stream.config().input_buffer_size.clone();
        let mut buffer = vec![0; input_buffer_size];

        loop {
            let bytes_read = input_reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break; // EOF
            }

            let chunk = JsonsorChunk::new(&buffer[..bytes_read]);
            self.root_stream.write(&chunk, &mut buf_output);
        }

        Ok(self.root_stream.schema().clone())
    }

    fn is_gzipped<R: Read>(reader: &mut BufReader<R>) -> bool {
        match reader.fill_buf() {
            Ok(buf) => buf.len() >= 2 && buf[0] == 0x1f && buf[1] == 0x8b,
            Err(_) => false,
        }
    }
}

