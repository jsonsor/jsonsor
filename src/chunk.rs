pub struct JsonsorChunk<'a> {
    data: Vec<&'a [u8]>,
    size: usize,
    pub injected_bytes: usize, // TODO: rethink the pub API
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
