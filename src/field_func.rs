use crate::stream::FieldNameProcessor;


pub struct LowercaseFieldNameProcessor{}
impl FieldNameProcessor for LowercaseFieldNameProcessor {
    fn process_unicode(&self, field_name: &String) -> String {
        field_name.to_lowercase()
    }

    fn process_ascii(&self, field_name: &Vec<u8>) -> Vec<u8> {
        field_name.to_ascii_lowercase()
    }
}

pub struct ReplaceCharsFieldNameProcessor {
    unwanted_chars: String,
    replacement: String,
}

impl ReplaceCharsFieldNameProcessor {
    pub fn new(unwanted_chars: &str, replacement: &str) -> Self {
        Self {
            unwanted_chars: unwanted_chars.to_string(),
            replacement: replacement.to_string(),
        }
    }
}

impl FieldNameProcessor for ReplaceCharsFieldNameProcessor {
    fn process_unicode(&self, field_name: &String) -> String {
        let mut processed_name = field_name.clone();
        for ch in self.unwanted_chars.chars() {
            processed_name = processed_name.replace(ch, &self.replacement);
        }
        processed_name
    }

    fn process_ascii(&self, field_name: &Vec<u8>) -> Vec<u8> {
        let mut unwanted_table = [false; 256];
        for &b in self.unwanted_chars.as_bytes() {
            unwanted_table[b as usize] = true;
        }
        let replacement = self.replacement.as_bytes().to_vec();
        let mut out = Vec::with_capacity(field_name.len());
        for &b in field_name {
            if unwanted_table[b as usize] {
                out.extend_from_slice(&replacement);
            } else {
                out.push(b);
            }
        }
        out
    }
}
