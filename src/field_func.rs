use std::sync::Arc;

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
