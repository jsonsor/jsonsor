use std::{collections::HashMap, fmt::{Debug, Display, Formatter, Result}, sync::Arc};

#[derive(PartialEq, Eq, Clone)]
pub enum JsonsorFieldType {
    Number,
    String,
    Boolean,
    Null,
    Object {
        schema: Arc<HashMap<Vec<u8>, JsonsorFieldType>>,
    },
    Array {
        item_type: Box<JsonsorFieldType>,
    },
}

impl Display for JsonsorFieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            JsonsorFieldType::Null => write!(f, "Null"),
            JsonsorFieldType::Boolean => write!(f, "Boolean"),
            JsonsorFieldType::Number => write!(f, "Number"),
            JsonsorFieldType::String => write!(f, "String"),
            JsonsorFieldType::Array { item_type } => {
                write!(f, "Array of {}", item_type)
            }
            JsonsorFieldType::Object { schema } => {
                write!(f, "Object with fields: [")?;
                for (key, value) in schema.iter() {
                    write!(f, "'{}': {}, ", String::from_utf8_lossy(key), value)?;
                }
                write!(f, "]")
            }
        }
    }
}

impl JsonsorFieldType {
    pub fn is_primitive(&self) -> bool {
        matches!(self, JsonsorFieldType::Number | JsonsorFieldType::String | JsonsorFieldType::Boolean | JsonsorFieldType::Null)
    }
}

impl Debug for JsonsorFieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        Display::fmt(self, f)
    }
}
