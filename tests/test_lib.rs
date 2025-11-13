use std::{collections::HashMap, sync::Arc};

use jsonsor::{field_func::{lowercase_field_name, replace_chars_processor}, stream::{HeterogeneousArrayStrategy, JsonsorConfig, JsonsorFieldType, JsonsorStream}};


// TODO: Switch to Jsonsor API

#[test]
fn test_reconcile_case1() {
    let input = b"{\"name\": \"Alice\", \"age\": 30, \"city\": \"Wonderland\"}";

    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"name".to_vec(), JsonsorFieldType::String);
    init_schema.insert(b"age".to_vec(), JsonsorFieldType::String);
    init_schema.insert(b"city".to_vec(), JsonsorFieldType::Number);

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);
    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output = "{\"name\":\"Alice\", \"age__num\":30, \"city__str\":\"Wonderland\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_case2() {
    let input = b"{\"product\": \"Laptop\", \"price\": 999.99, \"in_stock\": true}";

    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"price".to_vec(), JsonsorFieldType::Number);
    init_schema.insert(b"in_stock".to_vec(), JsonsorFieldType::String);

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);
    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output = "{\"product\":\"Laptop\", \"price\":999.99, \"in_stock__bool\":true}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_case3() {
    // TODO: Failing test
    // Test fails on escaped quotes that makes the parser react on the wrong closing brace inside
    // the string
    let input = b"{\"text\": \"Some text with quotes \\\" {inside} \", \"another_text\": \"More } ] text\"}";
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);
    println!("Output: {:?}", String::from_utf8_lossy(&output));
    println!("Schema: {:?}", reconciliating_stream.schema);

    let expected_output =
        "{\"text\":\"Some text with quotes \\\" {inside} \", \"another_text\":\"More } ] text\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_unicode() {
    let input = b"{\"greeting \\u4e16\": \"Hello, \\u4e16\\u754c\", \"\\u306a farewell\": \"Goodbye, \\u3055\\u3089\\u306a\"}";
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);
    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output = "{\"greeting \\u4e16\":\"Hello, \\u4e16\\u754c\", \"\\u306a farewell\":\"Goodbye, \\u3055\\u3089\\u306a\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_field_name_processor_lowercase() {
    let input = b"{\"Id\": 101, \"nAMe\": \"Gadget\", \"availabLE\": false, \"TAGS\": [\"tech\", \"gadget\"], \"extra_field\": 42, \"nested_object\": {\"subField\": \"value\"}}";

    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![
                Arc::new(lowercase_field_name),
            ],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );

    let (output, is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);

    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output =
        "{\"id\":101, \"name\":\"Gadget\", \"available\":false, \"tags\":[\"tech\",\"gadget\"], \"extra_field\":42, \"nested_object\":{\"subfield\":\"value\"}}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_field_name_processor_lowercase_wrap_array_in_object() {
    let input = b"{\"Id\": 101, \"nAMe\": \"Gadget\", \"availabLE\": false, \"TAGS\": [\"tech\", \"gadget\"], \"extra_field\": 42, \"nested_object\": {\"subField\": \"value\"}}";

    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![
                Arc::new(lowercase_field_name),
            ],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::WrapInObject,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );

    let (output, is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);

    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output =
        "{\"id\":101, \"name\":\"Gadget\", \"available\":false, \"tags\":[{\"value\":\"tech\"},{\"value\":\"gadget\"}], \"extra_field\":42, \"nested_object\":{\"subfield\":\"value\"}}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_field_name_processor_unwanted_chars() {
    let input = b"{\"user.id\": 202, \"full-name\": \"Widget Pro\", \"is_active?\": true, \"preferences!\": {\"theme color\": \"dark\"}}";

    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![
                replace_chars_processor(" -!?.", "_"),
            ],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );

    let (output, is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);

    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output =
        "{\"user_id\":202, \"full_name\":\"Widget Pro\", \"is_active_\":true, \"preferences_\":{\"theme_color\":\"dark\"}}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_nested_obj_case1() {
    let input = b"{\"a\": 123.043, \"x\": {\"y\": 10, \"z\": \"test\"}, \"b\": \"abc\"}";
    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"a".to_vec(), JsonsorFieldType::Number);
    init_schema.insert(b"x".to_vec(), JsonsorFieldType::String);

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, completed, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(completed);
    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output = "{\"a\":123.043, \"x__obj\":{\"y\":10, \"z\":\"test\"}, \"b\":\"abc\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_nested_arr_case1() {
    let input = b"{\"items\": [1, 2, 3], \"details\": [{\"name\": \"Item1\", \"price\": 19.99}, {\"name\": \"Item2\", \"price\": 29.99}]}";
    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"items".to_vec(), JsonsorFieldType::String);
    init_schema.insert(
        b"details".to_vec(),
        JsonsorFieldType::Array {
            item_type: Box::new(JsonsorFieldType::String),
        },
    );

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, completed, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(completed);
    println!("Output: {:?}", String::from_utf8_lossy(&output));
    println!("Schema: {:?}", reconciliating_stream.schema);

    // TODO: Doesn't really work. items__arr__obj should be iteams__arr__num
    // It happens because the array type is hardcoded. No way to check the actual type in advance.
    let expected_output = "{\"items__arr__obj\":[1,2,3], \"details__arr__obj\":[{\"name\":\"Item1\", \"price\":19.99},{\"name\":\"Item2\", \"price\":29.99}]}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_nested_arr_case1_arr_wrap_in_object() {
    let input = b"{\"items\": [1, 2, 3], \"details\": [{\"name\": \"Item1\", \"price\": 19.99}, {\"name\": \"Item2\", \"price\": 29.99}]}";
    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"items".to_vec(), JsonsorFieldType::String);
    init_schema.insert(
        b"details".to_vec(),
        JsonsorFieldType::Array {
            item_type: Box::new(JsonsorFieldType::String),
        },
    );

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::WrapInObject,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, completed, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(completed);
    println!("Output: {:?}", String::from_utf8_lossy(&output));
    println!("Schema: {:?}", reconciliating_stream.schema);

    let expected_output = "{\"items__arr__obj\":[{\"value\":1},{\"value\":2},{\"value\":3}], \"details__arr__obj\":[{\"value\":{\"name\":\"Item1\", \"price\":19.99}},{\"value\":{\"name\":\"Item2\", \"price\":29.99}}]}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}
#[test]
fn test_reconcile_heterogeneous_arr_case1() {
    // TODO: Failing test. Array type is overridden by each new element of the array. So
    // effectively, the last element type wins.
    let input = b"{\"values\": [1, \"two\", 3.0, true, {\"key\": \"value\"}]}";
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::WrapInObject,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, completed, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(completed);
    println!("Output: {:?}", String::from_utf8_lossy(&output));
    println!("Schema: {:?}", reconciliating_stream.schema);

    let expected_output = "{\"values\":[{\"value\":1},{\"value__str\":\"two\"},{\"value\":3.0},{\"value__bool\":true},{\"value__obj\":{\"key\":\"value\"}}]}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_streaming_reconciliation1() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output1, _, _) = reconciliating_stream.reconcile_object(b"{\"id\": 1, \"value\": \"test\"");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(
        String::from_utf8_lossy(&output1),
        "{\"id\":1, \"value\":\"test\""
    );

    let (output2, _, _) = reconciliating_stream.reconcile_object(b", \"active\": true}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output2), ", \"active\":true}");

    let (output3, _, _) = reconciliating_stream.reconcile_object(b"");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), "");

    let (output4, _, _) = reconciliating_stream.reconcile_object(b"{");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output4), "{");

    let (output5, _, _) = reconciliating_stream.reconcile_object(b"\"id\":");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), ""); // No output yet, waiting for the value type
                                                      // to decide on removing it entirely

    let (output6, _, _) = reconciliating_stream.reconcile_object(b" \"2\", \"value\": null, \"active\": false");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(
        String::from_utf8_lossy(&output6),
        "\"id__str\":\"2\", \"value\":null, \"active\":false"
    );

    let (output7, _, _) = reconciliating_stream.reconcile_object(b"}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output7), "}");
}

#[test]
fn test_streaming_reconciliation1_arr_wrap_in_object() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::WrapInObject,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output1, _, _) = reconciliating_stream.reconcile_object(b"{\"id\": 1, \"value\": \"test\"");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(
        String::from_utf8_lossy(&output1),
        "{\"id\":1, \"value\":\"test\""
    );

    let (output2, _, _) = reconciliating_stream.reconcile_object(b", \"active\": true}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output2), ", \"active\":true}");

    let (output3, _, _) = reconciliating_stream.reconcile_object(b"");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), "");

    let (output4, _, _) = reconciliating_stream.reconcile_object(b"{");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output4), "{");

    let (output5, _, _) = reconciliating_stream.reconcile_object(b"\"id\":");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), ""); // No output yet, waiting for the value type
                                                      // to decide on removing it entirely

    let (output6, _, _) = reconciliating_stream.reconcile_object(b" \"2\", \"value\": null, \"active\": false");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(
        String::from_utf8_lossy(&output6),
        "\"id__str\":\"2\", \"value\":null, \"active\":false"
    );

    let (output7, _, _) = reconciliating_stream.reconcile_object(b"}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output7), "}");
}

#[test]
fn test_streaming_reconciliation2() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output1, _, _) = reconciliating_stream.reconcile_object(b"{\"x\": {");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output1), "{\"x\":{");

    let (output2, _, _) = reconciliating_stream.reconcile_object(b"\"a\": true, \"b\": 42");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output2), "\"a\":true, \"b\":42");

    let (output3, _, _) = reconciliating_stream.reconcile_object(b"");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), "");

    let (output4, _, _) = reconciliating_stream.reconcile_object(b", \"c3\": \"hello\"},");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output4), ", \"c3\":\"hello\"},");

    let (output5, _, _) = reconciliating_stream.reconcile_object(b"\"y\": 3.14}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), "\"y\":3.14}");
}

#[test]
fn test_streaming_reconciliation3() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output1, _, _) = reconciliating_stream.reconcile_object(b"{\"data\": [");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output1), "{\"data\":[");

    let (output2, _, _) = reconciliating_stream.reconcile_object(b"{\"id\": 1, \"value\": \"A\"},");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output2), "{\"id\":1, \"value\":\"A\"},");

    let (output3, _, _) = reconciliating_stream.reconcile_object(b"{\"id\": 2");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), "{\"id\":2");

    let (output4, _, _) = reconciliating_stream.reconcile_object(b", \"value__num\":200}, {\"id\": 3, \"value\": true}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output4), ", \"value__num\":200},{\"id\":3, \"value__bool\":true}");

    let (output5, _, _) = reconciliating_stream.reconcile_object(b"]}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), "]}");
}

#[test]
fn test_streaming_reconciliation3_arr_wrap_in_object() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::WrapInObject,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output1, _, _) = reconciliating_stream.reconcile_object(b"{\"data\": [");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output1), "{\"data\":[");

    let (output2, _, _) = reconciliating_stream.reconcile_object(b"{\"id\": 1, \"value\": \"A\"},");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output2), "{\"value\":{\"id\":1, \"value\":\"A\"}},");

    let (output3, _, _) = reconciliating_stream.reconcile_object(b"{\"id\": 2");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), "{\"value\":{\"id\":2");

    let (output4, _, _) = reconciliating_stream.reconcile_object(b", \"value__num\":200}, {\"id\": 3, \"value\": true}");
    print_schema(&reconciliating_stream.schema);
    // not 
    // assert_eq!(String::from_utf8_lossy(&output4), ", \"value__num\":200}},{\"value\":{\"id\":3, \"value__bool\":true}}");
    // because wrapping object has no bytes in the input after the element object is closed
    assert_eq!(String::from_utf8_lossy(&output4), ", \"value__num\":200}},{\"value\":{\"id\":3, \"value__bool\":true}");

    let (output5, _, _) = reconciliating_stream.reconcile_object(b"]}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), "}]}");
}

#[test]
fn test_streaming_reconciliation4() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output1, _, _) = reconciliating_stream.reconcile_object(b"{\"meta\": {");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(reconciliating_stream.schema.get(&b"meta".to_vec()), Some(&JsonsorFieldType::Object { schema: HashMap::new() }));
    assert_eq!(String::from_utf8_lossy(&output1), "{\"meta\":{");

    let (output2, _, _) = reconciliating_stream.reconcile_object(b"\"version\": 1");

    print_schema(&reconciliating_stream.schema);
    let mut output2_schema = HashMap::new();
    output2_schema.insert(b"version".to_vec(), JsonsorFieldType::Number);

    assert_eq!(reconciliating_stream.schema.get(&b"meta".to_vec()), Some(&JsonsorFieldType::Object { schema: output2_schema }));
    assert_eq!(String::from_utf8_lossy(&output2), "\"version\":1");

    let (output3, _, _) = reconciliating_stream.reconcile_object(b", \"type\": \"example\"}, \"data\":{");
    let mut output3_schema = HashMap::new();
    output3_schema.insert(b"version".to_vec(), JsonsorFieldType::Number);
    output3_schema.insert(b"type".to_vec(), JsonsorFieldType::String);
    assert_eq!(reconciliating_stream.schema.get(&b"meta".to_vec()), Some(&JsonsorFieldType::Object { schema: output3_schema }));
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), ", \"type\":\"example\"}, \"data\":{");

    let (output4, _, _) = reconciliating_stream.reconcile_object(b"\"items\": [\"item1\"");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output4), "\"items\":[\"item1\"");

    let (output5, _, _) = reconciliating_stream.reconcile_object(b", \"item2\"]}}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), ",\"item2\"]}}");
}

#[test]
fn test_streaming_reconciliation4_array_wrap_in_object() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::WrapInObject,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output1, _, _) = reconciliating_stream.reconcile_object(b"{\"meta\": {");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(reconciliating_stream.schema.get(&b"meta".to_vec()), Some(&JsonsorFieldType::Object { schema: HashMap::new() }));
    assert_eq!(String::from_utf8_lossy(&output1), "{\"meta\":{");

    let (output2, _, _) = reconciliating_stream.reconcile_object(b"\"version\": 1");

    print_schema(&reconciliating_stream.schema);
    let mut output2_schema = HashMap::new();
    output2_schema.insert(b"version".to_vec(), JsonsorFieldType::Number);

    assert_eq!(reconciliating_stream.schema.get(&b"meta".to_vec()), Some(&JsonsorFieldType::Object { schema: output2_schema }));
    assert_eq!(String::from_utf8_lossy(&output2), "\"version\":1");

    let (output3, _, _) = reconciliating_stream.reconcile_object(b", \"type\": \"example\"}, \"data\":{");
    let mut output3_schema = HashMap::new();
    output3_schema.insert(b"version".to_vec(), JsonsorFieldType::Number);
    output3_schema.insert(b"type".to_vec(), JsonsorFieldType::String);
    assert_eq!(reconciliating_stream.schema.get(&b"meta".to_vec()), Some(&JsonsorFieldType::Object { schema: output3_schema }));
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), ", \"type\":\"example\"}, \"data\":{");

    let (output4, _, _) = reconciliating_stream.reconcile_object(b"\"items\": [\"item1\"");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output4), "\"items\":[{\"value\":\"item1\"");

    let (output5, _, _) = reconciliating_stream.reconcile_object(b", \"item2\"]}}");
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), "},{\"value\":\"item2\"}]}}");
}

#[test]
fn test_multiline_input() {
    let input = b"{\"a\":1, \"b\":true}\n{\"a\":\"two\", \"b\":false}\n{\"a\":3.0, \"b\":\"yes\"}";
    let init_schema = std::collections::HashMap::new();
    let mut reconciliating_stream = JsonsorStream::new(
        init_schema,
        JsonsorConfig {
            field_name_processors: vec![],
            heterogeneous_array_strategy: HeterogeneousArrayStrategy::KeepAsIs,
            input_buffer_size: 8192,
            output_buffer_size: 8192,
        },
    );
    let (output, is_complete, processed_bytes) = reconciliating_stream.reconcile_object(input);
    assert!(is_complete);
    assert_eq!(processed_bytes, input.len());
    println!("Output: {:?}", String::from_utf8_lossy(&output));
    let expected_output = "{\"a\":1, \"b\":true}\n{\"a__str\":\"two\", \"b\":false}\n{\"a\":3.0, \"b__str\":\"yes\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);

    let schema = &reconciliating_stream.schema;
    println!("Schema: {:?}", schema);
}

fn print_schema(schema: &std::collections::HashMap<Vec<u8>, JsonsorFieldType>) {
    println!("Current Schema:");
    for (key, value) in schema {
        println!("  {:?}: {:?}", String::from_utf8_lossy(key), value);
    }
}
