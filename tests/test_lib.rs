#[test]
fn test_reconcile_case1() {
    let input = b"{\"name\": \"Alice\", \"age\": 30, \"city\": \"Wonderland\"}";

    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"name".to_vec(), jsonsor::JsonsorFieldType::String);
    init_schema.insert(b"age".to_vec(), jsonsor::JsonsorFieldType::String);
    init_schema.insert(b"city".to_vec(), jsonsor::JsonsorFieldType::Number);

    let mut reconciliating_stream = jsonsor::ReconciliatingStream::new(
        0,
        jsonsor::JsonsorStreamStatus::SeekingObjectStart,
        init_schema,
    );
    let (is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);
    let output = reconciliating_stream.output_buf;
    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output = "{\"name\":\"Alice\", \"age__num\":30, \"city__str\":\"Wonderland\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_case2() {
    let input = b"{\"product\": \"Laptop\", \"price\": 999.99, \"in_stock\": true}";

    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"price".to_vec(), jsonsor::JsonsorFieldType::Number);
    init_schema.insert(b"in_stock".to_vec(), jsonsor::JsonsorFieldType::String);

    let mut reconciliating_stream = jsonsor::ReconciliatingStream::new(
        0,
        jsonsor::JsonsorStreamStatus::SeekingObjectStart,
        init_schema,
    );
    let (is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);
    let output = reconciliating_stream.output_buf;
    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output = "{\"product\":\"Laptop\", \"price\":999.99, \"in_stock__bool\":true}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
#[ignore]
fn test_reconcile_case3() {
    // TODO: Failing test
    // Test fails on escaped quotes that makes the parser react on the wrong closing brace inside
    // the string
    let input = b"{\"text\": \"Some text with quotes \\\" {inside} \", \"another_text\": \"More } ] text\"}";
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = jsonsor::ReconciliatingStream::new(
        0,
        jsonsor::JsonsorStreamStatus::SeekingObjectStart,
        init_schema,
    );
    let (is_complete_obj, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(is_complete_obj);
    let output = reconciliating_stream.output_buf;
    println!("Output: {:?}", String::from_utf8_lossy(&output));
    println!("Schema: {:?}", reconciliating_stream.schema);

    let expected_output =
        "{\"text\":\"Some text with quotes \\\" inside \", \"another_text\":\"More } ] text\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_nested_obj_case1() {
    let input = b"{\"a\": 123.043, \"x\": {\"y\": 10, \"z\": \"test\"}, \"b\": \"abc\"}";
    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"a".to_vec(), jsonsor::JsonsorFieldType::Number);
    init_schema.insert(b"x".to_vec(), jsonsor::JsonsorFieldType::String);

    let mut reconciliating_stream = jsonsor::ReconciliatingStream::new(
        0,
        jsonsor::JsonsorStreamStatus::SeekingObjectStart,
        init_schema,
    );
    let (completed, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(completed);
    let output = reconciliating_stream.output_buf;
    println!("Output: {:?}", String::from_utf8_lossy(&output));

    let expected_output = "{\"a\":123.043, \"x__obj\":{\"y\":10, \"z\":\"test\"}, \"b\":\"abc\"}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_reconcile_nested_arr_case1() {
    let input = b"{\"items\": [1, 2, 3], \"details\": [{\"name\": \"Item1\", \"price\": 19.99}, {\"name\": \"Item2\", \"price\": 29.99}]}";
    let mut init_schema = std::collections::HashMap::new();
    init_schema.insert(b"items".to_vec(), jsonsor::JsonsorFieldType::String);
    init_schema.insert(
        b"details".to_vec(),
        jsonsor::JsonsorFieldType::Array {
            item_type: Box::new(jsonsor::JsonsorFieldType::String),
        },
    );

    let mut reconciliating_stream = jsonsor::ReconciliatingStream::new(
        0,
        jsonsor::JsonsorStreamStatus::SeekingObjectStart,
        init_schema,
    );
    let (completed, offset) = reconciliating_stream.reconcile_object(input);
    assert_eq!(offset, input.len());
    assert!(completed);
    let output = reconciliating_stream.output_buf;
    println!("Output: {:?}", String::from_utf8_lossy(&output));
    println!("Schema: {:?}", reconciliating_stream.schema);

    let expected_output = "{\"items__arr__num\":[1,2,3], \"details__arr__obj\":[{\"name\":\"Item1\", \"price\":19.99},{\"name\":\"Item2\", \"price\":29.99}]}";
    assert_eq!(String::from_utf8_lossy(&output), expected_output);
}

#[test]
fn test_streaming_reconciliation1() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = jsonsor::ReconciliatingStream::new(
        0,
        jsonsor::JsonsorStreamStatus::SeekingObjectStart,
        init_schema,
    );
    reconciliating_stream.reconcile_object(b"{\"id\": 1, \"value\": \"test\"");
    let output1 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(
        String::from_utf8_lossy(output1),
        "{\"id\":1, \"value\":\"test\""
    );

    reconciliating_stream.reconcile_object(b", \"active\": true}");
    let output2 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(output2), ", \"active\":true}");

    reconciliating_stream.reconcile_object(b"");
    let output3 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(output3), "");

    reconciliating_stream.reconcile_object(b"{");
    let output4 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(output4), "{");

    reconciliating_stream.reconcile_object(b"\"id\":");
    let output5 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(output5), ""); // No output yet, waiting for the value type
                                                      // to decide on removing it entirely

    reconciliating_stream.reconcile_object(b" \"2\", \"value\": null, \"active\": false");
    let output6 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(
        String::from_utf8_lossy(output6),
        "\"id__str\":\"2\", \"value\":null, \"active\":false"
    );

    reconciliating_stream.reconcile_object(b"}");
    let output7 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(output7), "}");
}

#[test]
fn test_streaming_reconciliation2() {
    let init_schema = std::collections::HashMap::new();

    let mut reconciliating_stream = jsonsor::ReconciliatingStream::new(
        0,
        jsonsor::JsonsorStreamStatus::SeekingObjectStart,
        init_schema,
    );
    reconciliating_stream.reconcile_object(b"{\"x\": {");
    let output1 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output1), "{\"x\":{");

    reconciliating_stream.reconcile_object(b"\"a\": true, \"b\": 42");
    let output2 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output2), "\"a\":true, \"b\":42");

    reconciliating_stream.reconcile_object(b"");
    let output3 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output3), "");

    reconciliating_stream.reconcile_object(b", \"c3\": \"hello\"},");
    let output4 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output4), ", \"c3\":\"hello\"},");

    reconciliating_stream.reconcile_object(b"\"y\": 3.14}");
    let output5 = &reconciliating_stream.output_buf;
    print_schema(&reconciliating_stream.schema);
    assert_eq!(String::from_utf8_lossy(&output5), "\"y\":3.14}");
}

fn print_schema(schema: &std::collections::HashMap<Vec<u8>, jsonsor::JsonsorFieldType>) {
    println!("Current Schema:");
    for (key, value) in schema {
        println!("  {:?}: {:?}", String::from_utf8_lossy(key), value);
    }
}
