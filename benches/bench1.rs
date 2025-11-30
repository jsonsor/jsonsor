use std::{fs::File, sync::Arc};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use jsonsor::{field_func::{LowercaseFieldNameProcessor, ReplaceCharsFieldNameProcessor}, jsonsor::Jsonsor};

fn bench_process_ndjson(c: &mut Criterion) {
    let path = "benches/bench1.sample.ndjson.gz";

    c.bench_with_input(BenchmarkId::new("bench1", path), &path, |b, i| {
        let init_schema = std::collections::HashMap::new();
        let config = jsonsor::stream::JsonsorConfig {
            field_name_processors: vec![
                Arc::new(LowercaseFieldNameProcessor {}),
                Arc::new(ReplaceCharsFieldNameProcessor::new(" \t\n\r,.;{}()=", "_")),
            ],
            heterogeneous_array_strategy: jsonsor::stream::HeterogeneousArrayStrategy::WrapInObject,
            exclude_null_fields: true,
            input_buffer_size: 8 * 1024,
            output_buffer_size: 8 * 1024,
        };
        let mut jsonsor = Jsonsor::new(init_schema, config);
        let mut input = File::open(i).expect("Failed to open file");

        b.iter(|| {
            let mut o = Vec::new();
            let result = jsonsor.process_stream(&mut input, &mut o);
            assert!(result.is_ok());
        })
    });
}

criterion_group!(benches, bench_process_ndjson);
criterion_main!(benches);
