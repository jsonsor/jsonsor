.PHONY: test

build:
	cargo build ${CARGO_ARGS}

test:
	RUST_BACKTRACE=full cargo test ${CARGO_ARGS} -- --nocapture --test-threads=1 ${ARGS}

benchmark:
	cargo bench
