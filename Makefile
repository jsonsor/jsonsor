.PHONY: tests

tests:
	RUST_BACKTRACE=full cargo test ${CARGO_ARGS} -- --nocapture --test-threads=1 ${ARGS}
