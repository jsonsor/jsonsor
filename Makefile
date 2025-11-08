.PHONY: tests

tests:
	RUST_BACKTRACE=full cargo test -- --nocapture --test-threads=1 ${ARGS}
