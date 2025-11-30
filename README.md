# JSON-sor

> Streaming library for JSON schema reconciliation and data harmonization

[![Build](https://github.com/ekvii/jsonsor/actions/workflows/rust.yml/badge.svg?branch=main)](https://github.com/ekvii/jsonsor/actions/workflows/rust.yml)

Status: Alpha

Reconciles the following challenges when working with JSON data:

- Data type conflicts (e.g., string field became integer)
- null values (unsupported by some systems or mistakenly inferred as string type)
- Heterogeneous arrays (unsupported by most analytical systems)
- Different cases in field names (e.g., "userID" vs "userId")
- Unsupported characters in field names (e.g., dots, dollar signs)

The library works on all nested structures of arbitrary complexity.

All problems solved in a streaming fashion, with minimal memory footprint and
minimal latency. Ready to handle huge volumes of JSON data or very intensive
stream of JSON events.

Speed references: gzipped 1.5 GB of raw NDJSON data reconciled in 8 seconds on
a single CPU (M1 Pro). Multithreaded version processes the volume in 1 second
on 8 cores (M1 Pro).

## Architecture

See [jsonsor.arch](./jsonsor.arch)
