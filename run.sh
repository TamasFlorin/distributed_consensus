#!/bin/bash
cargo build --release
./target/release/distributed_consensus 0 --config nodes.json