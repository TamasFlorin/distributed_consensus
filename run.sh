#!/bin/bash
if [ $# -lt 2 ];
then
    echo "usage: ./run.sh <mode> <node_id>";
elif [ $1 = "debug" ];
then
    cargo clippy && cargo build
    RUST_LOG=distributed_consensus ./target/debug/distributed_consensus $2 --config nodes.json --hub hub.json
elif [ $1 = "debug_no_logs" ];
then
    cargo clippy && cargo build
    ./target/debug/distributed_consensus $2 --config nodes.json --hub hub.json
else
    cargo clippy && cargo build --release
    RUST_LOG=info ./target/release/distributed_consensus $2 --config nodes.json --hub hub.json
fi