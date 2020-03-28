#!/bin/bash
if [ $# -lt 2 ];
then
    echo "usage: ./run.sh <mode> <node_id>";
elif [ $1 = "debug" ];
then
    cargo build
    ./target/debug/distributed_consensus $2 --config nodes.json
else
    cargo build --release
    ./target/release/distributed_consensus $2 --config nodes.json  
fi