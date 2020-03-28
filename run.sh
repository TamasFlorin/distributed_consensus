#!/bin/bash
echo $1
if [ $1 = "debug" ];
then
    cargo build
    ./target/debug/distributed_consensus 0 --config nodes.json
else
    cargo build --release
    ./target/release/distributed_consensus 0 --config nodes.json  
fi