#!/bin/bash

usage="Usage: run-local-cluster.sh [server_type]"
cluster_size=3
server_bin="server"
if [ -n "$1" ]; then
    server_bin="multileader-server"
fi

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

for ((i = 1; i <= cluster_size; i++)); do
    config_path="./server-${i}-config.toml"
    RUST_LOG=debug CONFIG_FILE="$config_path" cargo run --release --manifest-path="../Cargo.toml" --bin $server_bin &
done
wait

