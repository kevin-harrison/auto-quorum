#!/bin/bash

usage="Usage: run-local-cluster.sh [server_type]"
cluster_size=3
server_bin="server"
rust_log="info"

if [ -n "$1" ] && [ "$1" != "multi" ]; then
    echo "Error: Invalid argument. Expected 'multi', got '$1'."
    exit 1
fi
if [ "$1" = "multi" ]; then
    server_bin="multileader-server"
fi

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Servers' output is saved into logs dir
local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

# Run servers
cluster_config_path="./cluster-config.toml"
for ((i = 1; i <= cluster_size; i++)); do
    server_config_path="./server-${i}-config.toml"
    RUST_LOG=$rust_log SERVER_CONFIG_FILE=$server_config_path CLUSTER_CONFIG_FILE=$cluster_config_path cargo run --manifest-path="../Cargo.toml" --bin $server_bin &
done
wait

