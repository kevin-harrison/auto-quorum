usage="Usage: run-local-cluster.sh cluster_size"
[ -z "$1" ] &&  echo "No cluster_size given! $usage" && exit 1
cluster_size=$1

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

for ((i = 1; i <= cluster_size; i++)); do
    config_path="./server-${i}-config.toml"
    RUST_LOG=debug CONFIG_FILE="$config_path" cargo run --release --manifest-path="../Cargo.toml" --bin server &
done
wait

