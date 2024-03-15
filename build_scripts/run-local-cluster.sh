usage="Usage: run-local-cluster.sh cluster_size"
[ -z "$1" ] &&  echo "No cluster_size given! $usage" && exit 1

kill_children() {
    pkill -P $$
}
trap "kill_children" SIGINT

cluster_size=$1

rm -rf ../../auto-quorum-benchmark/logs/test-local*
for ((i = 1; i <= cluster_size; i++)); do
    RUST_LOG=debug CONFIG_FILE=server-${i}-config.toml cargo run --release --manifest-path="../omnipaxos_server/Cargo.toml" 1> ../../auto-quorum-benchmark/logs/test-local_server-${i}.log &
done
wait


