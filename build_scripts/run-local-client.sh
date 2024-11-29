usage="Usage: run-local-client.sh first_clients_server_id second_clients_server_id"
[ -z "$1" ] &&  echo "No first_clients_server_id given! $usage" && exit 1
[ -z "$2" ] &&  echo "No second_clients_server_id given! $usage" && exit 1

server_id=$1
other_id=$2
client1_config_path="./client-${server_id}-config.toml"
client2_config_path="./client-${other_id}-config.toml"
client1_log_path="../../auto-quorum-benchmark/logs/test-local_client-${server_id}.log"
client2_log_path="../../auto-quorum-benchmark/logs/test-local_client-${other_id}.log"

interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

RUST_LOG=debug CONFIG_FILE="$client1_config_path"  cargo run --release --manifest-path="../Cargo.toml" --bin client 1> "$client1_log_path" &
RUST_LOG=debug CONFIG_FILE="$client2_config_path"  cargo run --release --manifest-path="../Cargo.toml" --bin client 1> "$client2_log_path"
