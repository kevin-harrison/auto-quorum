usage="Usage: run-local-client.sh server_id server_id"
[ -z "$1" ] &&  echo "No server_id given! $usage" && exit 1
[ -z "$2" ] &&  echo "No server_id given! $usage" && exit 1

server_id=$1
other_id=$2
client1_config_path="./client-${server_id}-config.toml"
client2_config_path="./client-${other_id}-config.toml"

sync_buffer=1000
utc_sync=$(($(date +%s%3N) + sync_buffer))

interrupt() {
    pkill -P $$
    cleanup_logs
}
cleanup_logs() {
    sed -i "s/$utc_sync/SCHEDULE/g" "$client1_config_path"
    sed -i "s/$utc_sync/SCHEDULE/g" "$client2_config_path"
}
trap "interrupt" SIGINT
trap "cleanup_logs" EXIT


sed -i "s/SCHEDULE/$utc_sync/g" "$client1_config_path" &&
sed -i "s/SCHEDULE/$utc_sync/g" "$client2_config_path" &&

CONFIG_FILE="$client1_config_path"  cargo run --manifest-path="../omnipaxos_client/Cargo.toml" 1> ../../auto-quorum-benchmark/logs/test-local_client-${server_id}.log &
CONFIG_FILE="$client2_config_path"  cargo run --manifest-path="../omnipaxos_client/Cargo.toml" 1> ../../auto-quorum-benchmark/logs/test-local_client-${other_id}.log

