usage="Usage: run-local-client.sh server_id server_id"
[ -z "$1" ] &&  echo "No server_id given! $usage" && exit 1
[ -z "$2" ] &&  echo "No server_id given! $usage" && exit 1

kill_children() {
    pkill -P $$
}
trap "kill_children" SIGINT

server_id=$1
other_id=$2
CONFIG_FILE=./client-${server_id}-config.toml  cargo run --manifest-path="../omnipaxos_client/Cargo.toml" 1> ../../auto-quorum-benchmark/logs/test-local_client-${server_id}.log &
CONFIG_FILE=./client-${other_id}-config.toml  cargo run --manifest-path="../omnipaxos_client/Cargo.toml" 1> ../../auto-quorum-benchmark/logs/test-local_client-${other_id}.log
