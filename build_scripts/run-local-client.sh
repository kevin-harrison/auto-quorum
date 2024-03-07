usage="Usage: run-local-client.sh server_id server_id"
[ -z "$1" ] &&  echo "No server_id given! $usage" && exit 1
[ -z "$2" ] &&  echo "No server_id given! $usage" && exit 1

kill_children() {
    pkill -P $$
}
trap "kill_children" SIGINT

server_id=$1
other_id=$2
SERVER_ID=$server_id DURATION=20 START_DELAY=100 END_DELAY=1000 LOCAL=true cargo run --release --manifest-path="../omnipaxos_client/Cargo.toml" 1> ../../auto-quorum-benchmark/logs/test-local_client-${server_id}.log &
SERVER_ID=$other_id DURATION=20 START_DELAY=1000 END_DELAY=100 LOCAL=true cargo run --release --manifest-path="../omnipaxos_client/Cargo.toml" 1> ../../auto-quorum-benchmark/logs/test-local_client-${other_id}.log
