#!/bin/bash

# Function to clean up any running server container
cleanup() {
    docker kill server > /dev/null 2>&1
}
cleanup

set -eu

# Generate fresh output directory
OUTPUT_DIR="./results"
[ -d $OUTPUT_DIR ] && rm -r $OUTPUT_DIR
mkdir -p "${OUTPUT_DIR}"

# Ensure the container is killed when this script exits.
# Note: will only work with ssh with -t flag
trap cleanup EXIT SIGHUP SIGINT SIGPIPE SIGTERM SIGQUIT

# Run the Docker container
docker run \
  -p 2379:2379 \
  -p 2380:2380 \
  --rm \
  --name server \
  --env ETCD_DATA_DIR="etcd-data" \
  --env ETCD_ALLOW_NONE_AUTHENTICATION="yes" \
  --env ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:2379" \
  --env ETCD_LISTEN_PEER_URLS="http://0.0.0.0:2380" \
  --env ETCD_INITIAL_CLUSTER_STATE="new" \
  --env ETCD_INITIAL_CLUSTER=$ETCD_INITIAL_CLUSTER \
  --env ETCD_NAME=$ETCD_NAME \
  --env ETCD_ADVERTISE_CLIENT_URLS=$ETCD_ADVERTISE_CLIENT_URLS\
  --env ETCD_INITIAL_ADVERTISE_PEER_URLS=$ETCD_INITIAL_ADVERTISE_PEER_URLS \
  gcr.io/etcd-development/etcd:v3.4.35 \
  2> "./results/xerr-server-$SERVER_ID.log"

# TODO: mount the volume to a temp file system for in-memory etcd
# https://github.com/kubernetes-sigs/kind/issues/845
  # --volume=${DATA_DIR}:/etcd-data \
