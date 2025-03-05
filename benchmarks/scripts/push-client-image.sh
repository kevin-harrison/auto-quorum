#!/usr/bin/env bash
set -eu

usage="Usage: push-client-image.sh [client_type]"

if [ -n "${1:-}" ] && [ "${1:-}" != "etcd" ]; then
    echo "Error: Invalid argument. Expected 'etcd', got '$1'."
    exit 1
fi

println_green() {
    printf "\033[0;32m$1\033[0m\n"
}

source ./project_env.sh # Get image names
if [ "${1:-}" = "etcd" ]; then
    image_name=$ETCD_CLIENT_DOCKER_IMAGE_NAME
else
    image_name=$CLIENT_DOCKER_IMAGE_NAME
fi

println_green "Building client docker image with name '${image_name}'"
if [ "${1:-}" = "etcd" ]; then
    docker build --platform linux/amd64 -t "${image_name}" -f  ../../etcd-client.dockerfile ../../
else
    docker build --platform linux/amd64 -t "${image_name}" -f  ../../client.dockerfile ../../
fi

println_green "Pushing '${image_name}' to registry"
docker push "${image_name}"

println_green "Done!"
