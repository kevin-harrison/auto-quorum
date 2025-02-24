#!/usr/bin/env bash
set -eu

usage="Usage: push-server-image.sh [server_type]"

if [ -n "${1:-}" ] && [ "${1:-}" != "multi" ]; then
    echo "Error: Invalid argument. Expected 'multi', got '$1'."
    exit 1
fi

println_green() {
    printf "\033[0;32m$1\033[0m\n"
}

source ./project_env.sh # Get PROJECT_NAME and SERVER_DOCKER_IMAGE_NAME env vars
if [ "${1:-}" = "multi" ]; then
    image_name=$MULTILEADER_SERVER_DOCKER_IMAGE_NAME
else
    image_name=$SERVER_DOCKER_IMAGE_NAME
fi

println_green "Building server docker image with name '${image_name}'"
if [ "${1:-}" = "multi" ]; then
    docker build --platform linux/amd64 -t "${image_name}" -f  ../../multileader-server.dockerfile ../../
else
    docker build --platform linux/amd64 -t "${image_name}" -f  ../../server.dockerfile ../../
fi

println_green "Pushing '${image_name}' to registry"
docker push "${image_name}"

println_green "Done!"
