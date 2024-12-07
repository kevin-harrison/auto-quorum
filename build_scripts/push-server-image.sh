#!/usr/bin/env bash

usage="Usage: push-server-image.sh [server_type]"
server_bin="server"
if [ -n "$1" ]; then
    server_bin="multileader-server"
fi

set -eu
println_green() {
    printf "\033[0;32m$1\033[0m\n"
}

project_id=my-project-1499979282244
image_name="gcr.io/${project_id}/autoquorum_${server_bin}"

println_green "Building ${server_bin} docker image with name '${image_name}'"
docker build -t "${image_name}" -f  ../${server_bin}.dockerfile ../

println_green "Pushing '${image_name}' to registry"
docker push "${image_name}"

println_green "Done!"
