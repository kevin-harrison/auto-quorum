#!/usr/bin/env bash
set -eux

GREEN="\033[0;32m"
NO_COLOR="\033[0m"

project_id=auto-quorum-benchmark
image_name="gcr.io/${project_id}/omnipaxos_client"
deployment_service_account_key_location=~/rise/auto-quorum-benchmark/service-account-key.json

printf "${GREEN}Building client docker image with name '${image_name}'${NO_COLOR}\n"
sudo docker build -t "${image_name}" -f  ./../omnipaxos_client/Dockerfile ./..

printf "${GREEN}Authenticating docker${NO_COLOR}\n"
cat "${deployment_service_account_key_location}" | sudo docker login -u _json_key --password-stdin https://gcr.io

printf "${GREEN}Pushing '${image_name}' to registry${NO_COLOR}\n"
sudo docker push "${image_name}"

printf "\n\n${GREEN}Done!${NO_COLOR}\n"
