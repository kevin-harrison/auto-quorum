# AutoQuorum Benchmarks
Benchmarking code for configuring and deploying AutoQuorum servers and clients to [GCP](https://cloud.google.com) as docker containers. Uses the GCP python client API to provison GCP instances. Then uses gcloud for authentication and starting servers/clients via SSHing into the provisioned instances.

Documentation on the GCP python client API seems to be scarce. The best resource I've found are the samples [here](https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/compute).
## Prerequisites
 - [gcloud](https://cloud.google.com/sdk/gcloud) CLI tool for interacting with GCP
 - [uv](https://docs.astral.sh/uv/) a Python project/package manager.
## Project Structure
 - `setup-gcp.sh` Initial GCP setup. Only necessary if you are starting a new GCP project.
 - `scripts` Utilities for configuring the environment and pushing to GCP artifact registry
 - `clusters/` Classes for creating and managing GCP instances clusters.
 - `experiments/` Classes for running GCP experiments with defined clusters and workloads
 - `graphs/` Scripts for graphing the results of the experiments
 - `benchmarks.py` CLI for running experiments in `experiments/`, results are saved to `logs/`
 - `graph_experiments.ipynb` Graph experiment data in `logs/`
## Deployment steps
![gcp-diagram](https://github.com/user-attachments/assets/7dcea25f-f2f5-44a9-a15e-7c18a7e5f517)

## To Run
 1. Have an owner of the GCP project add you to the project and configure your permissions. Or start your own project with the help of `setup-gcp.sh`
 2. Copy the contents of `./scripts/project_env_template` to `./scripts/project_env.sh` and then configure environment variables in `./scripts/project_env.sh`
 3. Run the commands in `./scripts/auth.sh` to configure your gcloud credentials
 4. Run `./scripts/push-server-image.sh` and `./scripts/push-client-image.sh` to push docker images to GCP Artifact Registry
 5. Run python code with `uv run <python-file-here>`.
     - `uv run benchmarks.py` to run AutoQuorum benchmark experiments
     - `uv run --with jupyter jupyter notebook graph_experiments.ipynb` to run graphing notebook

