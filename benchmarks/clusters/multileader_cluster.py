import subprocess
from pathlib import Path

from clusters.autoquorum_configs import (
    ClientConfig,
    FlexibleQuorum,
    RequestInterval,
    ServerConfig,
)

from .base_cluster import ClientServerCluster
from .gcp_cluster import InstanceConfig
from .multileader_configs import ClusterConfig
from .startup_scripts import (
    INSTANCE_STARTUP_SCRIPT,
    RUN_CLIENT_SCRIPT,
    RUN_SERVER_SCRIPT,
)


class MultiLeaderCluster(ClientServerCluster[ClusterConfig]):
    """
    Orchestration class for managing a MultiLeader cluster on GCP.

    This class automates the setup, deployment, and management of MultiLeader servers and clients
    on Google Cloud Platform (GCP) instances. It abstracts the steps required to push Docker images,
    start instances, configure and launch MultiLeader containers, and manage logs and shutdown operations.

    Deployment Steps:
    1.   Configure project settings (See `./scripts/project_env.sh`). Configure gcloud authentication (see `./scripts/auth.sh`).
    2.   Push MultiLeader server and client Docker images to GCR (Google Cloud Registry).
         See `./scripts/push-server-image.sh` and `./scripts/push-client-image.sh` for details.
    3-4. `__init__` Initializes the cluster by creating GCP instances (using the GcpCluster class) for MultiLeader servers and clients.
         The instances will run startup scripts (passed via ClusterConfig) to configure Docker for the gcloud OS login user and assign
         DNS names to the servers.
    5-6. Use `run()` to SSH into client and server instances, pass configuration files,
         and run Docker containers from the artifact registry. This also waits for client processes to finish
         and then kills the remote processes and pulls logs from the server and client GCP instances
    7.   Use `shutdown()` to shut down the GCP instances (or leave them running for reuse).
    """

    def _start_server_command(self, server_id: int, pull_image: bool = False) -> str:
        config = self._cluster_config.server_configs[server_id]
        aq_config = config.autoquorum_server_config
        server_config_toml = config.generate_server_toml()
        cluster_config_toml = self._cluster_config.generate_cluster_toml()
        start_server_command = (
            f"{{ cat <<'EOF' > run_container.sh\n{config.run_script}\nEOF\n}} &&",
            f"PULL_IMAGE={'true' if pull_image else 'false'}",
            f"SERVER_IMAGE={self._cluster_config.server_image}",
            f"SERVER_CONFIG_TOML=$(cat <<EOF\n{server_config_toml}\nEOF\n)",
            f"CLUSTER_CONFIG_TOML=$(cat <<EOF\n{cluster_config_toml}\nEOF\n)",
            f"LISTEN_PORT={aq_config.listen_port}",
            f"RUST_LOG={config.rust_log}",
            f"SERVER_ID={aq_config.server_id}",
            "bash ./run_container.sh",
        )
        return " ".join(start_server_command)

    def _start_client_command(self, client_id: int, pull_image: bool = False) -> str:
        config = self._cluster_config.client_configs[client_id]
        client_config_toml = config.generate_client_toml()
        start_client_command = (
            f"{{ cat <<'EOF' > run_container.sh\n{config.run_script}\nEOF\n}} &&",
            f"PULL_IMAGE={'true' if pull_image else 'false'}",
            f"CLIENT_CONFIG_TOML=$(cat <<EOF\n{client_config_toml}\nEOF\n)",
            f"RUST_LOG={config.rust_log}",
            f"CLIENT_IMAGE={self._cluster_config.client_image}",
            "bash ./run_container.sh",
        )
        return " ".join(start_client_command)


class MultiLeaderClusterBuilder:
    """
    Builder class for defining and validating configurations to start a MultiLeaderCluster.
    Relies on environment variables from `../scripts/project_env.sh` to configure settings.
    """

    def __init__(self, cluster_id: int = 1) -> None:
        env_vals = self._get_project_env_variables()
        self.cluster_id = cluster_id
        self._project_id = env_vals["PROJECT_ID"]
        self._service_account = env_vals["SERVICE_ACCOUNT"]
        self._gcloud_ssh_user = env_vals["OSLOGIN_USERNAME"]
        self._gcloud_oslogin_uid = env_vals["OSLOGIN_UID"]
        self._server_docker_image_name = env_vals[
            "MULTILEADER_SERVER_DOCKER_IMAGE_NAME"
        ]
        self._client_docker_image_name = env_vals["CLIENT_DOCKER_IMAGE_NAME"]
        self._instance_startup_script = self._get_instance_startup_script()
        self._server_configs: dict[int, ServerConfig] = {}
        self._client_configs: dict[int, ClientConfig] = {}
        # Cluster-wide settings
        self._server_port: int = 8000
        self._flex_quorum: FlexibleQuorum | None = None

    def server(
        self,
        server_id: int,
        zone: str,
        machine_type: str = "e2-standard-8",
        rust_log: str = "info",
    ):
        if server_id in self._server_configs.keys():
            raise ValueError(f"Server {server_id} already exists")
        instance_name = f"user-{self._gcloud_oslogin_uid}-cluster-{self.cluster_id}-server-{server_id}"
        instance_config = InstanceConfig(
            name=instance_name,
            zone=zone,
            machine_type=machine_type,
            startup_script=self._instance_startup_script,
            custom_metadata={
                "oslogin_user": self._gcloud_ssh_user,
            },
            dns_name=instance_name,
            service_account=self._service_account,
        )
        server_address = (
            f"{instance_config.dns_name}.internal.zone.:{self._server_port}"
        )
        server_config = ServerConfig(
            instance_config=instance_config,
            server_address=server_address,
            autoquorum_server_config=ServerConfig.AutoQuorumServerConfig(
                location=zone,
                server_id=server_id,
                listen_address="0.0.0.0",
                listen_port=self._server_port,
                num_clients=0,  # dummy value
                output_filepath=f"server-{server_id}.json",
            ),
            output_dir="./results",
            kill_command="docker kill server > /dev/null 2>&1",
            run_script=self._get_run_server_script(),
            rust_log=rust_log,
        )
        self._server_configs[server_id] = server_config
        return self

    def client(
        self,
        server_id: int,
        zone: str,
        requests: list[RequestInterval] = [],
        kill_signal_sec: int | None = None,
        machine_type: str = "e2-standard-2",
        rust_log: str = "info",
    ):
        if server_id in self._client_configs.keys():
            raise ValueError(f"Client {server_id} already exists")
        instance_config = InstanceConfig(
            name=f"user-{self._gcloud_oslogin_uid}-cluster-{self.cluster_id}-client-{server_id}",
            zone=zone,
            machine_type=machine_type,
            startup_script=self._instance_startup_script,
            service_account=self._service_account,
            custom_metadata={
                "oslogin_user": self._gcloud_ssh_user,
            },
        )
        client_config = ClientConfig(
            instance_config=instance_config,
            autoquorum_client_config=ClientConfig.AutoQuorumClientConfig(
                location=zone,
                server_id=server_id,
                server_address="",  # dummy value
                requests=requests,
                kill_signal_sec=kill_signal_sec,
                summary_filepath=f"client-{server_id}.json",
                output_filepath=f"client-{server_id}.csv",
            ),
            output_dir="./results",
            kill_command="docker kill client > /dev/null 2>&1",
            run_script=self._get_run_client_script(),
            rust_log=rust_log,
        )
        self._client_configs[server_id] = client_config
        return self

    def flex_quorum(self, flex_quorum: FlexibleQuorum):
        self._flex_quorum = flex_quorum
        return self

    def build(self) -> MultiLeaderCluster:
        # Add num_clients to server configs
        for server_id, server_config in self._server_configs.items():
            client_configs = self._client_configs.values()
            server_id_matches = sum(
                1
                for _ in filter(
                    lambda c: c.autoquorum_client_config.server_id == server_id,
                    client_configs,
                )
            )
            total_matches = server_id_matches
            self._server_configs[server_id] = server_config.update_config(
                num_clients=total_matches
            )
        # Add server_address to client configs
        for client_id, client_config in self._client_configs.items():
            server_config = self._server_configs[
                client_config.autoquorum_client_config.server_id
            ]
            self._client_configs[client_id] = client_config.update_config(
                server_address=server_config.server_address
            )
        nodes = sorted(self._server_configs.keys())
        node_addrs = list(
            map(lambda id: self._server_configs[id].server_address, nodes)
        )

        cluster_config = ClusterConfig(
            multileader_cluster_config=ClusterConfig.MultiLeaderClusterConfig(
                nodes=nodes,
                node_addrs=node_addrs,
                initial_flexible_quorum=self._flex_quorum,
            ),
            server_configs=self._server_configs,
            client_configs=self._client_configs,
            client_image=self._client_docker_image_name,
            server_image=self._server_docker_image_name,
        )
        return MultiLeaderCluster(self._project_id, cluster_config)

    @staticmethod
    def _get_project_env_variables() -> dict[str, str]:
        env_keys = [
            "PROJECT_ID",
            "SERVICE_ACCOUNT",
            "OSLOGIN_USERNAME",
            "OSLOGIN_UID",
            "CLIENT_DOCKER_IMAGE_NAME",
            "MULTILEADER_SERVER_DOCKER_IMAGE_NAME",
        ]
        base_dir = Path(__file__).resolve().parent
        env_path = (base_dir / ".." / "scripts" / "project_env.sh").resolve()
        env_vals = {}
        process = subprocess.run(
            ["bash", "-c", f"source {env_path} && env"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        for line in process.stdout.split("\n"):
            (key, _, value) = line.partition("=")
            if key in env_keys:
                env_keys.remove(key)
                env_vals[key] = value
        for key in env_keys:
            raise ValueError(f"{key} env var must be set. Sourcing from {env_path}")
        return env_vals

    @staticmethod
    def _get_instance_startup_script() -> str:
        with open(INSTANCE_STARTUP_SCRIPT, "r") as f:
            return f.read()

    @staticmethod
    def _get_run_server_script() -> str:
        with open(RUN_SERVER_SCRIPT, "r") as f:
            return f.read()

    @staticmethod
    def _get_run_client_script() -> str:
        with open(RUN_CLIENT_SCRIPT, "r") as f:
            return f.read()
