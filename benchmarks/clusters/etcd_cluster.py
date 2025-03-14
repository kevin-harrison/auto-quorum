import subprocess
from pathlib import Path

from clusters.base_cluster import ClientServerCluster

from .etcd_configs import ClientConfig, ClusterConfig, RequestInterval, ServerConfig
from .gcp_cluster import InstanceConfig
from .startup_scripts import (
    INSTANCE_STARTUP_SCRIPT,
    RUN_CLIENT_SCRIPT,
    RUN_ETCD_SERVER_SCRIPT,
)


class EtcdCluster(ClientServerCluster[ClusterConfig]):
    """
    Orchestration class for managing an ETCD cluster on GCP.

    This class automates the setup, deployment, and management of ETCD servers and clients
    on Google Cloud Platform (GCP) instances. It abstracts the steps required to push Docker images,
    start instances, configure and launch ETCD containers, and manage logs and shutdown operations.

    Deployment Steps:
    1.   Configure project settings (See `./scripts/project_env.sh`). Configure gcloud authentication (see `./scripts/auth.sh`).
    2.   Push ETCD server and client Docker images to GCR (Google Cloud Registry).
         See `./scripts/push-server-image.sh` and `./scripts/push-client-image.sh` for details.
    3-4. `__init__` Initializes the cluster by creating GCP instances (using the GcpCluster class) for ETCD servers and clients.
         The instances will run startup scripts (passed via ClusterConfig) to configure Docker for the gcloud OS login user and assign
         DNS names to the servers.
    5-6. Use `run()` to SSH into client and server instances, pass configuration files,
         and run Docker containers from the artifact registry. This also waits for client processes to finish
         and then kills the remote processes and pulls logs from the server and client GCP instances
    7.   Use `shutdown()` to shut down the GCP instances (or leave them running for reuse).
    """

    def _start_server_command(self, server_id: int, pull_image: bool = False) -> str:
        config = self._cluster_config.server_configs[server_id]
        etcd_config = config.etcd_server_config
        start_server_command = (
            f"{{ cat <<'EOF' > run_container.sh\n{config.run_script}\nEOF\n}} &&",
            f"SERVER_ID={server_id}",
            f"ETCD_INITIAL_CLUSTER={etcd_config.initial_cluster}",
            f"ETCD_NAME={etcd_config.name}",
            f"ETCD_ADVERTISE_CLIENT_URLS={etcd_config.advertise_client_urls}",
            f"ETCD_INITIAL_ADVERTISE_PEER_URLS={etcd_config.initial_advertise_peer_urls}",
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


class EtcdClusterBuilder:
    """
    Builder class for defining and validating configurations to start a EtcdCluster.
    Relies on environment variables from `../scripts/project_env.sh` to configure settings.
    """

    def __init__(self, cluster_id: int = 1) -> None:
        env_vals = self._get_project_env_variables()
        self.cluster_id = cluster_id
        self._project_id = env_vals["PROJECT_ID"]
        self._service_account = env_vals["SERVICE_ACCOUNT"]
        self._gcloud_ssh_user = env_vals["OSLOGIN_USERNAME"]
        self._gcloud_oslogin_uid = env_vals["OSLOGIN_UID"]
        self._client_docker_image_name = env_vals["ETCD_CLIENT_DOCKER_IMAGE_NAME"]
        self._instance_startup_script = self._get_instance_startup_script()
        self._server_configs: dict[int, ServerConfig] = {}
        self._client_configs: dict[int, ClientConfig] = {}
        # Cluster-wide settings
        self._initial_leader: int | None = None

    def server(
        self,
        server_id: int,
        zone: str,
        machine_type: str = "e2-standard-8",
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
        server_address = f"{instance_config.dns_name}.internal.zone."
        server_config = ServerConfig(
            instance_config=instance_config,
            server_address=f"{server_address}:2379",
            etcd_server_config=ServerConfig.EtcdServerConfig(
                name=f"node{server_id}",
                advertise_client_urls=f"http://{server_address}:2379",
                initial_advertise_peer_urls=f"http://{server_address}:2380",
                initial_cluster="",  # dummy val
            ),
            output_dir="./results",
            kill_command="docker kill server > /dev/null 2>&1",
            run_script=self._get_run_server_script(),
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
            etcd_client_config=ClientConfig.EtcdClientConfig(
                location=zone,
                server_id=server_id,
                server_name="",  # dummy val
                server_address="",  # dummy val
                initial_leader="",  # dummy val
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

    def initial_leader(self, initial_leader: int):
        self._initial_leader = initial_leader
        return self

    def build(self) -> EtcdCluster:
        if self._initial_leader is None:
            raise ValueError("Need to set cluster's initial leader")

        # Add server_address to client configs
        for client_id, client_config in self._client_configs.items():
            server_config = self._server_configs[
                client_config.etcd_client_config.server_id
            ]
            self._client_configs[client_id] = client_config.update_config(
                server_name=server_config.etcd_server_config.name,
                server_address=server_config.server_address,
                initial_leader=f"node{self._initial_leader}",
            )

        # Add initial cluster to server configs
        initial_cluster = []
        for server_config in self._server_configs.values():
            address_mapping = f"{server_config.etcd_server_config.name}={server_config.etcd_server_config.initial_advertise_peer_urls}"
            initial_cluster.append(address_mapping)
        initial_cluster = ",".join(initial_cluster)
        for server_id, server_config in self._server_configs.items():
            self._server_configs[server_id] = server_config.update_config(
                initial_cluster=initial_cluster
            )

        cluster_config = ClusterConfig(
            etcd_cluster_config=ClusterConfig.EtcdClusterConfig(
                initial_leader=self._initial_leader,
            ),
            server_configs=self._server_configs,
            client_configs=self._client_configs,
            client_image=self._client_docker_image_name,
        )
        return EtcdCluster(self._project_id, cluster_config)

    @staticmethod
    def _get_project_env_variables() -> dict[str, str]:
        env_keys = [
            "PROJECT_ID",
            "SERVICE_ACCOUNT",
            "OSLOGIN_USERNAME",
            "OSLOGIN_UID",
            "ETCD_CLIENT_DOCKER_IMAGE_NAME",
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
        with open(RUN_ETCD_SERVER_SCRIPT, "r") as f:
            return f.read()

    @staticmethod
    def _get_run_client_script() -> str:
        with open(RUN_CLIENT_SCRIPT, "r") as f:
            return f.read()
