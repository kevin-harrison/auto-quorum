from abc import ABC, abstractmethod
from pathlib import Path

from clusters.autoquorum_cluster import AutoQuorumClusterBuilder
from clusters.autoquorum_configs import FlexibleQuorum, RequestInterval
from clusters.base_cluster import ClientServerCluster
from clusters.etcd_cluster import EtcdCluster, EtcdClusterBuilder
from clusters.multileader_cluster import MultiLeaderCluster, MultiLeaderClusterBuilder

ClusterWorkload = dict[int, list[RequestInterval]]


class BaseExperiment(ABC):
    CLUSTER_TYPES = None
    EXPERIMENT_TYPES = None

    def __init__(
        self,
        cluster_type: str,
        experiment_dir: Path,
        destroy_instances: bool = False,
    ) -> None:
        self.experiment_dir = experiment_dir
        self.destroy_instances = destroy_instances
        self.nodes = {
            1: "us-west2-a",
            2: "us-south1-a",
            3: "us-east4-a",
            4: "europe-southwest1-a",
            5: "europe-west4-a",
        }
        self.cluster_type = cluster_type
        self.cluster: ClientServerCluster = self.create_cluster()

    def __del__(self):
        if self.destroy_instances:
            self.cluster.shutdown()

    @abstractmethod
    def create_cluster(self) -> ClientServerCluster:
        """Creates the appropriate cluster based on the experiment's cluster type."""
        pass

    def _add_nodes(self, cluster_builder):
        for id, zone in self.nodes.items():
            cluster_builder = cluster_builder.server(id, zone)
            cluster_builder = cluster_builder.client(id, zone)
        return cluster_builder

    def _autoquorum_builder_base(self) -> AutoQuorumClusterBuilder:
        cluster = AutoQuorumClusterBuilder()
        return self._add_nodes(cluster)

    def _multileader_supermajority_cluster(self) -> MultiLeaderCluster:
        cluster = MultiLeaderClusterBuilder().flex_quorum(FlexibleQuorum(4, 4))
        return self._add_nodes(cluster).build()

    def _multileader_majority_cluster(self) -> MultiLeaderCluster:
        cluster = MultiLeaderClusterBuilder().flex_quorum(FlexibleQuorum(3, 3))
        return self._add_nodes(cluster).build()

    def _etcd_cluster(self, leader: int) -> EtcdCluster:
        cluster = EtcdClusterBuilder().initial_leader(leader)
        return self._add_nodes(cluster).build()

    def _update_workload(self, workload: ClusterWorkload):
        for id, requests in workload.items():
            self.cluster.change_client_config(id, requests=requests)

    @abstractmethod
    def run(self):
        """Runs the experiment."""
        pass
