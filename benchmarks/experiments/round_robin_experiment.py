from pathlib import Path

from clusters.autoquorum_cluster import AutoQuorumCluster
from clusters.autoquorum_configs import FlexibleQuorum, RequestInterval
from clusters.base_cluster import ClientServerCluster
from experiments.base_experiment import BaseExperiment, ClusterWorkload


class RoundRobinExperiment(BaseExperiment):
    CLUSTER_TYPES = [
        "AutoQuorum",
        "Baseline",
        "MultiLeader-SuperMajority",
        "MultiLeader-Majority",
        "Etcd",
    ]
    DEFAULT_EXPERIMENT_DIR = (
        Path(__file__).parent.parent / "logs" / "round-robin"
    ).resolve()

    def __init__(
        self,
        cluster_type: str,
        experiment_dir: Path | None = None,
        destroy_instances: bool = False,
    ) -> None:
        super().__init__(
            cluster_type,
            experiment_dir or self.DEFAULT_EXPERIMENT_DIR,
            destroy_instances,
        )

    def create_cluster(self) -> ClientServerCluster:
        cluster_types = {
            "AutoQuorum": self._autoquorum_cluster,
            "Baseline": self._baseline_cluster,
            "MultiLeader-SuperMajority": self._multileader_supermajority_cluster,
            "MultiLeader-Majority": self._multileader_majority_cluster,
            "Etcd": lambda: self._etcd_cluster(1),
        }
        return cluster_types[self.cluster_type]()

    def _autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(1)
            .optimize_setting(True)
            .optimize_threshold(0.85)
            .initial_quorum(FlexibleQuorum(4, 2))
        )
        return cluster.build()

    def _baseline_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(1)
            .optimize_setting(False)
            .initial_quorum(FlexibleQuorum(3, 3))
        )
        return cluster.build()

    def _round_robin_workload(self) -> ClusterWorkload:
        experiment_duration = 60
        read_ratio = 0.0
        total_load = 300
        relative_load = 0.9

        high_load = round(total_load * relative_load)
        low_load = round(total_load * (1 - relative_load) / 4)
        high_requests = RequestInterval(experiment_duration, high_load, read_ratio)
        low_requests = RequestInterval(experiment_duration, low_load, read_ratio)
        workload = {}
        nodes = self.nodes.keys()
        for node in nodes:
            requests = [high_requests if id == node else low_requests for id in nodes]
            workload[node] = requests
        return workload

    def run(self):
        print(f"RUNNING EXPERIMENT: Round Robin - {self.cluster_type}")
        workload = self._round_robin_workload()
        self._update_workload(workload)
        self.cluster.run(self.experiment_dir / self.cluster_type)
