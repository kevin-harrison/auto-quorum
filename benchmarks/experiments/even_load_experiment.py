from pathlib import Path

from clusters.autoquorum_cluster import AutoQuorumCluster
from clusters.autoquorum_configs import FlexibleQuorum, RequestInterval
from clusters.base_cluster import ClientServerCluster
from experiments.base_experiment import BaseExperiment, ClusterWorkload


class EvenLoadExperiment(BaseExperiment):
    CLUSTER_TYPES = [
        "AutoQuorum",
        "MultiLeader-SuperMajority",
        "MultiLeader-Majority",
        "Etcd",
        "Baseline",
    ]
    DEFAULT_EXPERIMENT_DIR = (
        Path(__file__).parent.parent / "logs" / "even-load"
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
            "MultiLeader-SuperMajority": self._multileader_supermajority_cluster,
            "MultiLeader-Majority": self._multileader_majority_cluster,
            "Etcd": lambda: self._etcd_cluster(3),
            "Baseline": self._baseline_cluster,
        }
        return cluster_types[self.cluster_type]()

    def _even_load_workload(self, read_ratio: float) -> ClusterWorkload:
        workload = {}
        requests = [
            RequestInterval(
                duration_sec=10, requests_per_sec=100, read_ratio=read_ratio
            )
        ]
        for node in self.nodes:
            workload[node] = requests
        return workload

    def _autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(3)
            .initial_quorum(FlexibleQuorum(4, 2))
            .optimize_setting(True)
            .optimize_threshold(0.85)
        )
        return cluster.build()

    def _baseline_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(3)
            .initial_quorum(FlexibleQuorum(3, 3))
            .optimize_setting(False)
        )
        return cluster.build()

    def run(self):
        print(f"RUNNING EXPERIMENT: Even Load - {self.cluster_type}")
        # for read_ratio in [0.0, 0.5, 0.95, 1.0]:
        for read_ratio in [0.5]:
            workload = self._even_load_workload(read_ratio)
            self._update_workload(workload)
            print(f"Iteration: {read_ratio=}")
            iteration_dir = self.experiment_dir / f"read-ratio-{read_ratio}"
            self.cluster.run(iteration_dir / self.cluster_type)
