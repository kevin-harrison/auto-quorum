from pathlib import Path

from clusters.autoquorum_cluster import AutoQuorumCluster
from clusters.autoquorum_configs import FlexibleQuorum, ReadStrategy, RequestInterval
from clusters.base_cluster import ClientServerCluster
from experiments.base_experiment import BaseExperiment, ClusterWorkload


class ShiftingConditionsExperiment(BaseExperiment):
    CLUSTER_TYPES = [
        "AutoQuorum",
        "MultiLeader-SuperMajority",
        "MultiLeader-Majority",
        "Etcd",
    ]
    DEFAULT_EXPERIMENT_DIR = (
        Path(__file__).parent.parent / "logs" / "shifting-conditions"
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
            "Etcd": lambda: self._etcd_cluster(2),
        }
        return cluster_types[self.cluster_type]()

    def _autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base().initial_leader(2).optimize_setting(False)
        )
        return cluster.build()

    def _shifting_workload(self, period: int) -> ClusterWorkload:
        if period == 1 or period == 2:
            read_ratio = 0.4
            hotspot = 2
        elif period == 3:
            read_ratio = 0.4
            hotspot = 5
        elif period == 4:
            read_ratio = 0.98
            hotspot = None
        else:
            raise ValueError("Unsupported shifting workload period")
        period_dur = 60
        low_requests = [RequestInterval(period_dur, 10, read_ratio)]
        high_requests = [RequestInterval(period_dur, 30, read_ratio)]

        workload = {}
        for node in self.nodes:
            if node == hotspot:
                requests = high_requests
            else:
                requests = low_requests
            workload[node] = requests
        return workload

    def run(self):
        print(f"RUNNING EXPERIMENT: Shifting conditions - {self.cluster_type}")
        print("PERIOD 1:")
        period1_workload = self._shifting_workload(period=1)
        self._update_workload(period1_workload)
        self.cluster.run(self.experiment_dir / f"period-1/{self.cluster_type}")

        # Period 2 (US node failure)
        print("PERIOD 2:")
        failed_node = 1
        period2_workload = self._shifting_workload(period=2)
        self._update_workload(period2_workload)
        self.cluster.change_client_config(failed_node, kill_signal_sec=0)
        if self.cluster_type == "AutoQuorum":
            self.cluster.change_cluster_config(
                initial_flexible_quorum=FlexibleQuorum(4, 2)
            )
        self.cluster.run(self.experiment_dir / f"period-2/{self.cluster_type}")

        # Period 3 (Write shift)
        print("PERIOD 3:")
        period3_workload = self._shifting_workload(period=3)
        self._update_workload(period3_workload)
        if self.cluster_type == "AutoQuorum":
            self.cluster.change_cluster_config(initial_leader=5)
        self.cluster.run(self.experiment_dir / f"period-3/{self.cluster_type}")

        # Period 4 (Read dominant)
        print("PERIOD 4:")
        period4_workload = self._shifting_workload(period=4)
        self._update_workload(period4_workload)
        if self.cluster_type == "AutoQuorum":
            self.cluster.change_cluster_config(
                initial_leader=2,
                initial_flexible_quorum=FlexibleQuorum(2, 4),
                initial_read_strat=[ReadStrategy.BallotRead] * 5,
            )
        self.cluster.run(self.experiment_dir / f"period-4/{self.cluster_type}")
