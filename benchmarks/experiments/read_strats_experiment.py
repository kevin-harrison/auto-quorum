from pathlib import Path

from clusters.autoquorum_cluster import AutoQuorumCluster
from clusters.autoquorum_configs import FlexibleQuorum, ReadStrategy, RequestInterval
from clusters.base_cluster import ClientServerCluster
from experiments.base_experiment import BaseExperiment, ClusterWorkload


class ReadStratsExperiment(BaseExperiment):
    CLUSTER_TYPES = [
        "AutoQuorum",
        "BallotRead",
        "QuorumRead",
        "ReadAsWrite",
    ]
    EXPERIMENT_TYPES = ["basic", "skew", "read-ratio", "absolute-rate"]
    DEFAULT_EXPERIMENT_DIR = (
        Path(__file__).parent.parent / "logs" / "read-strats"
    ).resolve()

    def __init__(
        self,
        cluster_type: str,
        experiment_type: str,
        experiment_dir: Path | None = None,
        destroy_instances: bool = False,
    ) -> None:
        super().__init__(
            cluster_type,
            experiment_dir or self.DEFAULT_EXPERIMENT_DIR,
            destroy_instances,
        )
        run_functions = {
            "basic": self.run_basic,
            "skew": self.run_varied_skew,
            "read-ratio": self.run_varied_read_ratio,
            "absolute-rate": self.run_varied_absolute_rate,
        }
        self.run_func = run_functions[experiment_type]

    def create_cluster(self) -> ClientServerCluster:
        cluster_types = {
            "AutoQuorum": self._autoquorum_cluster,
            "BallotRead": self._ballot_read_cluster,
            "QuorumRead": self._quorum_read_cluster,
            "ReadAsWrite": self._read_as_write_cluster,
        }
        return cluster_types[self.cluster_type]()

    def _autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(2)
            .initial_quorum(FlexibleQuorum(2, 4))
            .initial_read_strategy([ReadStrategy.BallotRead] * 5)
            .optimize_setting(True)
            .optimize_threshold(0.85)
        )
        return cluster.build()

    def _ballot_read_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(2)
            .initial_quorum(FlexibleQuorum(2, 4))
            .initial_read_strategy([ReadStrategy.BallotRead] * 5)
            .optimize_setting(False)
        )
        return cluster.build()

    def _quorum_read_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(2)
            .initial_quorum(FlexibleQuorum(2, 4))
            .initial_read_strategy([ReadStrategy.QuorumRead] * 5)
            .optimize_setting(False)
        )
        return cluster.build()

    def _read_as_write_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(2)
            .initial_quorum(FlexibleQuorum(4, 2))
            .initial_read_strategy([ReadStrategy.ReadAsWrite] * 5)
            .optimize_setting(False)
        )
        return cluster.build()

    def _read_strats_workload(
        self,
        read_ratio_range=None,
        total_load_range=None,
        relative_load_range=None,
        experiment_dur=20,
        default_read_ratio=0.9,
        default_total_load=300,
        default_relative_load=0.9,
    ) -> list[ClusterWorkload]:
        us_nodes = [1, 2, 3]
        workloads = []

        read_ratio_range = read_ratio_range or [default_read_ratio]
        total_load_range = total_load_range or [default_total_load]
        relative_load_range = relative_load_range or [default_relative_load]

        for read_ratio in read_ratio_range:
            for total_load in total_load_range:
                for relative_load in relative_load_range:
                    us_load = round(total_load * relative_load / 3)
                    eu_load = round(total_load * (1 - relative_load) / 2)
                    node_workloads = {}
                    for node in self.nodes:
                        if node in us_nodes:
                            node_workloads[node] = [
                                RequestInterval(experiment_dur, us_load, read_ratio)
                            ]
                        else:
                            node_workloads[node] = [
                                RequestInterval(experiment_dur, eu_load, read_ratio)
                            ]
                    workloads.append(node_workloads)
        return workloads

    def run(self):
        self.run_func()

    # Show how ballot read is superior to normal quorum read and read as write
    def run_basic(self):
        print(f"RUNNING EXPERIMENT: Read Strats Basic - {self.cluster_type}")
        base_workload = self._read_strats_workload()[0]
        self._update_workload(base_workload)
        self.cluster.run(self.experiment_dir / self.cluster_type)

    def run_varied_skew(self):
        print(f"RUNNING EXPERIMENT: Read Strats varied skew - {self.cluster_type}")
        relative_us_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
        workloads = self._read_strats_workload(relative_load_range=relative_us_loads)
        for relative_us_load, workload in zip(relative_us_loads, workloads):
            self._update_workload(workload)
            print(f"Iteration: {relative_us_load=}")
            iteration_dir = self.experiment_dir / f"us-load-{relative_us_load}"
            self.cluster.run(iteration_dir / self.cluster_type)

    def run_varied_read_ratio(self):
        print(
            f"RUNNING EXPERIMENT: Read Strats varied read ratio - {self.cluster_type}"
        )
        read_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
        workloads = self._read_strats_workload(read_ratio_range=read_ratios)
        for read_ratio, workload in zip(read_ratios, workloads):
            self._update_workload(workload)
            print(f"Iteration: {read_ratio=}")
            iteration_dir = self.experiment_dir / f"read-ratio-{read_ratio}"
            self.cluster.run(iteration_dir / self.cluster_type)

    def run_varied_absolute_rate(self):
        print(
            f"RUNNING EXPERIMENT: Read Strats varied absolute rate - {self.cluster_type}"
        )
        total_loads = [30, 90, 180, 210, 300]
        workloads = self._read_strats_workload(total_load_range=total_loads)
        for total_load, workload in zip(total_loads, workloads):
            self._update_workload(workload)
            print(f"Iteration: {total_load=}")
            iteration_dir = self.experiment_dir / f"total-load-{total_load}"
            self.cluster.run(iteration_dir / self.cluster_type)
