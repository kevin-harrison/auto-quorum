from pathlib import Path

from clusters.autoquorum_cluster import AutoQuorumCluster
from clusters.autoquorum_configs import FlexibleQuorum, ReadStrategy, RequestInterval
from clusters.base_cluster import ClientServerCluster
from experiments.base_experiment import BaseExperiment, ClusterWorkload


class MixedStratsExperiment(BaseExperiment):
    CLUSTER_TYPES = [
        "AutoQuorum",
        "Mixed",
        "BallotRead",
        "ReadAsWrite",
    ]
    EXPERIMENT_TYPES = ["basic", "skew", "read-ratio"]
    DEFAULT_EXPERIMENT_DIR = (
        Path(__file__).parent.parent / "logs" / "mixed-strats"
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
        }
        self.run_func = run_functions[experiment_type]

    def create_cluster(self) -> ClientServerCluster:
        cluster_types = {
            "AutoQuorum": self._autoquorum_cluster,
            "Mixed": self._mixed_read_cluster,
            "BallotRead": self._ballot_read_cluster,
            "ReadAsWrite": self._read_as_write_cluster,
        }
        return cluster_types[self.cluster_type]()

    def _autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(1)
            .initial_quorum(FlexibleQuorum(4, 2))
            .initial_read_strategy(
                (2 * [ReadStrategy.ReadAsWrite]) + (3 * [ReadStrategy.BallotRead])
            )
            .optimize_setting(True)
            .optimize_threshold(0.85)
        )
        return cluster.build()

    def _mixed_read_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(1)
            .initial_quorum(FlexibleQuorum(4, 2))
            .initial_read_strategy(
                (2 * [ReadStrategy.ReadAsWrite]) + (3 * [ReadStrategy.BallotRead])
            )
            .optimize_setting(False)
        )
        return cluster.build()

    def _ballot_read_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(1)
            .initial_quorum(FlexibleQuorum(4, 2))
            .initial_read_strategy([ReadStrategy.BallotRead] * 5)
            .optimize_setting(False)
        )
        return cluster.build()

    def _read_as_write_cluster(self) -> AutoQuorumCluster:
        cluster = (
            self._autoquorum_builder_base()
            .initial_leader(1)
            .initial_quorum(FlexibleQuorum(4, 2))
            .initial_read_strategy([ReadStrategy.ReadAsWrite] * 5)
            .optimize_setting(False)
        )
        return cluster.build()

    def _mixed_strats_workload(
        self,
        read_ratio_range=None,
        total_load_range=None,
        relative_load_range=None,
        experiment_dur=20,
        default_read_ratio=0.5,
        default_total_load=150,
        default_relative_load=0.9,
    ) -> list[ClusterWorkload]:
        left_node = 1
        right_nodes = [4, 5]
        center_nodes = [2, 3]
        workloads = []

        read_ratio_range = read_ratio_range or [default_read_ratio]
        total_load_range = total_load_range or [default_total_load]
        relative_load_range = relative_load_range or [default_relative_load]

        for read_ratio in read_ratio_range:
            for total_load in total_load_range:
                left_load = round(total_load * 0.5)
                for relative_load in relative_load_range:
                    right_load = round(total_load * 0.5 * relative_load / 2)
                    center_load = round(total_load * 0.5 * (1 - relative_load) / 2)
                    assert right_load > 0
                    assert center_load > 0
                    node_workloads = {}
                    for node in self.nodes:
                        if node == left_node:
                            node_workloads[node] = [
                                RequestInterval(experiment_dur, left_load, read_ratio)
                            ]
                        elif node in center_nodes:
                            node_workloads[node] = [
                                RequestInterval(experiment_dur, center_load, read_ratio)
                            ]
                        else:
                            assert node in right_nodes
                            node_workloads[node] = [
                                RequestInterval(experiment_dur, right_load, read_ratio)
                            ]
                    workloads.append(node_workloads)
        return workloads

    def run(self):
        self.run_func()

    # Show how having different strats per node is optimal
    def run_basic(self):
        print(f"RUNNING EXPERIMENT: Mixed Strats Basic - {self.cluster_type}")
        base_workload = self._mixed_strats_workload()[0]
        self._update_workload(base_workload)
        self.cluster.run(self.experiment_dir / self.cluster_type)

    def run_varied_skew(self):
        print(f"RUNNING EXPERIMENT: Mixed Strats varied skew - {self.cluster_type}")
        relative_eu_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
        workloads = self._mixed_strats_workload(relative_load_range=relative_eu_loads)
        for relative_eu_load, workload in zip(relative_eu_loads, workloads):
            self._update_workload(workload)
            print(f"Iteration: {relative_eu_load=}")
            iteration_dir = self.experiment_dir / f"eu-load-{relative_eu_load}"
            self.cluster.run(iteration_dir / self.cluster_type)

    def run_varied_read_ratio(self):
        print(
            f"RUNNING EXPERIMENT: Mixed Strats varied read ratio - {self.cluster_type}"
        )
        read_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
        workloads = self._mixed_strats_workload(read_ratio_range=read_ratios)
        for read_ratio, workload in zip(read_ratios, workloads):
            self._update_workload(workload)
            print(f"Iteration: {read_ratio=}")
            iteration_dir = self.experiment_dir / f"read-ratio-{read_ratio}"
            self.cluster.run(iteration_dir / self.cluster_type)
