from os import name
from pathlib import Path

from clusters.autoquorum_cluster import AutoQuorumCluster, AutoQuorumClusterBuilder
from clusters.autoquorum_configs import FlexibleQuorum, ReadStrategy, RequestInterval
from clusters.etcd_cluster import EtcdCluster, EtcdClusterBuilder
from clusters.multileader_cluster import MultiLeaderCluster, MultiLeaderClusterBuilder


class RoundRobinExperiment:
    def __init__(self, experiment_dir: Path = Path("./logs/round-robin-5")) -> None:
        self.experiment_dir = experiment_dir
        self.workload = self.round_robin_workload()
        self.initial_leader = 1
        self.cluster_id = 1

    def round_robin_workload(self) -> dict[int, tuple[str, list[RequestInterval]]]:
        nodes = {
            1: "us-west2-a",
            2: "us-south1-a",
            3: "us-east4-a",
            4: "europe-southwest1-a",
            5: "europe-west4-a",
        }
        experiment_duration = 2
        read_ratio = 0.0
        total_load = 300
        relative_load = 0.9

        high_load = round(total_load * relative_load)
        low_load = round(total_load * (1 - relative_load) / 4)
        high_requests = RequestInterval(experiment_duration, high_load, read_ratio)
        low_requests = RequestInterval(experiment_duration, low_load, read_ratio)
        workload = {}
        for node, zone in nodes.items():
            requests = [high_requests if id == node else low_requests for id in nodes]
            workload[node] = (zone, requests)
        return workload

    def add_workload(self, cluster):
        for id, (zone, requests) in self.workload.items():
            cluster = cluster.server(id, zone)
            cluster = cluster.client(id, zone, requests=requests)
        return cluster

    def autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            AutoQuorumClusterBuilder(self.cluster_id)
            .initial_leader(self.initial_leader)
            .optimize_setting(True)
            .optimize_threshold(0.85)
            .initial_quorum(FlexibleQuorum(4, 2))
        )
        return self.add_workload(cluster).build()

    def multileader_cluster(self) -> MultiLeaderCluster:
        cluster = MultiLeaderClusterBuilder(self.cluster_id).flex_quorum(
            FlexibleQuorum(4, 4)
        )
        return self.add_workload(cluster).build()

    def etcd_cluster(self) -> EtcdCluster:
        cluster = EtcdClusterBuilder(self.cluster_id).initial_leader(
            self.initial_leader
        )
        return self.add_workload(cluster).build()

    def run(self, destroy_instances: bool = False):
        print("RUNNING EXPERIMENT: Round Robin")
        # print("EXPERIMENT ITERATION: AutoQuorum")
        # cluster = self.autoquorum_cluster()
        # cluster.run(self.experiment_dir / "AutoQuorum")
        #
        # print("EXPERIMENT ITERATION: Baseline")
        # cluster.change_cluster_config(optimize=False)
        # cluster.run(self.experiment_dir / "Baseline")
        #
        # print("EXPERIMENT ITERATION: MultiLeader")
        # cluster = self.multileader_cluster()
        # cluster.run(self.experiment_dir / "MultiLeader-SuperMajority")
        # # cluster.change_cluster_config(initial_flexible_quorum=FlexibleQuorum(3, 3))
        # # cluster.run(self.experiment_dir / "MultiLeader-Majority")

        print("EXPERIMENT ITERATION: Etcd")
        cluster = self.etcd_cluster()
        cluster.run(self.experiment_dir / "Etcd")

        if destroy_instances:
            cluster.shutdown()


# class ShiftingConditionsExperiment:
#     def __init__(
#         self, experiment_dir: Path = Path("./logs/shifting-conditions")
#     ) -> None:
#         self.experiment_dir = experiment_dir
#
#     def autoquorum_cluster(self):
#         cluster = (
#             AutoQuorumClusterBuilder(1)
#             .initial_leader(2)
#             .optimize_setting(False)
#             .server(1, "us-west2-a")
#             .server(2, "us-south1-a")
#             .server(3, "us-east4-a")
#             .server(4, "europe-southwest1-a")
#             .server(5, "europe-west4-a")
#             .client(1, "us-west2-a")
#             .client(2, "us-south1-a")
#             .client(3, "us-east4-a")
#             .client(4, "europe-southwest1-a")
#             .client(5, "europe-west4-a")
#         ).build()
#


def shifting_conditions_experiment(cluster_type: str):
    assert cluster_type in [
        "baseline",
        "autoquorum",
        "multileader-majority",
        "multileader-supermajority",
    ]
    if cluster_type in ["baseline", "autoquorum"]:
        multileader = False
    else:
        multileader = True
    optimize = cluster_type == "autoquorum"

    cluster_size = 5
    period_dur = 10
    period1_hotspot = 2
    failed_node = 1
    period3_hotspot = 5
    experiment_dir = Path("./logs/shifting-conditions")
    cluster = (
        AutoQuorumClusterBuilder(1)
        .initial_leader(period1_hotspot)
        .optimize_setting(False)
        .multileader(multileader)
        .server(1, "us-west2-a")
        .server(2, "us-south1-a")
        .server(3, "us-east4-a")
        .server(4, "europe-southwest1-a")
        .server(5, "europe-west4-a")
        .client(1, "us-west2-a")
        .client(2, "us-south1-a")
        .client(3, "us-east4-a")
        .client(4, "europe-southwest1-a")
        .client(5, "europe-west4-a")
    ).build()

    if cluster_type == "multileader-supermajority":
        cluster.change_cluster_config(initial_flexible_quorum=FlexibleQuorum(4, 4))

    # Period 1
    print("PERIOD 1:")
    read_ratio = 0.4
    low_requests = [RequestInterval(period_dur, 10, read_ratio)]
    high_requests = [RequestInterval(period_dur, 30, read_ratio)]
    for id in range(1, cluster_size + 1):
        requests = high_requests if id == period1_hotspot else low_requests
        cluster.change_client_config(id, requests=requests)
    cluster.run(Path.joinpath(experiment_dir, f"period-1/{cluster_type}"))

    # Period 2 (US node faiure)
    print("PERIOD 2:")
    cluster.change_client_config(failed_node, kill_signal_sec=0)
    if optimize:
        cluster.change_cluster_config(initial_flexible_quorum=FlexibleQuorum(4, 2))
    cluster.run(Path.joinpath(experiment_dir, f"period-2/{cluster_type}"))

    # Period 3 (Write shift)
    print("PERIOD 3:")
    cluster.change_client_config(period1_hotspot, requests=low_requests)
    cluster.change_client_config(period3_hotspot, requests=high_requests)
    if optimize:
        cluster.change_cluster_config(initial_leader=period3_hotspot)
    cluster.run(Path.joinpath(experiment_dir, f"period-3/{cluster_type}"))

    # Period 4 (Read dominant)
    print("PERIOD 4:")
    read_ratio = 0.98
    low_requests = [RequestInterval(period_dur, 10, read_ratio)]
    high_requests = [RequestInterval(period_dur, 30, read_ratio)]
    for id in range(1, cluster_size + 1):
        # requests = high_requests if id == period3_hotspot else low_requests
        cluster.change_client_config(id, requests=high_requests)
    if optimize:
        cluster.change_cluster_config(
            initial_flexible_quorum=FlexibleQuorum(2, 4),
            initial_read_strat=[ReadStrategy.BallotRead] * cluster_size,
        )
    cluster.run(Path.joinpath(experiment_dir, f"period-4/{cluster_type}"))
    # cluster.shutdown()


class ReadStratsExperiment:
    def __init__(self, experiment_dir: Path = Path("./logs/read-strats")) -> None:
        self.experiment_dir = experiment_dir
        self.base_workload = self.read_strats_workload()[0]
        self.aq_cluster = self.autoquorum_cluster()
        self.ml_cluster = self.multileader_cluster()
        self.et_cluster = self.etcd_cluster()

    def read_strats_workload(
        self,
        read_ratio_range=None,
        total_load_range=None,
        relative_load_range=None,
        experiment_dur=20,
        default_read_ratio=0.9,
        default_total_load=300,
        default_relative_load=0.9,
    ) -> list[dict[int, tuple[str, list[RequestInterval]]]]:
        nodes = {
            1: "us-west2-a",
            2: "us-south1-a",
            3: "us-east4-a",
            4: "europe-southwest1-a",
            5: "europe-west4-a",
        }
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
                    for node, zone in nodes.items():
                        if node in us_nodes:
                            requests = [
                                RequestInterval(experiment_dur, us_load, read_ratio)
                            ]
                            node_workloads[node] = (zone, requests)
                        else:
                            requests = [
                                RequestInterval(experiment_dur, eu_load, read_ratio)
                            ]
                            node_workloads[node] = (zone, requests)
                    workloads.append(node_workloads)
        return workloads

    def add_base_workload(self, cluster):
        for id, (zone, requests) in self.base_workload.items():
            cluster = cluster.server(id, zone)
            cluster = cluster.client(id, zone, requests=requests)
        return cluster

    def autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = AutoQuorumClusterBuilder(1).initial_leader(2)
        return self.add_base_workload(cluster).build()

    def multileader_cluster(self) -> MultiLeaderCluster:
        cluster = MultiLeaderClusterBuilder(1).flex_quorum(FlexibleQuorum(4, 4))
        return self.add_base_workload(cluster).build()

    def etcd_cluster(self) -> EtcdCluster:
        cluster = EtcdClusterBuilder(1).initial_leader(2)
        return self.add_base_workload(cluster).build()

    def run_cluster(self, iteration_dir: Path):
        print("EXPERIMENT ITERATION: AutoQuorum")
        self.aq_cluster.change_cluster_config(
            optimize=True,
            optimize_threshold=0.85,
            initial_leader=2,
            initial_flexible_quorum=FlexibleQuorum(2, 4),
            initial_read_strat=[ReadStrategy.BallotRead] * 5,
        )
        self.aq_cluster.run(iteration_dir / "AutoQuorum")

        print("EXERIMENT ITERATION: Fixed read strategies")
        bread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.BallotRead)
        qread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.QuorumRead)
        wread_strat = (2, FlexibleQuorum(4, 2), ReadStrategy.ReadAsWrite)
        for leader, quorum, read_strat in [bread_strat, qread_strat, wread_strat]:
            print(f"Read Strat Iteration: {leader=} {quorum=} {read_strat=}")
            self.aq_cluster.change_cluster_config(
                optimize=False,
                initial_leader=leader,
                initial_flexible_quorum=quorum,
                initial_read_strat=[read_strat] * 5,
            )
            self.aq_cluster.run(iteration_dir / f"{read_strat.value}")

        print("EXPERIMENT ITERATION: EPaxos")
        self.ml_cluster.run(iteration_dir / "EPaxos")

        print("EXPERIMENT ITERATION: Etcd")
        self.et_cluster.run(iteration_dir / "Etcd")

    # Show how ballot read is superior to normal quorum read and read as write
    def run_basic(self, destroy_instances: bool = False):
        self.run_cluster(self.experiment_dir)
        if destroy_instances:
            self.aq_cluster.shutdown()

    def run_varied_skew(self, destroy_instances: bool = False):
        relative_us_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
        workloads = self.read_strats_workload(relative_load_range=relative_us_loads)
        for relative_us_load, workload in zip(relative_us_loads, workloads):
            for id, (_, requests) in workload.items():
                self.aq_cluster.change_client_config(id, requests=requests)
                self.ml_cluster.change_client_config(id, requests=requests)
                self.et_cluster.change_client_config(id, requests=requests)
            print(f"Iteration: {relative_us_load=}")
            iteration_dir = self.experiment_dir / f"us-load-{relative_us_load}"
            self.run_cluster(iteration_dir)
        if destroy_instances:
            self.aq_cluster.shutdown()

    def run_varied_read_ratio(self, destroy_instances: bool = False):
        read_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
        workloads = self.read_strats_workload(read_ratio_range=read_ratios)
        for read_ratio, workload in zip(read_ratios, workloads):
            for id, (_, requests) in workload.items():
                self.aq_cluster.change_client_config(id, requests=requests)
                self.ml_cluster.change_client_config(id, requests=requests)
                self.et_cluster.change_client_config(id, requests=requests)
            print(f"Iteration: {read_ratio=}")
            iteration_dir = self.experiment_dir / f"read-ratio-{read_ratio}"
            self.run_cluster(iteration_dir)
        if destroy_instances:
            self.aq_cluster.shutdown()

    def run_varied_absolute_rate(self, destroy_instances: bool = False):
        total_loads = [30, 90, 180, 210, 300]
        workloads = self.read_strats_workload(total_load_range=total_loads)
        for total_load, workload in zip(total_loads, workloads):
            for id, (_, requests) in workload.items():
                self.aq_cluster.change_client_config(id, requests=requests)
                self.ml_cluster.change_client_config(id, requests=requests)
                self.et_cluster.change_client_config(id, requests=requests)
            print(f"Iteration: {total_load=}")
            iteration_dir = self.experiment_dir / f"total-load-{total_load}"
            self.run_cluster(iteration_dir)
        if destroy_instances:
            self.aq_cluster.shutdown()


class MixedStratsExperiment:
    def __init__(self, experiment_dir: Path = Path("./logs/mixed-strats")) -> None:
        self.experiment_dir = experiment_dir
        self.base_workload = self.mixed_strats_workload()[0]
        self.aq_cluster = self.autoquorum_cluster()
        self.ml_cluster = self.multileader_cluster()
        self.et_cluster = self.etcd_cluster()

    def mixed_strats_workload(
        self,
        read_ratio_range=None,
        total_load_range=None,
        relative_load_range=None,
        experiment_dur=20,
        default_read_ratio=0.5,
        default_total_load=150,
        default_relative_load=0.9,
    ) -> list[dict[int, tuple[str, list[RequestInterval]]]]:
        nodes = {
            1: "us-west2-a",
            2: "us-south1-a",
            3: "us-east4-a",
            4: "europe-southwest1-a",
            5: "europe-west4-a",
        }
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
                    for node, zone in nodes.items():
                        if node == left_node:
                            requests = [
                                RequestInterval(experiment_dur, left_load, read_ratio)
                            ]
                            node_workloads[node] = (zone, requests)
                        elif node in center_nodes:
                            requests = [
                                RequestInterval(experiment_dur, center_load, read_ratio)
                            ]
                            node_workloads[node] = (zone, requests)
                        else:
                            assert node in right_nodes
                            requests = [
                                RequestInterval(experiment_dur, right_load, read_ratio)
                            ]
                            node_workloads[node] = (zone, requests)
                    workloads.append(node_workloads)
        return workloads

    def add_base_workload(self, cluster):
        for id, (zone, requests) in self.base_workload.items():
            cluster = cluster.server(id, zone)
            cluster = cluster.client(id, zone, requests=requests)
        return cluster

    def autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            AutoQuorumClusterBuilder(1)
            .initial_leader(1)
            .initial_quorum(FlexibleQuorum(4, 2))
            .optimize_setting(False)
        )
        return self.add_base_workload(cluster).build()

    def multileader_cluster(self) -> MultiLeaderCluster:
        cluster = MultiLeaderClusterBuilder(1).flex_quorum(FlexibleQuorum(4, 4))
        return self.add_base_workload(cluster).build()

    def etcd_cluster(self) -> EtcdCluster:
        cluster = EtcdClusterBuilder(1).initial_leader(1)
        return self.add_base_workload(cluster).build()

    def run_cluster(self, iteration_dir: Path):
        print("EXERIMENT ITERATION: Fixed read strategies")
        mixed_strat = (
            "Mixed",
            (2 * [ReadStrategy.ReadAsWrite]) + (3 * [ReadStrategy.BallotRead]),
        )
        bread_strat = ("BallotRead", [ReadStrategy.BallotRead] * 5)
        wread_strat = ("ReadAsWrite", [ReadStrategy.ReadAsWrite] * 5)
        for name, read_strat in [mixed_strat, bread_strat, wread_strat]:
            print(f"Read Strat Iteration: {name}")
            self.aq_cluster.change_cluster_config(
                optimize=False,
                initial_read_strat=read_strat,
                initial_flexible_quorum=FlexibleQuorum(4, 2),
            )
            self.aq_cluster.run(iteration_dir / name)

        print("EXPERIMENT ITERATION: AutoQuorum")
        self.aq_cluster.change_cluster_config(
            optimize=True,
            optimize_threshold=0.85,
            initial_flexible_quorum=FlexibleQuorum(4, 2),
            initial_read_strat=mixed_strat[1],
        )
        self.aq_cluster.run(iteration_dir / "AutoQuorum")

        print("EXPERIMENT ITERATION: EPaxos")
        self.ml_cluster.run(iteration_dir / "EPaxos")

        print("EXPERIMENT ITERATION: Etcd")
        self.et_cluster.run(iteration_dir / "Etcd")

    # Show how having different strats per node is optimal
    def run_basic(self, destroy_instances: bool = False):
        self.run_cluster(self.experiment_dir)
        if destroy_instances:
            self.aq_cluster.shutdown()

    def run_varied_skew(self, destroy_instances: bool = False):
        relative_eu_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
        workloads = self.mixed_strats_workload(relative_load_range=relative_eu_loads)
        for relative_eu_load, workload in zip(relative_eu_loads, workloads):
            for id, (_, requests) in workload.items():
                self.aq_cluster.change_client_config(id, requests=requests)
                self.ml_cluster.change_client_config(id, requests=requests)
                self.et_cluster.change_client_config(id, requests=requests)
            print(f"Iteration: {relative_eu_load=}")
            iteration_dir = self.experiment_dir / f"eu-load-{relative_eu_load}"
            self.run_cluster(iteration_dir)
        if destroy_instances:
            self.aq_cluster.shutdown()

    def run_varied_read_ratio(self, destroy_instances: bool = False):
        read_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
        workloads = self.mixed_strats_workload(read_ratio_range=read_ratios)
        for read_ratio, workload in zip(read_ratios, workloads):
            for id, (_, requests) in workload.items():
                self.aq_cluster.change_client_config(id, requests=requests)
                self.ml_cluster.change_client_config(id, requests=requests)
                self.et_cluster.change_client_config(id, requests=requests)
            print(f"Iteration: {read_ratio=}")
            iteration_dir = self.experiment_dir / f"read-ratio-{read_ratio}"
            self.run_cluster(iteration_dir)
        if destroy_instances:
            self.aq_cluster.shutdown()


class EvenLoadExperiment:
    def __init__(self, experiment_dir: Path = Path("./logs/even-load")) -> None:
        self.experiment_dir = experiment_dir
        self.workload = self.even_load_workload()
        self.initial_leader = 3
        self.cluster_id = 1

    def even_load_workload(self):
        nodes = {
            1: "us-west2-a",
            2: "us-south1-a",
            3: "us-east4-a",
            4: "europe-southwest1-a",
            5: "europe-west4-a",
        }
        workload = {}
        requests = [
            RequestInterval(duration_sec=10, requests_per_sec=100, read_ratio=0)
        ]
        for node, zone in nodes.items():
            workload[node] = (zone, requests)
        return workload

    def add_workload(self, cluster):
        for id, (zone, requests) in self.workload.items():
            cluster = cluster.server(id, zone)
            cluster = cluster.client(id, zone, requests=requests)
        return cluster

    def autoquorum_cluster(self) -> AutoQuorumCluster:
        cluster = (
            AutoQuorumClusterBuilder(self.cluster_id)
            .initial_leader(self.initial_leader)
            .initial_quorum(FlexibleQuorum(4, 2))
            .optimize_setting(True)
            .optimize_threshold(0.85)
        )
        return self.add_workload(cluster).build()

    def multileader_cluster(self) -> MultiLeaderCluster:
        cluster = MultiLeaderClusterBuilder(self.cluster_id).flex_quorum(
            FlexibleQuorum(4, 4)
        )
        return self.add_workload(cluster).build()

    def etcd_cluster(self) -> EtcdCluster:
        cluster = EtcdClusterBuilder(self.cluster_id).initial_leader(
            self.initial_leader
        )
        return self.add_workload(cluster).build()

    def run(self, destroy_instances: bool = False):
        print("RUNNING EXPERIMENT: Even Load")
        print("EXPERIMENT ITERATION: AutoQuorum")
        cluster = self.autoquorum_cluster()
        cluster.run(self.experiment_dir / "AutoQuorum")

        print("EXPERIMENT ITERATION: MultiLeader Super Majority")
        cluster = self.multileader_cluster()
        cluster.run(self.experiment_dir / "MultiLeader-SuperMajority")

        print("EXPERIMENT ITERATION: MultiLeader Majority")
        cluster.change_cluster_config(initial_flexible_quorum=FlexibleQuorum(3, 3))
        cluster.run(self.experiment_dir / "MultiLeader-Majority")

        print("EXPERIMENT ITERATION: Etcd")
        cluster = self.etcd_cluster()
        cluster.run(self.experiment_dir / "Etcd")

        if destroy_instances:
            cluster.shutdown()


def main():
    RoundRobinExperiment(experiment_dir=Path("./logs/TEST-round-robin-5")).run()
    # round_robin_experiment5()

    # shifting_conditions_experiment("baseline")
    # shifting_conditions_experiment("autoquorum")
    # shifting_conditions_experiment("multileader-majority")
    # shifting_conditions_experiment("multileader-supermajority")

    # read_strats_benchmark()
    # read_strats_over_workload_benchmark()
    # read_strats_over_ratio_benchmark()
    # read_strats_over_absolute_rate_benchmark()

    # mixed_strats_benchmark()
    # mixed_strats_over_workload_benchmark()
    # mixed_strats_over_ratio_benchmark()

    # even_load_benchmark()
    pass


if __name__ == "__main__":
    main()
