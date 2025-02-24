from pathlib import Path

from autoquorum_cluster import AutoQuorumCluster, AutoQuorumClusterBuilder
from autoquorum_configs import FlexibleQuorum, ReadStrategy, RequestInterval


def round_robin_workload() -> dict[int, list[RequestInterval]]:
    nodes = [1, 2, 3, 4, 5]
    experiment_duration = 60
    read_ratio = 0.0
    total_load = 300
    relative_load = 0.9

    high_load = round(total_load * relative_load)
    low_load = round(total_load * (1 - relative_load) / 4)
    high_requests = RequestInterval(experiment_duration, high_load, read_ratio)
    low_requests = RequestInterval(experiment_duration, low_load, read_ratio)
    workload = {}
    for node in nodes:
        requests = [high_requests if id == node else low_requests for id in nodes]
        workload[node] = requests
    return workload


def round_robin_cluster():
    cluster = (
        AutoQuorumClusterBuilder(1)
        .initial_leader(1)
        .optimize_setting(False)
        .optimize_threshold(0.85)
        .initial_quorum(FlexibleQuorum(4, 2))
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
    return cluster


def round_robin_experiment5():
    experiment_log_dir = Path("./logs/round-robin-5")
    cluster = round_robin_cluster()
    workload = round_robin_workload()
    for id, requests in workload.items():
        cluster.change_client_config(id, requests=requests)

    # Baseline
    iteration_dir = Path.joinpath(experiment_log_dir, "Baseline")
    cluster.run(iteration_dir)

    # AutoQuorum
    cluster.change_cluster_config(optimize=True)
    iteration_dir = Path.joinpath(experiment_log_dir, "AutoQuorum")
    cluster.run(iteration_dir)

    # MultiLeader
    cluster.change_cluster_config(
        optimize=False, multileader=True, initial_flexible_quorum=FlexibleQuorum(3, 3)
    )
    iteration_dir = Path.joinpath(experiment_log_dir, "MultiLeader-Majority")
    cluster.run(iteration_dir)

    # Super Majority
    cluster.change_cluster_config(initial_flexible_quorum=FlexibleQuorum(4, 4))
    iteration_dir = Path.joinpath(experiment_log_dir, "MultiLeader-SuperMajority")
    cluster.run(iteration_dir)

    # cluster.shutdown()


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


def read_strats_workload(
    read_ratio_range=None,
    total_load_range=None,
    relative_load_range=None,
    experiment_dur=20,
    default_read_ratio=0.9,
    default_total_load=300,
    default_relative_load=0.9,
) -> list[dict[int, list[RequestInterval]]]:
    nodes = [1, 2, 3, 4, 5]
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
                for node in nodes:
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


def read_strats_cluster():
    cluster = (
        AutoQuorumClusterBuilder(1)
        .initial_leader(2)
        .optimize_setting(False)
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
    return cluster


def run_read_strats_cluster(cluster: AutoQuorumCluster, experiment_dir: Path):
    bread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.BallotRead)
    qread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.QuorumRead)
    wread_strat = (2, FlexibleQuorum(4, 2), ReadStrategy.ReadAsWrite)
    for leader, quorum, read_strat in [bread_strat, qread_strat, wread_strat]:
        print(f"Read Strat Iteration: {leader=} {quorum=} {read_strat=}")
        cluster.change_cluster_config(
            initial_leader=leader,
            initial_flexible_quorum=quorum,
            initial_read_strat=[read_strat] * 5,
        )
        cluster.run(Path.joinpath(experiment_dir, f"{read_strat.value}"))
    # Run EPaxos
    print("EPaxos iteration")
    cluster.change_cluster_config(
        initial_leader=2,
        initial_flexible_quorum=FlexibleQuorum(4, 4),
        initial_read_strat=[ReadStrategy.ReadAsWrite] * 5,
        multileader=True,
    )
    cluster.run(Path.joinpath(experiment_dir, "EPaxos"))
    # Run AutoQuorum
    print("AutoQuorum iteration")
    cluster.change_cluster_config(
        optimize=True,
        optimize_threshold=0.85,
        initial_leader=2,
        initial_flexible_quorum=FlexibleQuorum(2, 4),
        initial_read_strat=[ReadStrategy.BallotRead] * 5,
        multileader=False,
    )
    cluster.run(Path.joinpath(experiment_dir, "AutoQuorum"))
    cluster.change_cluster_config(optimize=False)


# Show how ballot read is superior to normal quorum read and read as write
def read_strats_benchmark():
    cluster = read_strats_cluster()
    workloads = read_strats_workload()
    assert len(workloads) == 1
    workload = workloads[0]
    for id, requests in workload.items():
        cluster.change_client_config(id, requests=requests)
    experiment_dir = Path("./logs/read-strats")
    run_read_strats_cluster(cluster, experiment_dir)
    # cluster.shutdown()


def read_strats_over_workload_benchmark():
    cluster = read_strats_cluster()
    relative_us_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
    workloads = read_strats_workload(relative_load_range=relative_us_loads)
    for relative_us_load, workload in zip(relative_us_loads, workloads):
        for id, requests in workload.items():
            cluster.change_client_config(id, requests=requests)
        print(f"Iteration: {relative_us_load=}")
        iteration_dir = Path(f"./logs/read-strats/us-load-{relative_us_load}")
        run_read_strats_cluster(cluster, iteration_dir)
    # cluster.shutdown()


def read_strats_over_ratio_benchmark():
    cluster = read_strats_cluster()
    read_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
    read_ratios = [0.9]
    workloads = read_strats_workload(read_ratio_range=read_ratios)
    for read_ratio, workload in zip(read_ratios, workloads):
        for id, requests in workload.items():
            cluster.change_client_config(id, requests=requests)
        print(f"Iteration: {read_ratio=}")
        iteration_dir = Path(f"./logs/read-strats/read-ratio-{read_ratio}")
        run_read_strats_cluster(cluster, iteration_dir)
    # cluster.shutdown()


def read_strats_over_absolute_rate_benchmark():
    cluster = read_strats_cluster()
    total_loads = [30, 90, 180, 210, 300]
    workloads = read_strats_workload(total_load_range=total_loads)
    for total_load, workload in zip(total_loads, workloads):
        for id, requests in workload.items():
            cluster.change_client_config(id, requests=requests)
        print(f"Iteration: {total_load=}")
        iteration_dir = Path(f"./logs/read-strats/total-load-{total_load}")
        run_read_strats_cluster(cluster, iteration_dir)
    # cluster.shutdown()


def mixed_strats_workload(
    read_ratio_range=None,
    total_load_range=None,
    relative_load_range=None,
    experiment_dur=20,
    default_read_ratio=0.5,
    default_total_load=150,
    default_relative_load=0.9,
) -> list[dict[int, list[RequestInterval]]]:
    nodes = [1, 2, 3, 4, 5]
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
                for node in nodes:
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


def mixed_strats_cluster():
    cluster = (
        AutoQuorumClusterBuilder(1)
        .initial_leader(1)
        .initial_quorum(FlexibleQuorum(4, 2))
        .optimize_setting(False)
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
    return cluster


def run_mixed_strats_cluster(cluster: AutoQuorumCluster, experiment_dir: Path):
    mixed_strat = (
        "Mixed",
        (2 * [ReadStrategy.ReadAsWrite]) + (3 * [ReadStrategy.BallotRead]),
    )
    bread_strat = ("BallotRead", [ReadStrategy.BallotRead] * 5)
    wread_strat = ("ReadAsWrite", [ReadStrategy.ReadAsWrite] * 5)
    for name, read_strat in [mixed_strat, bread_strat, wread_strat]:
        print(f"Read Strat Iteration: {name}")
        cluster.change_cluster_config(initial_read_strat=read_strat)
        cluster.run(Path.joinpath(experiment_dir, name))
    # Run EPaxos
    print("EPaxos iteration")
    cluster.change_cluster_config(
        initial_flexible_quorum=FlexibleQuorum(4, 4),
        initial_read_strat=[ReadStrategy.ReadAsWrite] * 5,
        multileader=True,
    )
    cluster.run(Path.joinpath(experiment_dir, "EPaxos"))
    # Run AutoQuorum
    print("AutoQuorum iteration")
    cluster.change_cluster_config(
        optimize=True,
        optimize_threshold=0.85,
        initial_flexible_quorum=FlexibleQuorum(4, 2),
        initial_read_strat=mixed_strat[1],
        multileader=False,
    )
    cluster.run(Path.joinpath(experiment_dir, "AutoQuorum"))
    cluster.change_cluster_config(optimize=False)


# Show how having different strats per node is optimal
def mixed_strats_benchmark():
    experiment_dir = Path("./logs/mixed-strats")
    cluster = mixed_strats_cluster()
    workload = mixed_strats_workload()[0]
    for id, requests in workload.items():
        cluster.change_client_config(id, requests=requests)
    run_mixed_strats_cluster(cluster, experiment_dir)
    # cluster.shutdown()


def mixed_strats_over_workload_benchmark():
    cluster = mixed_strats_cluster()
    relative_eu_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
    workloads = mixed_strats_workload(relative_load_range=relative_eu_loads)
    for relative_eu_load, workload in zip(relative_eu_loads, workloads):
        for id, requests in workload.items():
            cluster.change_client_config(id, requests=requests)
        print(f"Iteration: {relative_eu_load=}")
        iteration_dir = Path(f"./logs/mixed-strats/eu-load-{relative_eu_load}")
        run_mixed_strats_cluster(cluster, iteration_dir)
    # cluster.shutdown()


def mixed_strats_over_ratio_benchmark():
    cluster = mixed_strats_cluster()
    read_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
    workloads = mixed_strats_workload(read_ratio_range=read_ratios)
    for read_ratio, workload in zip(read_ratios, workloads):
        for id, requests in workload.items():
            cluster.change_client_config(id, requests=requests)
        print(f"Iteration: {read_ratio=}")
        iteration_dir = Path(f"./logs/mixed-strats/read-ratio-{read_ratio}")
        run_mixed_strats_cluster(cluster, iteration_dir)
    # cluster.shutdown()


def even_load_cluster():
    cluster = (
        AutoQuorumClusterBuilder(1)
        .initial_leader(3)
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
    return cluster


def even_load_workload():
    workload = {}
    requests = [RequestInterval(duration_sec=10, requests_per_sec=100, read_ratio=0)]
    for node in [1, 2, 3, 4, 5]:
        workload[node] = requests
    return workload


# Show how having different strats per node is optimal
def even_load_benchmark():
    experiment_dir = Path("./logs/even-load")
    cluster = even_load_cluster()
    workload = even_load_workload()
    for id, requests in workload.items():
        cluster.change_client_config(id, requests=requests)

    # AutoQuorum Run
    cluster.change_cluster_config(
        initial_leader=3,
        initial_flexible_quorum=FlexibleQuorum(4, 2),
        optimize=True,
        optimize_threshold=0.85,
    )
    iteration_dir = Path(experiment_dir, "AutoQuorum")
    cluster.run(iteration_dir)

    # SuperMajority Run
    cluster.change_cluster_config(
        multileader=True,
        initial_flexible_quorum=FlexibleQuorum(4, 4),
        optimize=False,
    )
    iteration_dir = Path(experiment_dir, "MultiLeader-SuperMajority")
    cluster.run(iteration_dir)

    # Majority Run
    cluster.change_cluster_config(
        multileader=True,
        initial_flexible_quorum=FlexibleQuorum(3, 3),
        optimize=False,
    )
    iteration_dir = Path(experiment_dir, "MultiLeader-Majority")
    cluster.run(iteration_dir)

    # cluster.shutdown()


def main():
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
