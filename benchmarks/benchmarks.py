from pathlib import Path

from autoquorum_cluster import AutoQuorumClusterBuilder
from autoquorum_configs import FlexibleQuorum, ReadStrategy, RequestInterval


def round_robin_experiment():
    cluster_size = 3
    experiment_log_dir = Path(f"./logs/round-robin-w-multileader")
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = (
        AutoQuorumClusterBuilder(cluster_name)
        .initial_leader(2)
        .optimize_setting(True)
        .optimize_threshold(0.85)
        .server(1, "europe-west2-a")
        .client(1, "europe-west2-a")
        .server(2, "europe-southwest1-a")
        .client(2, "europe-southwest1-a")
        .server(3, "europe-west10-a")
        .client(3, "europe-west10-a")
    ).build()

    # Shifting workload
    r = 0.0  # read ratio
    cluster.change_client_config(
        2,
        requests=[RequestInterval(10, 10, r), RequestInterval(20, 2, r)],
    )
    cluster.change_client_config(
        1,
        requests=[
            RequestInterval(10, 2, r),
            RequestInterval(10, 10, r),
            RequestInterval(10, 2, r),
        ],
    )
    cluster.change_client_config(
        3,
        requests=[RequestInterval(20, 2, r), RequestInterval(10, 10, r)],
    )

    # Omnipaxos runs
    for optimize in [True, False]:
        cluster.change_cluster_config(optimize=optimize)
        cluster.start_servers()
        cluster.start_clients()
        cluster.await_cluster()
        iteration_directory = Path.joinpath(
            experiment_log_dir, f"{'autoquorum' if optimize else 'baseline'}"
        )
        cluster.get_logs(iteration_directory)

    # Multileader run
    cluster.change_cluster_config(multileader=True)
    cluster.start_servers()
    cluster.start_clients()
    cluster.await_cluster()
    iteration_directory = Path.joinpath(experiment_log_dir, "multileader")
    cluster.get_logs(iteration_directory)

    cluster.shutdown()


def shifting_conditions_experiment(baseline: bool, multileader: bool = False):
    cluster_size = 5
    period_dur = 10
    period1_hotspot = 2
    failed_node = 1
    period3_hotspot = 5
    experiment_dir = Path(f"./logs/shifting-conditions")
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = (
        AutoQuorumClusterBuilder(cluster_name)
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

    # Period 1
    print("PERIOD 1:")
    read_ratio = 0.4
    low_requests = [RequestInterval(period_dur, 10, read_ratio)]
    high_requests = [RequestInterval(period_dur, 30, read_ratio)]
    for id in range(1, cluster_size + 1):
        requests = high_requests if id == period1_hotspot else low_requests
        cluster.change_client_config(id, requests=requests)
    if multileader:
        cluster.run(Path.joinpath(experiment_dir, f"period-1/multileader"))
    elif baseline:
        cluster.run(Path.joinpath(experiment_dir, f"period-1/baseline"))
    else:
        cluster.run(Path.joinpath(experiment_dir, f"period-1/autoquorum"))

    # Period 2 (US node faiure)
    print("PERIOD 2:")
    cluster.change_client_config(failed_node, kill_signal_sec=0, next_server=None)
    if multileader:
        cluster.run(Path.joinpath(experiment_dir, f"period-2/multileader"))
    elif baseline:
        cluster.run(Path.joinpath(experiment_dir, f"period-2/baseline"))
    else:
        cluster.change_cluster_config(initial_flexible_quorum=FlexibleQuorum(4, 2))
        cluster.run(Path.joinpath(experiment_dir, f"period-2/autoquorum"))

    # Period 3 (Write shift)
    print("PERIOD 3:")
    cluster.change_client_config(period1_hotspot, requests=low_requests)
    cluster.change_client_config(period3_hotspot, requests=high_requests)
    if multileader:
        cluster.run(Path.joinpath(experiment_dir, f"period-3/multileader"))
    elif baseline:
        cluster.run(Path.joinpath(experiment_dir, f"period-3/baseline"))
    else:
        cluster.change_cluster_config(initial_leader=period3_hotspot)
        cluster.run(Path.joinpath(experiment_dir, f"period-3/autoquorum"))

    # Period 4 (Read dominant)
    print("PERIOD 4:")
    read_ratio = 0.95
    low_requests = [RequestInterval(period_dur, 10, read_ratio)]
    high_requests = [RequestInterval(period_dur, 30, read_ratio)]
    for id in range(1, cluster_size + 1):
        # requests = high_requests if id == period3_hotspot else low_requests
        cluster.change_client_config(id, requests=low_requests)
    if multileader:
        cluster.run(Path.joinpath(experiment_dir, f"period-4/multileader"))
    elif baseline:
        cluster.run(Path.joinpath(experiment_dir, f"period-4/baseline"))
    else:
        cluster.change_cluster_config(
            initial_flexible_quorum=FlexibleQuorum(2, 4),
            initial_read_strat=[ReadStrategy.BallotRead] * cluster_size,
        )
        cluster.run(Path.joinpath(experiment_dir, f"period-4/autoquorum"))
    # cluster.shutdown()


# Show how ballot read is superior to normal quorum read and read as write
def read_strats_benchmark():
    experiment_dir = Path(f"./logs/read-strats-test")
    experiment_dur = 20

    # Define cluster
    cluster = (
        AutoQuorumClusterBuilder("cluster-5-1")
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

    # Define workload
    read_ratio = 0.9
    us_nodes = [1, 2, 3]
    low_requests = [RequestInterval(experiment_dur, 10, read_ratio)]
    high_requests = [RequestInterval(experiment_dur, 90, read_ratio)]
    for id in cluster.get_nodes():
        requests = high_requests if id in us_nodes else low_requests
        cluster.change_client_config(id, requests=requests)

    # Run with different cluster strategies
    bread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.BallotRead)
    qread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.QuorumRead)
    wread_strat = (3, FlexibleQuorum(4, 2), ReadStrategy.ReadAsWrite)
    for leader, quorum, read_strat in [bread_strat, qread_strat, wread_strat]:
        print(f"Iteration: {leader=} {quorum=} {read_strat=}")
        cluster.change_cluster_config(
            initial_leader=leader,
            initial_flexible_quorum=quorum,
            initial_read_strat=[read_strat] * 5,
        )
        cluster.run(Path.joinpath(experiment_dir, f"{read_strat.value}"))
    # cluster.shutdown()

def read_strats_over_workload_benchmark():
    experiment_dir = Path(f"./logs/read-strats")
    experiment_dur = 20

    # Define cluster
    cluster = (
        AutoQuorumClusterBuilder("cluster-5-1")
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

    # Define workload
    read_ratio = 0.9
    us_nodes = [1, 2, 3]
    low_requests = [RequestInterval(experiment_dur, 10, read_ratio)]
    for high in [10, 30, 50, 70, 90]:
        high_requests = [RequestInterval(experiment_dur, high, read_ratio)]
        for id in cluster.get_nodes():
            requests = high_requests if id in us_nodes else low_requests
            cluster.change_client_config(id, requests=requests)

        # Run with different cluster strategies
        bread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.BallotRead)
        qread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.QuorumRead)
        wread_strat = (3, FlexibleQuorum(4, 2), ReadStrategy.ReadAsWrite)
        strats = [bread_strat, qread_strat, wread_strat]
        for leader, quorum, read_strat in strats:
            print(f"Iteration: {leader=} {quorum=} {read_strat=}")
            cluster.change_cluster_config(
                initial_leader=leader,
                initial_flexible_quorum=quorum,
                initial_read_strat=[read_strat] * 5,
            )
            cluster.run(Path.joinpath(experiment_dir, f"hotspot-{high}/{read_strat.value}"))
    # cluster.shutdown()

def read_strats_over_ratio_benchmark():
    experiment_dir = Path(f"./logs/read-strats")
    experiment_dur = 20

    # Define cluster
    cluster = (
        AutoQuorumClusterBuilder("cluster-5-1")
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

    # Define workload
    us_nodes = [1, 2, 3]
    for read_ratio in [0.1, 0.3, 0.5, 0.7, 0.9]:
        low_requests = [RequestInterval(experiment_dur, 10, read_ratio)]
        high_requests = [RequestInterval(experiment_dur, 90, read_ratio)]
        for id in cluster.get_nodes():
            requests = high_requests if id in us_nodes else low_requests
            cluster.change_client_config(id, requests=requests)

        # Run with different cluster strategies
        bread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.BallotRead)
        qread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.QuorumRead)
        wread_strat = (3, FlexibleQuorum(4, 2), ReadStrategy.ReadAsWrite)
        strats = [bread_strat, qread_strat, wread_strat]
        for leader, quorum, read_strat in strats:
            print(f"Iteration: {leader=} {quorum=} {read_strat=}")
            cluster.change_cluster_config(
                initial_leader=leader,
                initial_flexible_quorum=quorum,
                initial_read_strat=[read_strat] * 5,
            )
            cluster.run(Path.joinpath(experiment_dir, f"ratio-{read_ratio}/{read_strat.value}"))
    # cluster.shutdown()


def read_strats_over_absolute_rate_benchmark():
    experiment_dir = Path(f"./logs/read-strats")
    experiment_dur = 20

    # Define cluster
    cluster = (
        AutoQuorumClusterBuilder("cluster-5-1")
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

    # Define workload
    read_ratio = 0.9
    us_nodes = [1, 2, 3]
    for rate in [1, 3, 5, 7, 9]:
        low_requests = [RequestInterval(experiment_dur, rate, read_ratio)]
        high_requests = [RequestInterval(experiment_dur, rate*9, read_ratio)]
        for id in cluster.get_nodes():
            requests = high_requests if id in us_nodes else low_requests
            cluster.change_client_config(id, requests=requests)

        # Run with different cluster strategies
        bread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.BallotRead)
        qread_strat = (2, FlexibleQuorum(2, 4), ReadStrategy.QuorumRead)
        wread_strat = (3, FlexibleQuorum(4, 2), ReadStrategy.ReadAsWrite)
        strats = [bread_strat, qread_strat, wread_strat]
        for leader, quorum, read_strat in strats:
            print(f"Iteration: {leader=} {quorum=} {read_strat=}")
            cluster.change_cluster_config(
                initial_leader=leader,
                initial_flexible_quorum=quorum,
                initial_read_strat=[read_strat] * 5,
            )
            cluster.run(Path.joinpath(experiment_dir, f"rate-{rate}/{read_strat.value}"))
    # cluster.shutdown()


# Show how having different strats per node is optimal
def mixed_strats_no_rinse_benchmark():
    experiment_dir = Path(f"./logs/read-strats")

    # Define cluster
    cluster = (
        AutoQuorumClusterBuilder("cluster-5-1")
        .initial_leader(1)
        .initial_quorum(FlexibleQuorum(4, 2))
        .optimize_setting(False)
        # For mixed this was on
        # .optimize_setting(True)
        # .optimize_threshold(0.9)
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

    # Define workload
    experiment_dur = 20
    read_ratio = 0.5
    cluster.change_client_config(1, requests=RequestInterval(experiment_dur, 20, read_ratio))
    cluster.change_client_config(2, requests=RequestInterval(experiment_dur,  2, read_ratio))
    cluster.change_client_config(3, requests=RequestInterval(experiment_dur,  2, read_ratio))
    cluster.change_client_config(4, requests=RequestInterval(experiment_dur,  4, read_ratio))
    cluster.change_client_config(5, requests=RequestInterval(experiment_dur,  6, read_ratio))

    # Run with different cluster strategies
    mixed_strat = ("Mixed", (3 * [ReadStrategy.ReadAsWrite]) + (2 * [ReadStrategy.BallotRead]))
    bread_strat = ("BallotRead", [ReadStrategy.BallotRead]*5)
    wread_strat = ("ReadAsWrite", [ReadStrategy.ReadAsWrite]*5)
    for name, read_strat in [mixed_strat, bread_strat, wread_strat]:
        cluster.change_cluster_config(initial_read_strat=read_strat)
        cluster.run(Path.joinpath(experiment_dir, name))
    # cluster.shutdown()


def main():
    # round_robin_experiment()
    #read_strats_benchmark shifting_conditions_experiment(baseline=False)
    # shifting_conditions_experiment(baseline=False)
    # shifting_conditions_experiment(baseline=True)
    shifting_conditions_experiment(baseline=True, multileader=True)
    # read_strats_benchmark()
    # read_strats_over_workload_benchmark()
    # read_strats_over_ratio_benchmark()
    # read_strats_over_absolute_rate_benchmark()


if __name__ == "__main__":
    main()
