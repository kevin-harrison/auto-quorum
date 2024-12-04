from pathlib import Path

from autoquorum_cluster import AutoQuorumClusterBuilder
from autoquorum_configs import FlexibleQuorum, ReadStrategy, RequestInterval


def round_robin_experiment():
    cluster_size = 3
    experiment_log_dir = Path(f"./logs/round-robin")
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

    for optimize in [True, False]:
        cluster.change_cluster_config(optimize=optimize)
        cluster.start_servers()
        cluster.start_clients()
        cluster.await_cluster()
        iteration_directory = Path.joinpath(experiment_log_dir, f"optimize-{optimize}")
        cluster.get_logs(iteration_directory)
    cluster.shutdown()


def shifting_conditions_experiment(baseline: bool):
    cluster_size = 5
    period_dur = 5
    period1_hotspot = 2
    failed_node = 1
    period3_hotspot = 5
    experiment_dir = Path(f"./logs/shifting-conditions")
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = (
        AutoQuorumClusterBuilder(cluster_name)
        .initial_leader(period1_hotspot)
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

    # Period 1
    print("PERIOD 1:")
    read_ratio = 0.4
    low_requests = [RequestInterval(period_dur, 10, read_ratio)]
    high_requests = [RequestInterval(period_dur, 30, read_ratio)]
    for id in range(1, cluster_size + 1):
        requests = high_requests if id == period1_hotspot else low_requests
        cluster.change_client_config(id, requests=requests)
    if baseline:
        cluster.run(Path.joinpath(experiment_dir, f"period-1/baseline"))
    else:
        cluster.run(Path.joinpath(experiment_dir, f"period-1/autoquorum"))

    # Period 2 (US node faiure)
    print("PERIOD 2:")
    cluster.change_client_config(failed_node, kill_signal_sec=0, next_server=None)
    if baseline:
        cluster.run(Path.joinpath(experiment_dir, f"period-2/baseline"))
    else:
        cluster.change_cluster_config(initial_flexible_quorum=FlexibleQuorum(4, 2))
        cluster.run(Path.joinpath(experiment_dir, f"period-2/autoquorum"))

    # Period 3 (Write shift)
    print("PERIOD 3:")
    cluster.change_client_config(period1_hotspot, requests=low_requests)
    cluster.change_client_config(period3_hotspot, requests=high_requests)
    if baseline:
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
    if baseline:
        cluster.run(Path.joinpath(experiment_dir, f"period-4/baseline"))
    else:
        cluster.change_cluster_config(
            initial_flexible_quorum=FlexibleQuorum(2, 4),
            initial_read_strat=[ReadStrategy.BallotRead] * cluster_size,
        )
        cluster.run(Path.joinpath(experiment_dir, f"period-4/autoquorum"))


def main():
    # round_robin_experiment()
    shifting_conditions_experiment(baseline=False)
    shifting_conditions_experiment(baseline=True)


if __name__ == "__main__":
    main()
