from pathlib import Path

from autoquorum_cluster import AutoQuorumClusterBuilder
from autoquorum_configs import RequestInterval


def round_robin_experiment():
    cluster_size = 3
    experiment_log_dir = Path(f"./logs/round-robin2")
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = (
        AutoQuorumClusterBuilder(cluster_name)
        .set_initial_leader(2)
        .set_optimize_setting(True)
        .set_optimize_threshold(0.85)
        .add_server(1, "europe-west2-a")
        .add_client(1, "europe-west2-a")
        .add_server(2, "europe-southwest1-a")
        .add_client(2, "europe-southwest1-a")
        .add_server(3, "europe-west10-a")
        .add_client(3, "europe-west10-a")
    ).build()

    r = 0.0  # read ratio
    cluster.change_client_config(
        2,
        request_rate_intervals=[RequestInterval(10, 10, r), RequestInterval(20, 2, r)],
    )
    cluster.change_client_config(
        1,
        request_rate_intervals=[
            RequestInterval(10, 2, r),
            RequestInterval(10, 10, r),
            RequestInterval(10, 2, r),
        ],
    )
    cluster.change_client_config(
        3,
        request_rate_intervals=[RequestInterval(20, 2, r), RequestInterval(10, 10, r)],
    )

    for optimize in [True, False]:
        cluster.change_cluster_config(optimize=optimize)
        cluster.start_servers()
        cluster.start_clients()
        cluster.await_cluster()
        iteration_directory = Path.joinpath(experiment_log_dir, f"optimize-{optimize}")
        cluster.get_logs(iteration_directory)
    cluster.shutdown()


def shifting_conditions_experiment():
    cluster_size = 5
    experiment_log_dir = Path(f"./logs/shifting-conditions")
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = (
        AutoQuorumClusterBuilder(cluster_name)
        .set_initial_leader(2)
        .set_optimize_setting(True)
        .set_optimize_threshold(0.85)
        .add_server(1, "us-west2-a")
        .add_client(1, "us-west2-a")
        .add_server(2, "us-south1-a")
        .add_client(2, "us-south1-a", kill_signal_sec=2)
        .add_server(3, "us-east4-a")
        .add_client(3, "us-east4-a")
        .add_server(4, "europe-southwest1-a")
        .add_client(4, "europe-southwest1-a")
        .add_server(5, "europe-west4-a")
        .add_client(5, "europe-west4-a")
    ).build()

    r = 0.0  # read ratio
    for id in range(1, cluster_size + 1):
        cluster.change_client_config(
            id, request_rate_intervals=[RequestInterval(5, 10, r)]
        )

    # for optimize in [True, False]:
    for optimize in [True]:
        cluster.change_cluster_config(optimize=optimize)
        cluster.start_servers()
        cluster.start_clients()
        cluster.await_cluster()
        iteration_directory = Path.joinpath(experiment_log_dir, f"optimize-{optimize}")
        cluster.get_logs(iteration_directory)
    cluster.shutdown()


def main():
    round_robin_experiment()


if __name__ == "__main__":
    main()
