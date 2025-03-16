import matplotlib.pyplot as plt
import pandas as pd

from experiments.experiment_data import ExperimentData
from graphs.base_graphs import create_base_figure
from graphs.colors import strat_colors


def graph_server_data(exp_data):
    exp_data.show_reconfigurations()

    for id, server_metrics in exp_data.server_data.items():
        title = f"Server {id} ({exp_data.get_server_location(id)}) Metrics"
        fig, axs = create_base_figure(
            exp_data, relative_rate=False, show_strategy_changes=True
        )
        fig.suptitle(title, y=0.95)

        # Graph request latencies
        client_requests = exp_data.client_data.get(id)
        if client_requests is not None:
            read_requests = client_requests.loc[client_requests["write"] == False]
            axs[0].scatter(
                read_requests.index,
                read_requests["response_latency"],
                marker="o",
                linestyle="-",
                label="Read Latency",
                color="brown",
                alpha=0.3,
            )
            write_requests = client_requests.loc[client_requests["write"]]
            axs[0].scatter(
                write_requests.index,
                write_requests["response_latency"],
                marker="o",
                linestyle="-",
                label="Write latency",
                color="black",
                alpha=0.3,
            )

        # Graph heartbeat latencies
        metric_skip = 0
        for peer in filter(lambda s: s != id, exp_data.server_data.keys()):
            peer_latencies = server_metrics["cluster_metrics"].apply(
                lambda metrics: metrics["latencies"][id - 1][peer - 1]
            )
            axs[0].plot(
                server_metrics.index[metric_skip:],
                peer_latencies[metric_skip:],
                linestyle="-",
                label=f"Peer {peer} ({exp_data.get_server_location(peer)}) latency",
                color=exp_data.get_server_color(peer),
            )

        # Graph estimated read/write latencies
        write_latency_estimate = server_metrics["operation_latency"].apply(
            lambda est: est["write_latency"]
        )
        read_latency_estimate = server_metrics["operation_latency"].apply(
            lambda est: est["read_latency"]
        )
        # read_latency_estimate = server_metrics["operation_latency.read_latency"]
        axs[0].plot(
            server_metrics.index[metric_skip:],
            write_latency_estimate[metric_skip:],
            linestyle="--",
            label=f"Estimated write latency",
            color="brown",
        )
        axs[0].plot(
            server_metrics.index[metric_skip:],
            read_latency_estimate[metric_skip:],
            linestyle="--",
            label=f"Estimated read latency",
            color="black",
        )

        # # Graph workload metrics
        # for server in exp_data.server_data.keys():
        #     a = server_metrics["cluster_metrics"].apply(
        #         lambda metrics: metrics["workload"][server - 1]["reads"]
        #         + metrics["workload"][server - 1]["writes"]
        #     )
        #     axs[1].plot(
        #         server_metrics.index,
        #         a,
        #         linestyle="--",
        #         label=f"Server {server} ({exp_data.get_server_location(server)}) workload estimate",
        #         color=exp_data.get_server_color(server),
        #     )

        axs[0].legend(title="Latency info", bbox_to_anchor=(1.01, 1), loc="upper left")
        axs[1].legend(
            title="Workload info", bbox_to_anchor=(1.01, 0.9), loc="upper left"
        )
        plt.show()
        plt.close()


def graph_latency_estimation_comparison(exp_data: ExperimentData):
    exp_data.show_reconfigurations()
    title = "Average request latency vs. optimizer predicted latency"
    fig, axs = create_base_figure(
        exp_data, relative_rate=False, show_strategy_changes=True
    )
    fig.suptitle(title, y=0.95)

    # Combine client requests in experiment
    all_requests = pd.concat(exp_data.client_data.values())
    # Moving average latency
    average_latency = all_requests["response_latency"].resample("500ms").mean()
    axs[0].plot(
        average_latency.index,
        average_latency.values,
        linestyle="-",
        label="with optimization",
        color=strat_colors["AutoQuorum"],
    )
    # Estimated average latency
    graph_estimated_latency(axs[0], exp_data)

    fig.legend()
    plt.show()
    plt.close()


def graph_cluster_latency(exp_data: ExperimentData):
    exp_data.show_reconfigurations()
    title = "Cluster-wide Request Latency"
    fig, axs = create_base_figure(
        exp_data, relative_rate=False, show_strategy_changes=True
    )
    fig.suptitle(title, y=0.95)

    # Request latency
    for id, requests in exp_data.client_data.items():
        axs[0].scatter(
            requests.index,
            requests["response_latency"],
            marker="o",
            linestyle="-",
            label=f"client {id}",
        )
    graph_estimated_latency(axs[0], exp_data)
    fig.legend()
    plt.show()
    plt.close()


def graph_estimated_latency(fig, exp_data: ExperimentData):
    if exp_data.strategy_data is not None:
        estimated_latency = exp_data.strategy_data.curr_strat_latency
        fig.plot(
            exp_data.strategy_data.index,
            estimated_latency,
            marker="o",
            linestyle="-",
            label=f"Estimated average latency",
            color="grey",
        )
