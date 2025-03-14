from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from experiments.experiment_data import ExperimentData
from graphs.base_graphs import create_base_barchart
from graphs.colors import strat_colors, strat_hatches
from graphs.debug_graphs import (
    graph_cluster_latency,
    graph_latency_estimation_comparison,
    graph_server_data,
)

_experiment_dir = Path(__file__).parent.parent / "logs" / "even-load"


def graph_even_load(debug: bool = False):
    if debug:
        aq_data = ExperimentData(_experiment_dir / "read-ratio-0.0" / "AutoQuorum")
        graph_server_data(aq_data)
        graph_latency_estimation_comparison(aq_data)
        graph_cluster_latency(aq_data)

    fig, ax = plt.subplots(1, 4, sharey=True, figsize=(18, 4))
    for i, read_ratio in enumerate([0.0, 0.5, 0.95, 1.0]):
        iteration_dir = _experiment_dir / f"read-ratio-{read_ratio}"
        latency_means = {}
        bar_group_labels = add_autoquorum_data(iteration_dir, latency_means)
        add_epaxos_data(iteration_dir, latency_means)
        add_etcd_data(iteration_dir, latency_means)
        add_baseline_data(iteration_dir, latency_means)
        ax[i].set_title(f"Read ratio {read_ratio}", size=10)
        create_base_barchart(
            ax[i],
            latency_means,
            bar_group_labels,
            error_bars=False,
            axis_label_size=10,
            axis_tick_size=6,
            rotate_x_ticks=True,
        )
    handles, labels = ax[0].get_legend_handles_labels()
    fig.suptitle(f"Even Load Benchmarks", size=20)
    fig.legend(
        handles,
        labels,
        bbox_to_anchor=(0.5, 0.83),
        loc="center",
        ncol=6,
        fontsize=10,
        frameon=False,
    )
    # Reserve top space for suptitle & legend
    plt.tight_layout(rect=(0.0, 0.0, 1.0, 0.90))
    plt.show()
    plt.close()


def add_autoquorum_data(iteration_dir: Path, latency_means) -> list[str]:
    aq_data = ExperimentData(iteration_dir / "AutoQuorum")
    num_clients = len(aq_data.client_data)
    latencies = [0] * num_clients
    bar_group_labels = [""] * num_clients
    stds = [None] * num_clients
    leader = aq_data.experiment_summary["autoquorum_cluster_config"]["initial_leader"]
    for client, client_requests in aq_data.client_data.items():
        location = aq_data.get_client_location(client)
        if client == leader:
            location += "*"
        aq_latency = client_requests["response_latency"].mean()
        latencies[client - 1] = aq_latency
        bar_group_labels[client - 1] = location
    latency_means["AutoQuorum"] = (
        latencies,
        stds,
        strat_colors["AutoQuorum"],
        strat_hatches["AutoQuorum"],
    )
    return bar_group_labels


def add_etcd_data(iteration_dir: Path, latency_means):
    exp_data = ExperimentData(iteration_dir / "Etcd")
    num_clients = len(exp_data.client_data)
    latencies = [0] * num_clients
    stds = [None] * num_clients
    for client, client_requests in exp_data.client_data.items():
        latency = client_requests["response_latency"].mean()
        latencies[client - 1] = latency
    latency_means["Etcd"] = (
        latencies,
        stds,
        strat_colors["Etcd"],
        strat_hatches["Etcd"],
    )


def add_baseline_data(iteration_dir: Path, latency_means):
    exp_data = ExperimentData(iteration_dir / "Baseline")
    num_clients = len(exp_data.client_data)
    latencies = [0] * num_clients
    stds = [None] * num_clients
    for client, client_requests in exp_data.client_data.items():
        latency = client_requests["response_latency"].mean()
        latencies[client - 1] = latency
    latency_means["Baseline"] = (
        latencies,
        stds,
        strat_colors["Baseline"],
        strat_hatches["Baseline"],
    )


def add_epaxos_data(iteration_dir: Path, latency_means):
    maj_data = ExperimentData(iteration_dir / "MultiLeader-Majority")
    super_maj_data = ExperimentData(iteration_dir / "MultiLeader-SuperMajority")
    num_clients = len(maj_data.client_data)
    conflict_rates = np.linspace(0, 1, 3)
    for conflict_rate in conflict_rates:
        latencies = [0] * num_clients
        stds = [None] * num_clients
        for client in maj_data.client_data:
            maj_latency = maj_data.client_data[client]["response_latency"].mean()
            super_maj_latency = super_maj_data.client_data[client][
                "response_latency"
            ].mean()
            epaxos_latency = super_maj_latency + (conflict_rate * maj_latency)
            latencies[client - 1] = epaxos_latency
        latency_means[f"EPaxos {conflict_rate}"] = (
            latencies,
            stds,
            str(1 - conflict_rate),
            None,
        )
