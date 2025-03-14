from pathlib import Path

import matplotlib.pyplot as plt

from experiments.experiment_data import ExperimentData
from graphs.base_graphs import graph_timeseries_latency
from graphs.debug_graphs import (
    graph_cluster_latency,
    graph_latency_estimation_comparison,
    graph_server_data,
)

_experiment_directory = Path(__file__).parent.parent / "logs" / "round-robin"


def graph_round_robin(debug: bool = False):
    aq_data = ExperimentData(_experiment_directory / "AutoQuorum")
    if debug:
        graph_server_data(aq_data)
        graph_latency_estimation_comparison(aq_data)
        graph_cluster_latency(aq_data)

    other_experiments = [
        ("Static LA Leader", ExperimentData(_experiment_directory / "Baseline")),
        ("EPaxos", ExperimentData(_experiment_directory / "MultiLeader-SuperMajority")),
        ("Etcd", ExperimentData(_experiment_directory / "Etcd")),
    ]
    fig, _ = graph_timeseries_latency(
        aq_data,
        other_experiments,
    )
    fig.legend(
        loc="upper left",
        bbox_to_anchor=(0.099, 0.99),
        fontsize=12,
        ncols=1,
        frameon=False,
    )
    # plt.savefig("logs/autoquorum-to-show/round-robin-5.svg", format="svg")
    plt.show()
    plt.close()


def round_robin_reconfigurations():
    aq_data = ExperimentData(_experiment_directory / "AutoQuorum")
    aq_data.show_reconfigurations()
