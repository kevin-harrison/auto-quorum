from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from experiments.experiment_data import ExperimentData
from graphs.base_graphs import read_write_latency_bar_chart, server_breakdown_bar_chart
from graphs.colors import strat_colors, strat_markers
from graphs.debug_graphs import (
    graph_cluster_latency,
    graph_latency_estimation_comparison,
    graph_server_data,
)

_experiment_dir = Path(__file__).parent.parent / "logs" / "mixed-strats"


def mixed_strats_data(cluster_type: str) -> ExperimentData:
    return ExperimentData(_experiment_dir / cluster_type)


def graph_mixed_strat(debug: bool = False):
    if debug:
        aq_data = ExperimentData(_experiment_dir / "AutoQuorum")
        graph_server_data(aq_data)
        graph_latency_estimation_comparison(aq_data)
        graph_cluster_latency(aq_data)
    overview_bar_chart()
    breakdown_bar_chart()
    graph_mixed_strats_over_workload()


def overview_bar_chart():
    experiments = [
        ("AutoQuorum", "AutoQuorum", ExperimentData(_experiment_dir / "AutoQuorum")),
        # ("Mixed", "[LA, (4,2), MIX]", ExperimentData(_experiment_dir / "Mixed")),
        (
            "BallotRead",
            "Only DQR reads",
            ExperimentData(_experiment_dir / "BallotRead"),
        ),
        (
            "ReadAsWrite",
            "Only RAW reads",
            ExperimentData(_experiment_dir / "ReadAsWrite"),
        ),
    ]
    fig, ax = read_write_latency_bar_chart(experiments)
    fig.suptitle(
        "Global vs. Per-node Read Strategy Effect on 50-50 Read-Write Workload"
    )
    ax.legend(
        loc="upper left",
        ncols=1,
        fontsize=14,
        frameon=False,
    )
    # plt.savefig("logs/autoquorum-to-show/mixed-strats-overview.svg", format="svg")
    plt.show()
    plt.close()


def breakdown_bar_chart():
    experiments = [
        (
            "ReadAsWrite",
            "Only RAW reads",
            ExperimentData(_experiment_dir / "ReadAsWrite"),
        ),
        (
            "BallotRead",
            "Only DQR reads",
            ExperimentData(_experiment_dir / "BallotRead"),
        ),
        ("AutoQuorum", "AutoQuorum", ExperimentData(_experiment_dir / "AutoQuorum")),
    ]
    fig, ax = server_breakdown_bar_chart(experiments)
    fig.suptitle("Read Strategy Latency When Leader=LA, RQ=4, WQ=2")
    ax.legend(
        loc="upper left",
        ncols=1,
        fontsize=14,
        frameon=False,
    )
    # plt.savefig("logs/autoquorum-to-show/mixed-strats-breakdown.svg", format="svg")
    plt.show()
    plt.close()


def graph_mixed_strats_over_workload():
    strats = ["BallotRead", "Mixed", "ReadAsWrite", "AutoQuorum"]
    labels = {"BallotRead": "DQR", "EPaxos": "EPaxos fast path"}
    fig, axs = plt.subplots(1, 2, figsize=(18, 6), constrained_layout=True, sharey=True)
    fig.suptitle(f"Mixed Strats - Latency Over Varied Workloads", size=20)
    graph_varied_read_ratio(axs[0], _experiment_dir, strats, labels)
    graph_varied_skew(axs[1], _experiment_dir, strats, labels)
    handles, labels = axs[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        bbox_to_anchor=(0.5, 0.88),
        loc="center",
        ncol=4,
        fontsize=10,
        frameon=False,
    )
    plt.tight_layout(rect=(0.0, 0.0, 1.0, 0.95))
    # plt.savefig("logs/autoquorum-to-show/mixed-strats-varied-strats.svg", format="svg")
    plt.show()
    plt.close()


def graph_varied_skew(
    fig, experiment_dir: Path, strats: list[str], labels: dict[str, str]
):
    hotspots_data = []
    relative_eu_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
    for hotspot in relative_eu_loads:
        for strat in strats:
            run_directory = experiment_dir / f"eu-load-{hotspot}/{strat}"
            exp_data = ExperimentData(run_directory)
            all_clients = pd.concat(exp_data.client_data.values())
            all_clients["hotspot"] = hotspot
            all_clients["strat"] = labels.get(strat) or strat
            hotspots_data.append(all_clients)
    hotspots_df = pd.concat(hotspots_data)

    grouped_rate = (
        hotspots_df.groupby(["strat", "hotspot"])["response_latency"]
        .mean()
        .reset_index()
    )
    for strat in grouped_rate["strat"].unique():
        strat_data = grouped_rate[grouped_rate["strat"] == strat]
        fig.plot(
            strat_data["hotspot"],
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    fig.set_xlabel("Relative EU Load")
    fig.grid(axis="y", linestyle="--")
    fig.tick_params(axis="y", which="both", length=0)


def graph_varied_read_ratio(
    fig, experiment_dir: Path, strats: list[str], labels: dict[str, str]
):
    read_ratio_data = []
    for read_ratio in [0.1, 0.3, 0.5, 0.7, 0.9]:
        for strat in strats:
            run_directory = experiment_dir / f"read-ratio-{read_ratio}/{strat}"
            exp_data = ExperimentData(run_directory)
            requests = pd.concat(exp_data.client_data)
            requests["read_ratio"] = read_ratio
            requests["strat"] = labels.get(strat) or strat
            read_ratio_data.append(requests)
    ratio_df = pd.concat(read_ratio_data)

    grouped_ratio = (
        ratio_df.groupby(["strat", "read_ratio"])["response_latency"]
        .mean()
        .reset_index()
    )
    for strat in grouped_ratio["strat"].unique():
        strat_data = grouped_ratio[grouped_ratio["strat"] == strat]
        fig.plot(
            strat_data["read_ratio"],
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    fig.set_xlabel("Read Ratio")
    fig.set_ylabel("Mean Response Latency")
    fig.set_ylim(bottom=0, top=160)
    fig.grid(axis="y", linestyle="--")
