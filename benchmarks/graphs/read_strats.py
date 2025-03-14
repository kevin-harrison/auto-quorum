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

_experiment_dir = Path(__file__).parent.parent / "logs" / "read-strats"


def graph_read_strats(debug: bool = False):
    if debug:
        aq_data = ExperimentData(_experiment_dir / "AutoQuorum")
        graph_server_data(aq_data)
        graph_latency_estimation_comparison(aq_data)
        graph_cluster_latency(aq_data)
    overview_bar_chart()
    breakdown_bar_chart()
    graph_read_strats_over_workloads()


def overview_bar_chart():
    experiments = [
        (
            "BallotRead",
            "[D, (2,4), DQR]",
            ExperimentData(_experiment_dir / "BallotRead"),
        ),
        (
            "QuorumRead",
            "[D, (2,4), QuorumRead]",
            ExperimentData(_experiment_dir / "QuorumRead"),
        ),
        (
            "ReadAsWrite",
            "[D, (4,2), RAW]",
            ExperimentData(_experiment_dir / "ReadAsWrite"),
        ),
        ("AutoQuorum", "AutoQuorum", ExperimentData(_experiment_dir / "AutoQuorum")),
    ]
    fig, ax = read_write_latency_bar_chart(experiments)
    fig.suptitle(f"Read Strategy Effect on 90% Read Ratio Workload")
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.02),
        ncol=3,
        borderaxespad=0,
        frameon=False,
    )
    # plt.savefig("logs/autoquorum-to-show/read-strats-overview.svg", format="svg")
    plt.show()
    plt.close()


def breakdown_bar_chart():
    experiments = [
        (
            "BallotRead",
            "[D, (2,4), DQR]",
            ExperimentData(_experiment_dir / "BallotRead"),
        ),
        (
            "QuorumRead",
            "[D, (2,4), QuorumRead]",
            ExperimentData(_experiment_dir / "QuorumRead"),
        ),
    ]
    fig, ax = server_breakdown_bar_chart(experiments)
    fig.suptitle(f"Read Strategy Effect on 90% Read Ratio Workload")
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.02),
        ncol=2,
        borderaxespad=0,
        frameon=False,
    )
    # plt.savefig("logs/autoquorum-to-show/read-strats-breakdown.svg", format="svg")
    plt.show()
    plt.close()


def graph_read_strats_over_workloads():
    strats = ["QuorumRead", "BallotRead", "ReadAsWrite", "AutoQuorum"]
    labels = {"BallotRead": "DQR", "EPaxos": "EPaxos fast path"}
    fig, axs = plt.subplots(1, 3, figsize=(18, 6), constrained_layout=True, sharey=True)
    fig.suptitle(f"Read Strats - Latency Over Varied Workloads", size=20)
    graph_varied_read_ratio(axs[0], _experiment_dir, strats, labels)
    graph_varied_absolute_rate(axs[1], _experiment_dir, strats, labels)
    graph_varied_skew(axs[2], _experiment_dir, strats, labels)
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
    # plt.savefig("logs/autoquorum-to-show/read-strats-varied-strat.svg", format="svg")
    plt.show()
    plt.close()


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
    fig.set_ylabel("Average Response Latency")
    fig.set_ylim(bottom=0, top=145)
    fig.grid(axis="y", linestyle="--")


def graph_varied_absolute_rate(
    fig, experiment_dir: Path, strats: list[str], labels: dict[str, str]
):
    rates_data = []
    slow_rates = {}
    for load in [30, 90, 180, 210, 300]:
        # for rate in [1, 3, 5, 7, 9]:
        for strat in strats:
            run_directory = experiment_dir / f"total-load-{load}/{strat}"
            exp_data = ExperimentData(run_directory)
            requests = pd.concat(exp_data.client_data)
            requests["load"] = load
            requests["strat"] = labels.get(strat) or strat
            if strat == "QuorumRead":
                slow = (requests["response_latency"] > 50).sum()
                slow_rates[load] = slow / requests["response_latency"].notna().sum()
            rates_data.append(requests)
    rates_df = pd.concat(rates_data)

    grouped_rate = (
        rates_df.groupby(["strat", "load"])["response_latency"].mean().reset_index()
    )
    for strat in grouped_rate["strat"].unique():
        strat_data = grouped_rate[grouped_rate["strat"] == strat]
        fig.plot(
            strat_data["load"].map(slow_rates),
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    fig.set_xlabel("Concurrent Write Rate")
    fig.grid(axis="y", linestyle="--")
    fig.tick_params(axis="y", which="both", length=0)


def graph_varied_skew(
    fig, experiment_dir: Path, strats: list[str], labels: dict[str, str]
):
    hotspots_data = []
    relative_us_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
    for hotspot in relative_us_loads:
        for strat in strats:
            run_directory = experiment_dir / f"us-load-{hotspot}/{strat}"
            exp_data = ExperimentData(run_directory)
            requests = pd.concat(exp_data.client_data)
            requests["hotspot"] = hotspot
            requests["strat"] = labels.get(strat) or strat
            hotspots_data.append(requests)
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
    fig.set_xlabel("Relative US Load")
    fig.grid(axis="y", linestyle="--")
    fig.tick_params(axis="y", which="both", length=0)
