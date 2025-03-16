import math
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

from experiments.experiment_data import ExperimentData
from graphs.colors import strat_colors, strat_hatches


def create_base_figure(
    exp_data: ExperimentData,
    relative_rate: bool = True,
    show_strategy_changes: bool = False,
):
    axis_label_size = 20
    axis_tick_size = 12
    fig, axs = plt.subplots(
        2, 1, sharex=True, gridspec_kw={"height_ratios": [4, 1]}, layout="constrained"
    )
    fig.set_size_inches((12, 6))
    # Axis labels
    axs[0].set_ylabel("Request Latency\n(ms)", fontsize=axis_label_size)
    axs[1].set_xlabel("Experiment Time", fontsize=axis_label_size)
    axs[1].set_ylabel("Request Rate\n(%)", fontsize=axis_label_size)
    fig.align_ylabels()
    # Splines
    axs[0].spines["top"].set_visible(False)
    axs[0].spines["right"].set_visible(False)
    # axs[0].spines['bottom'].set_color("#DDDDDD")
    axs[0].spines["bottom"].set_visible(False)
    axs[1].spines["top"].set_visible(False)
    axs[1].spines["right"].set_visible(False)
    # axs[1].spines['bottom'].set_visible(False)
    # Axis lim - Set y-axis limit to be just above the max client latency
    top = 0
    for df in exp_data.client_data.values():
        client_max = df["response_latency"].max()
        if client_max > top:
            top = client_max
    axs[0].set_ylim(bottom=0, top=math.ceil(top / 10) * 10)
    # Axis ticks
    # axs[0].set_yticks(axs[0].get_yticks()[1:])
    axs[0].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="x", labelsize=axis_tick_size)
    myFmt = mdates.DateFormatter("%M:%S")
    fig.gca().xaxis.set_major_formatter(myFmt)
    axs[0].tick_params(bottom=False)
    axs[1].tick_params(bottom=False)

    if show_strategy_changes:
        graph_strategy_changes(axs[0], exp_data)
    if relative_rate:
        graph_relative_request_rate_subplot(axs[1], exp_data)
    else:
        graph_request_rate_subplot(axs[1], exp_data)
    # plt.tight_layout()
    return fig, axs


def graph_strategy_changes(fig, exp_data: ExperimentData):
    if exp_data.strategy_data is not None:
        for change_time, strat_change in exp_data.strategy_data[
            exp_data.strategy_data["reconfigure"]
        ].iterrows():
            new_leader = int(strat_change["leader"])
            color = exp_data.get_server_color(new_leader)
            fig.axvline(x=change_time, linestyle="--", color=color)


def graph_request_rate_subplot(fig, exp_data: ExperimentData):
    for id, requests in exp_data.client_data.items():
        request_rate = requests.resample("1s").count()
        ma = request_rate.ewm(alpha=0.9).mean()
        fig.plot(
            request_rate.index,
            ma["response_latency"],
            linestyle="-",
            label=f"{exp_data.get_client_location(id)} Requests",
            color=exp_data.get_client_color(id),
            linewidth=1,
        )


def graph_relative_request_rate_subplot(fig, exp_data: ExperimentData):
    fig.set_ylim(bottom=0, top=1)
    fig.set_yticks([0.0, 0.5, 1.0])
    total_request_rate = pd.DataFrame()
    for requests in exp_data.client_data.values():
        request_rate = requests.resample("3s").count()
        total_request_rate = total_request_rate.add(request_rate, fill_value=0)
    total_request_rate = total_request_rate[
        (total_request_rate["response_latency"] != 0)
    ]
    fig.set_xlim(
        left=total_request_rate.index.min(), right=total_request_rate.index.max()
    )
    for id, requests in exp_data.client_data.items():
        request_rate = requests.resample("3s").count() / total_request_rate
        request_rate.fillna(0, inplace=True)
        ma = request_rate.ewm(alpha=0.9).mean()
        color = exp_data.get_client_color(id)
        fig.plot(
            request_rate.index,
            ma.response_latency,
            linestyle="-",
            # label=f"{exp_data.get_client_location(id)} Relative Requests",
            color=color,
            linewidth=1,
        )
        fig.fill_between(
            request_rate.index, ma.response_latency, color=color, alpha=0.3
        )


def graph_read_ratio_subplot(fig, exp_data: ExperimentData):
    fig.set_ylim(bottom=0, top=1)
    fig.set_yticks([0.0, 0.5, 1.0])
    requests = pd.concat(exp_data.client_data.values())
    read_requests = requests[requests["write"] == False]
    read_ratios = read_requests.resample("1s").mean()
    # ma = read_ratios.ewm(alpha=0.9).mean()
    fig.set_xlim(left=read_ratios.index.min(), right=read_ratios.index.max())
    fig.set_xticks(fig.get_xticks()[1:])
    fig.plot(read_ratios.index, read_ratios, linestyle="-", color="black")
    fig.fill_between(read_ratios.index, read_ratios, color="black", alpha=0.3)


# NOTE: Mutates experiment data indices
def graph_timeseries_latency(
    autoquorum_data: ExperimentData,
    experiments: list[tuple[str, ExperimentData]],
) -> tuple[Figure, Any]:
    # Create base figure with AutoQuorum data
    autoquorum_data.normalize_to_experiment_start()
    fig, axs = create_base_figure(autoquorum_data)

    # Plot experiment data
    experiments.append(("AutoQuorum", autoquorum_data))
    for experiment_name, exp_data in experiments:
        if experiment_name != "AutoQuorum":
            exp_data.normalize_to_experiment_start()
        requests = pd.concat(exp_data.client_data.values())
        average_latency = requests["response_latency"].resample("3s").mean()
        axs[0].plot(
            average_latency.index,
            average_latency,
            linestyle="-",
            label=experiment_name,
            color=strat_colors[experiment_name],
            # marker=strat_markers[experiment_name],
            linewidth=2,
        )
        if experiment_name == "AutoQuorum":
            place_strategy_change_markers(axs[0], exp_data, average_latency)
    return fig, axs


# Graph strat changes on latency line
def place_strategy_change_markers(
    fig, exp_data: ExperimentData, request_latencies: pd.DataFrame
):
    assert exp_data.strategy_data is not None
    strategy_changes = exp_data.strategy_data[exp_data.strategy_data["reconfigure"]]
    for change_time, strat_change in strategy_changes.iterrows():
        for window in request_latencies.rolling(2):
            if window.notnull().sum() != 2:
                continue
            start = window.index[0]
            end = window.index[1]
            # Ensure that start <= change_time <= end
            if start <= change_time <= end:
                start_value = window.iloc[0]
                end_value = window.iloc[1]
                # Calculate the interpolated y value
                time_difference = (end - start).total_seconds()
                fraction = (change_time - start).total_seconds() / time_difference
                interpolated_y = start_value + (end_value - start_value) * fraction
                # Plot the strat change dot
                new_leader = int(strat_change["opt_strat"]["leader"])
                color = exp_data.get_server_color(new_leader)
                fig.plot(change_time, interpolated_y, "o", color=color)


def create_base_barchart(
    ax,
    latency_means: dict,
    bar_group_labels: list[str],
    error_bars: bool = False,
    axis_label_size=20,
    axis_tick_size=12,
    rotate_x_ticks: bool = False,
):
    num_bars_per_group = len(latency_means)
    width = 0.25  # the width of the bars
    x = (
        np.arange(len(bar_group_labels)) * (num_bars_per_group + 1) * width
    )  # Adjust spacing between groups
    multiplier = 0

    for bar_in_group_label, (avg, std_dev, color, hatch) in latency_means.items():
        avg = tuple(0 if v is None else v for v in avg)
        std_dev = tuple(0 if v is None else v for v in std_dev)
        if error_bars is False:
            std_dev = None
        offset = width * multiplier
        ax.bar(
            x + offset,
            avg,
            width,
            label=bar_in_group_label,
            color=color,
            edgecolor="black",
            yerr=std_dev,
            linewidth=1.5,
            hatch=hatch,
        )
        ax.errorbar(
            x + offset,
            avg,
            yerr=std_dev,
            fmt="none",  # Do not plot any markers
            capsize=5,  # Length of the error bar caps
            elinewidth=1,  # Width of the error bars
            color="black",  # Color of the error bars
        )
        multiplier += 1

    # Add consistent group spacing
    ax.set_xticks(x + (width * (num_bars_per_group - 1)) / 2)
    ax.set_xticklabels(bar_group_labels, fontsize=axis_label_size)
    if rotate_x_ticks:
        ax.tick_params(axis="x", rotation=45)

    ax.set_ylabel("Latency (ms)", fontsize=axis_label_size)
    ax.tick_params(axis="y", labelsize=axis_tick_size)
    ax.tick_params(bottom=False)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color="#EEEEEE")
    ax.xaxis.grid(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def read_write_latency_bar_chart(
    experiments: list[tuple[str, str, ExperimentData]],  # name, data, label
) -> tuple[Figure, Any]:
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    bar_group_labels = [
        "Write Latency\nAverage",
        "Read Latency\nAverage",
        "Total Latency\nAverage",
    ]
    latency_means = {}
    for strat, label, exp_data in experiments:
        requests = pd.concat(exp_data.client_data.values())
        avg = get_averages(requests)
        std_dev = get_std_devs(requests)
        latency_means[label] = (
            avg,
            std_dev,
            strat_colors[strat],
            strat_hatches[strat],
        )
    create_base_barchart(ax, latency_means, bar_group_labels)
    return fig, ax


def get_std_devs(df: pd.DataFrame) -> tuple[float | None, float | None, float]:
    request_dev = df.response_latency.std()
    assert isinstance(request_dev, float)
    write_mask = df["write"]
    write_dev = df.loc[write_mask].response_latency.std()
    read_dev = df.loc[~write_mask].response_latency.std()
    return (write_dev, read_dev, request_dev)


def get_averages(df: pd.DataFrame) -> tuple[float | None, float | None, float]:
    request_avg = df.response_latency.mean()
    assert isinstance(request_avg, float)
    write_mask = df["write"]
    write_avg = df.loc[write_mask].response_latency.mean()
    read_avg = df.loc[~write_mask].response_latency.mean()
    return (write_avg, read_avg, request_avg)


def server_breakdown_bar_chart(
    experiments: list[tuple[str, str, ExperimentData]],  # (strat, label, data)
) -> tuple[Figure, Any]:
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    latency_means = {}
    bar_group_labels = [""] * 5
    for strat, label, exp_data in experiments:
        strat_avgs = [0.0] * 5
        strat_stds = [0.0] * 5
        for client, client_requests in exp_data.client_data.items():
            location = exp_data.get_client_location(client)
            _, read_avg, _ = get_averages(client_requests)
            _, read_std, _ = get_std_devs(client_requests)
            strat_avgs[client - 1] = read_avg
            strat_stds[client - 1] = read_std
            if strat == "BallotRead":
                leader = exp_data.experiment_summary["autoquorum_cluster_config"][
                    "initial_leader"
                ]
                if client == leader:
                    location += "*"
                bar_group_labels[client - 1] = location
        latency_means[label] = (
            strat_avgs,
            strat_stds,
            strat_colors[strat],
            strat_hatches[strat],
        )
    create_base_barchart(ax, latency_means, bar_group_labels, error_bars=True)
    return fig, ax
