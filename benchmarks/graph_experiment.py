import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


@dataclass
class ExperimentFiles:
    server_files: dict[int, Path]
    client_files: dict[int, tuple[Path, Path]]


def find_experiment_logs(experiment_name: str) -> ExperimentFiles:
    experiment_directory = Path(f"logs/{experiment_name}")
    assert (
        experiment_directory.exists()
    ), f"There is no {experiment_name} expermiment data"
    server_pattern = re.compile(r"^server-(\d+)\.json")
    experiment_files = ExperimentFiles({}, {})
    for file in experiment_directory.rglob("*.json"):
        match = server_pattern.search(file.name)
        if match:
            server_id = int(match.group(1))
            client_output = Path(
                str(file).replace(f"server-{server_id}.json", f"client-{server_id}.csv")
            )
            client_summary = Path(
                str(file).replace(
                    f"server-{server_id}.json", f"client-{server_id}.json"
                )
            )
            experiment_files.server_files[server_id] = file
            if client_output.exists():
                experiment_files.client_files[server_id] = (
                    client_output,
                    client_summary,
                )
    return experiment_files


def parse_server_log(output_file: Path, dump_df: bool = False) -> pd.DataFrame:
    with open(output_file, "r") as file:
        server_output = json.load(file)
    server_data = pd.json_normalize(server_output["log"])
    server_data = server_data.astype(dtype={"timestamp": "datetime64[ms]"})
    server_data.set_index("timestamp", inplace=True)
    server_data.attrs = server_output["server_config"]
    if dump_df:
        server_id = server_data.attrs["server_id"]
        df_file = str(output_file).replace(
            f"server-{server_id}.json", f"server-{server_id}.df"
        )
        with open(df_file, "w") as f:
            with pd.option_context(
                "display.max_rows", None, "display.max_columns", None
            ):
                print(server_data.to_string(), file=f)
    return server_data


def parse_client_log(output_file: Path, summary_file: Path) -> pd.DataFrame:
    print(output_file)
    client_data = pd.read_csv(
        output_file,
        dtype={"write": "bool"},
    )
    client_data["response_latency"] = (
        client_data.response_time - client_data.request_time
    )
    client_data = client_data.astype(dtype={"request_time": "datetime64[ms]"})
    client_data.set_index("request_time", inplace=True)
    with open(summary_file, "r") as file:
        client_summary = json.load(file)
    assert client_summary["sync_time"] >= 0, "Clients' start not synced"
    client_data.attrs = client_summary
    return client_data


def get_experiment_data(
    experiment_name: str, dump_server_df: bool = False
) -> tuple[dict[int, pd.DataFrame], dict[int, pd.DataFrame], pd.DataFrame]:
    servers_data = {}
    clients_data = {}
    strategy_data = []
    experiment_files = find_experiment_logs(experiment_name)
    for id, server_log_file in experiment_files.server_files.items():
        server_data = parse_server_log(server_log_file, True)
        server_color = location_color(server_data.attrs["location"])
        server_colors[id] = server_color
        server_locations[id] = server_data.attrs["location"]
        strategy_cols = get_strategy_columns(server_data)
        server_strategy_data = server_data.loc[server_data["leader"] == id][
            strategy_cols
        ]
        strategy_data.append(server_strategy_data)
        servers_data[id] = server_data
        client_files = experiment_files.client_files.get(id)
        if client_files is not None:
            client_log_file, client_summary_file = client_files
            clients_data[id] = parse_client_log(client_log_file, client_summary_file)
    strategy_data = pd.concat(strategy_data)
    # strategy_data.drop(columns=["cluster_strategy"], inplace=True)
    strategy_data.sort_values(by="timestamp", inplace=True)
    return clients_data, servers_data, strategy_data


def get_strategy_columns(server_data: pd.DataFrame) -> list[str]:
    strategy_cols = ["reconfigure", "leader", "curr_strat_latency", "opt_strat_latency"]
    pre_normalized_columns = ["curr_strat", "opt_strat", "operation_latency"]
    normalized_columns = list(
        filter(
            lambda col: any(
                col.startswith(prefix) for prefix in pre_normalized_columns
            ),
            server_data.columns,
        )
    )
    return strategy_cols + normalized_columns


def location_name(location: str) -> str:
    if location.startswith("local"):
        return location
    name_mapping = {
        "us-west2-a": "Los Angeles",
        "us-south1-a": "Dallas",
        "us-east4-a": "N. Virginia",
        "us-east5-a": "Columbus",
        "europe-west2-a": "London",
        "europe-west4-a": "Netherlands",
        "europe-west10-a": "Berlin",
        "europe-southwest1-a": "Madrid",
        "europe-central2-a": "Warsaw",
    }
    name = name_mapping.get(location)
    if name is None:
        raise ValueError(f"Don't have name for location {location}")
    return name


def location_color(location: str):
    """
    LA: FDBB3B
    D : 4CA98F
    NV: 9A6DB8
    N: FF6478
    """
    # color_mapping = {
    #     "us-west2-a" : "#FDBB3B",
    #     "us-south1-a": "#4CA98F",
    #     "us-east4-a": "#9A6DB8",
    #     "europe-west2-a": "#4CA98F",
    #     "europe-west4-a": "#FF6478",
    #     "europe-west10-a": "#9A6DB8",
    #     "europe-southwest1-a": "#5E5E5E",
    # }
    # Presentation mapping
    color_mapping = {
        "us-west2-a": "#FDBB3B",
        "us-south1-a": "#4CA98F",
        "us-east4-a": "#9A6DB8",
        "europe-west2-a": "#4CA98F",
        "europe-west4-a": "#FF6478",
        "europe-west10-a": "#9A6DB8",
        "europe-southwest1-a": "tab:blue",
        "local-1": "tab:blue",
        "local-2": "tab:green",
        "local-3": "tab:red",
    }
    color = color_mapping.get(location)
    if color is None:
        raise ValueError(f"Don't have color for location {location}")
    return color


def create_base_figure(
    title: str, clients_data: dict[int, pd.DataFrame], strategy_data: pd.DataFrame
):
    axis_label_size = 20
    axis_tick_size = 12
    fig, axs = plt.subplots(
        2, 1, sharex=True, gridspec_kw={"height_ratios": [4, 1]}, layout="constrained"
    )
    # fig.suptitle(title, y=0.95)
    # fig.subplots_adjust(hspace=0)
    fig.set_size_inches((12, 6))
    # axs[0].set_facecolor('#f0f0f0')
    # axs[1].set_facecolor('#f0f0f0')
    # axs[0].grid(color='white', linestyle='-', linewidth=0.7)
    # axs[1].grid(color='white', linestyle='-', linewidth=0.7)
    # axs[0].yaxis.grid(color='lightgrey', linestyle='-', linewidth=0.7)
    # axs[1].grid(color='lightgrey', linestyle='--', linewidth=0.7)
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
    # Axis ticks
    # axs[0].autoscale(axis='y')
    # Set y-axis limit to be just above the max client latency
    top = 0
    for df in clients_data.values():
        client_max = df["response_latency"].max()
        if client_max > top:
            top = client_max
    axs[0].set_ylim(bottom=0, top=math.ceil(top / 10) * 10)
    # axs[0].set_yticks(axs[0].get_yticks()[1:])
    axs[0].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="x", labelsize=axis_tick_size)
    myFmt = mdates.DateFormatter("%M:%S")
    fig.gca().xaxis.set_major_formatter(myFmt)
    axs[0].tick_params(bottom=False)
    axs[1].tick_params(bottom=False)
    graph_strategy_changes(axs[0], strategy_data)
    graph_relative_request_rate_subplot(axs[1], clients_data)
    # graph_request_rate_subplot(axs[1], clients_data)
    # plt.tight_layout()
    return fig, axs


def create_read_ratio_figure(
    title: str, clients_data: dict[int, pd.DataFrame], strategy_data: pd.DataFrame
):
    axis_label_size = 20
    axis_tick_size = 12
    fig, axs = plt.subplots(
        2, 1, sharex=True, gridspec_kw={"height_ratios": [4, 1]}, layout="constrained"
    )
    # fig.suptitle(title, y=0.95)
    fig.subplots_adjust(hspace=0)
    fig.set_size_inches((12, 6))
    # axs[0].grid(color='lightgrey', linestyle='--', linewidth=0.7)
    # axs[1].grid(color='lightgrey', linestyle='--', linewidth=0.7)
    # Axis labels
    axs[0].set_ylabel("Request Latency\n(ms)", fontsize=axis_label_size)
    axs[1].set_xlabel("Experiment Time", fontsize=axis_label_size)
    axs[1].set_ylabel("Read Ratio\n(%)", fontsize=axis_label_size)
    fig.align_ylabels()
    # Splines
    axs[0].spines["top"].set_visible(False)
    axs[0].spines["right"].set_visible(False)
    axs[0].spines["bottom"].set_visible(False)
    axs[1].spines["top"].set_visible(False)
    axs[1].spines["right"].set_visible(False)
    axs[1].spines["bottom"].set_visible(False)
    # Axis ticks
    # Set y-axis limit to be just above the max client latency
    top = 0
    for df in clients_data.values():
        client_max = df["response_latency"].max()
        if client_max > top:
            top = client_max
    top = 150
    axs[0].set_ylim(bottom=0, top=top + 10)
    axs[0].set_yticks(axs[0].get_yticks()[1:])
    axs[0].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="x", labelsize=axis_tick_size)
    myFmt = mdates.DateFormatter("%M:%S")
    fig.gca().xaxis.set_major_formatter(myFmt)
    axs[0].tick_params(bottom=False)
    axs[1].tick_params(bottom=False)
    # graph_strategy_changes(axs[0], strategy_data)
    graph_read_ratio_subplot(axs[1], clients_data)
    return fig, axs


def graph_strategy_changes(fig, strategy_data):
    for change_time, strat_change in strategy_data[
        strategy_data["reconfigure"]
    ].iterrows():
        new_leader = int(strat_change["leader"])
        color = server_colors[new_leader]
        fig.axvline(x=change_time, linestyle="--", color=color)


def graph_relative_request_rate_subplot(fig, clients_data):
    fig.set_ylim(bottom=0, top=1)
    fig.set_yticks([0.0, 0.5, 1.0])
    total_request_rate = pd.DataFrame()
    for requests in clients_data.values():
        request_rate = requests.resample("1s").count()
        total_request_rate = total_request_rate.add(request_rate, fill_value=0)
    total_request_rate = total_request_rate[
        (total_request_rate["response_latency"] != 0)
    ]
    fig.set_xlim(
        left=total_request_rate.index.min(), right=total_request_rate.index.max()
    )
    fig.set_xticks(fig.get_xticks()[1:])
    for requests in clients_data.values():
        request_rate = requests.resample("1s").count() / total_request_rate
        ma = request_rate.ewm(alpha=0.9).mean()
        color = location_color(requests.attrs["location"])
        label = location_name(requests.attrs["location"])
        fig.plot(
            request_rate.index,
            ma["response_latency"],
            linestyle="-",
            color=color,
            linewidth=1,
        )
        fig.fill_between(
            request_rate.index, ma["response_latency"], color=color, alpha=0.3
        )


def graph_request_rate_subplot(fig, clients_data):
    for requests in clients_data.values():
        request_rate = requests.resample("1s").count()
        ma = request_rate.ewm(alpha=0.9).mean()
        color = location_color(requests.attrs["location"])
        label = location_name(requests.attrs["location"])
        fig.plot(
            request_rate.index,
            ma["response_latency"],
            linestyle="-",
            label=label,
            color=color,
        )


def graph_read_ratio_subplot(fig, clients_data):
    fig.set_ylim(bottom=0, top=1)
    fig.set_yticks([0.0, 0.5, 1.0])
    all_requests = pd.concat(clients_data.values())
    all_requests.dropna(subset=["response_latency"], inplace=True)
    all_requests["reads"] = all_requests["response.message.Read"].notna()
    read_ratios = all_requests["reads"].resample("1s").mean()
    # ma = read_ratios.ewm(alpha=0.9).mean()
    fig.set_xlim(left=read_ratios.index.min(), right=read_ratios.index.max())
    fig.set_xticks(fig.get_xticks()[1:])
    fig.plot(read_ratios.index, read_ratios, linestyle="-", color="black")
    fig.fill_between(read_ratios.index, read_ratios, color="black", alpha=0.3)


def create_base_barchart(
    latency_means: dict,
    bar_group_labels: list[str],
    legend_args: dict = {"loc": "upper right", "ncols": 1, "fontsize": 16},
    error_bars: bool = False,
):
    axis_label_size = 20
    axis_tick_size = 12
    textures = ["/", ".", "-", "-"]  # Different hatch patterns
    texture_index = 0
    x = np.arange(len(bar_group_labels))  # the label locations
    bar_group_size = len(latency_means)
    width = 0.25  # the width of the bars
    multiplier = 0.5
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    for bar_in_group_label, (avg, std_dev, color) in latency_means.items():
        avg = tuple(0 if v is None else v for v in avg)
        std_dev = tuple(0 if v is None else v for v in std_dev)
        if error_bars is False:
            std_dev = None
        offset = width * multiplier
        _rects = ax.bar(
            x + offset,
            avg,
            width,
            label=bar_in_group_label,
            color=color,
            edgecolor="black",
            yerr=std_dev,
            linewidth=1.5,
            # hatch=textures[texture_index]
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
        texture_index += 1
        # Adds value labels above bars
        # ax.bar_label(rects, fmt='%.2f', padding=3)
        multiplier += 1
    ax.set_ylabel("Latency (ms)", fontsize=axis_label_size)
    ax.tick_params(axis="y", labelsize=axis_tick_size)
    ax.tick_params(bottom=False)  # ax.tick_params(left=False, bottom=False)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color="#EEEEEE")
    ax.xaxis.grid(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    # ax.spines['left'].set_visible(False)
    # ax.spines['bottom'].set_color('#DDDDDD')
    if bar_group_size == 2:
        ax.set_xticks(x + width, bar_group_labels, fontsize=axis_label_size)
    elif bar_group_size == 3:
        ax.set_xticks(x + width * 1.5, bar_group_labels, fontsize=axis_label_size)
    else:
        raise ValueError(f"Haven't implemented {bar_group_size} sized bar groups")
    ax.legend(**legend_args)
    return fig, ax


def print_strats(strategy_data: pd.DataFrame):
    # strat_cols = [col for col in strategy_data.columns if col.startswith("cluster_strategy")]
    rename = {
        "cluster_strategy.leader": "strat.leader",
        "cluster_strategy.read_quorum_size": "strat.r_size",
        "cluster_strategy.write_quorum_size": "strat.w_size",
        "cluster_strategy.read_strategies": "strat.r_strats",
        "operation_latency.write_latency": "op.w_lat",
        "operation_latency.read_latency": "op.r_lat",
        "operation_latency.read_strategy": "op.r_strat",
        "opt_strat_latency": "opt_strat_lat",
        "curr_strat_latency": "curr_strat_lat",
    }
    printable_data = strategy_data.rename(columns=rename)
    print(printable_data[printable_data["reconfigure"]])


def graph_estimated_latency(fig, strategy_data: pd.DataFrame):
    estimated_latency = strategy_data.curr_strat_latency
    fig.plot(
        strategy_data.index,
        estimated_latency,
        marker="o",
        linestyle="-",
        label=f"Estimated average latency",
        color="grey",
    )


def graph_server_data(experiment_name: str, specific_server: int | None = None):
    clients_data, servers_data, strategy_data = get_experiment_data(experiment_name)
    print_strats(strategy_data)

    for id, server_metrics in servers_data.items():
        if specific_server is not None and specific_server != id:
            continue
        # title = f'Server {server_metrics.attrs["location"]} Metrics'
        location = location_name(server_metrics.attrs["location"])
        title = f"Server {id} ({location}) Metrics"
        fig, axs = create_base_figure(title, clients_data, strategy_data)
        fig.suptitle(title, y=0.95)
        # fig.legend(title="Configurations", bbox_to_anchor=(1.01, 1), loc='upper left')  # Legend outside plot to right

        # Graph request latencies
        client_requests = clients_data.get(id)
        if client_requests is not None:
            read_requests = client_requests.loc[client_requests["write"] == False]
            axs[0].scatter(
                read_requests.index,
                read_requests["response_latency"],
                marker="o",
                linestyle="-",
                label="Read Latency",
                color="pink",
            )
            write_requests = client_requests.loc[client_requests["write"]]
            axs[0].scatter(
                write_requests.index,
                write_requests["response_latency"],
                marker="o",
                linestyle="-",
                label="Write latency",
                color="red",
            )
        # Graph heartbeat latencies
        metric_skip = 0
        for peer in filter(lambda s: s != id, servers_data.keys()):
            peer_latencies = server_metrics["cluster_metrics.latencies"].apply(
                lambda lat: lat[id - 1][peer - 1]
            )
            peer_location = location_name(servers_data[peer].attrs["location"])
            axs[0].plot(
                server_metrics.index[metric_skip:],
                peer_latencies[metric_skip:],
                marker="o",
                linestyle="-",
                label=f"Peer {peer} ({peer_location}) latency",
                color=server_colors[peer],
            )
        # Graph estimated read/write latencies
        write_latency_estimate = server_metrics["operation_latency.write_latency"]
        read_latency_estimate = server_metrics["operation_latency.read_latency"]
        axs[0].plot(
            server_metrics.index[metric_skip:],
            write_latency_estimate[metric_skip:],
            linestyle="--",
            label=f"Estimated write latency",
            color="red",
        )
        axs[0].plot(
            server_metrics.index[metric_skip:],
            read_latency_estimate[metric_skip:],
            linestyle="--",
            label=f"Estimated read latency",
            color="pink",
        )
        # Graph workload metrics
        for server in servers_data.keys():
            a = server_metrics["cluster_metrics.workload"].apply(
                lambda x: x[server - 1]["reads"] + x[server - 1]["writes"]
            )
            axs[1].plot(
                server_metrics.index,
                a,
                marker="o",
                linestyle="-",
                color=server_colors[server],
            )

        # fig.legend(loc="upper left")
        axs[0].legend(
            title="Configurations", bbox_to_anchor=(1.01, 1), loc="upper left"
        )  # Legend outside plot to right
        plt.show()


def graph_cluster_latency(experiment_name: str):
    clients_data, servers_data, strategy_data = get_experiment_data(experiment_name)
    fig, axs = create_base_figure(
        "Cluster-wide Request Latency", clients_data, strategy_data
    )

    # Request latency
    for id, requests in clients_data.items():
        location = requests.attrs["location"]
        axs[0].scatter(
            requests.index,
            requests["response_latency"],
            marker="o",
            linestyle="-",
            label=f"client {id}",
        )
    graph_estimated_latency(axs[0], strategy_data)

    all_requests = pd.concat(clients_data.values())
    opt_latency = all_requests["response_latency"].mean()
    read_latency = (
        all_requests[all_requests["response.message.Read"].notna()][
            "response_latency"
        ].mean()
        if "response.message.Read" in all_requests.columns
        else None
    )
    write_latency = (
        all_requests[all_requests["response.message.Write"].notna()][
            "response_latency"
        ].mean()
        if "response.message.Write" in all_requests.columns
        else None
    )
    print_strats(strategy_data)
    print(opt_latency, read_latency, write_latency)

    fig.legend()
    plt.show()


def graph_optimization_comparison(
    experiment_name: str, no_optimize_experiment_name: str
):
    clients_data, _, strategy_data = get_experiment_data(experiment_name)
    clients_data_no_opt, _, _ = get_experiment_data(no_optimize_experiment_name)
    title = "Request Latency with and without optimization"
    fig, axs = create_base_figure(title, clients_data, strategy_data)

    # Request latency subplot
    request_start_delta = None
    for client_id, requests in clients_data.items():
        requests_no_opt = clients_data_no_opt[client_id]
        color = server_colors[client_id]
        location = requests.attrs["location"]
        axs[0].scatter(
            requests.index,
            requests["response_latency"],
            marker="o",
            linestyle="-",
            label=f"client {client_id}",
            color=color,
        )
        # Graph request latencies without optimization (resets x-axis to match opt)
        request_start_delta = (
            min(requests_no_opt.index) - min(requests.index)
            if request_start_delta is None
            else request_start_delta
        )
        shifted_request_times = requests_no_opt.index - request_start_delta
        axs[0].scatter(
            shifted_request_times,
            requests_no_opt["response_latency"],
            marker="*",
            linestyle="--",
            label=f"without optimization",
            color=color,
        )
    # Request rate subplot
    for client_id, requests in clients_data_no_opt.items():
        color = server_colors[client_id]
        requests.index = requests.index - request_start_delta
        request_rate = requests.resample("1s").count()
        ma = request_rate.ewm(alpha=0.9).mean()
        axs[1].plot(
            request_rate.index, ma["response_latency"], linestyle="--", color=color
        )

    fig.legend()
    plt.show()


def graph_average_latency_comparison(
    experiment_name: str,
    no_optimize_experiment_name: str,
    title: str = "Request Latency with and without optimization",
    labels: tuple[str, str] = ("with optimization", "without optimization"),
    legend_args: dict = {"loc": "upper right"},
):
    clients_data, _, strategy_data = get_experiment_data(experiment_name)
    clients_data_no_opt, _, _ = get_experiment_data(no_optimize_experiment_name)
    # Go from UTC time to experiment time
    all_requests = pd.concat(clients_data.values())
    relative_sync_time = min(all_requests.index)
    experiment_start = pd.Timestamp("20180606")
    for client_data in clients_data.values():
        client_data.index = experiment_start + (client_data.index - relative_sync_time)
    for client_data in clients_data_no_opt.values():
        client_data.index = experiment_start + (client_data.index - relative_sync_time)
    strategy_data.index = experiment_start + (strategy_data.index - relative_sync_time)
    fig, axs = create_base_figure(title, clients_data, strategy_data)

    # Combine client requests in experiment
    all_requests = pd.concat(clients_data.values())
    all_requests_no_opt = pd.concat(clients_data_no_opt.values())
    # Moving average latency with optimization
    average_latency = all_requests["response_latency"].resample("1s").mean()
    axs[0].plot(
        average_latency.index,
        average_latency.values,
        linestyle="-",
        label=labels[0],
        color=opt_color,
        linewidth=2,
    )
    # # Error bands
    # std_latency = all_requests["response_latency"].resample("1s").std()
    # axs[0].fill_between(average_latency.index,
    #                 average_latency.values - std_latency.values,  # Lower bound
    #                 average_latency.values + std_latency.values,  # Upper bound
    #                 color=opt_color, alpha=0.3)  # Adjust alpha for transparency
    # Graph strat changes on latency line
    for change_time, strat_change in strategy_data[
        strategy_data["reconfigure"]
    ].iterrows():
        for window in average_latency.rolling(2):
            if window.notnull().sum() != 2:
                continue
            start = window.index[0]
            end = window.index[1]
            if start <= change_time <= end:
                start_value = window.iloc[0]
                value_difference = window.iloc[1] - start_value
                interpolated_y = (
                    start_value
                    + value_difference * (change_time - start).total_seconds()
                )
                new_leader_id = int(strat_change["opt_strat.leader"])
                new_leader_location = location_name(server_locations[new_leader_id])
                color = server_colors[new_leader_id]
                axs[0].plot(change_time, interpolated_y, "o", color=color)
                # axs[0].plot([change_time, change_time], [interpolated_y, interpolated_y - 10], color=color, linewidth=1)
                # axs[0].annotate(
                #     text = f"switched leader to {new_leader_location}",
                #     xy = (change_time, interpolated_y),
                #     textcoords = 'offset points',
                #     # xytext = (-5, -60), # Works for round robin
                #     xytext = (-5, -67),
                #     fontsize = 13,
                #     color=color,
                # )
    # # Scatter plot
    # average_latency = all_requests["response_latency"]
    # axs[0].scatter(average_latency.index, average_latency.values, label=labels[0], color=opt_color, alpha=0.6)
    # Moving average latency without optimization
    request_start_delta = min(all_requests_no_opt.index) - min(all_requests.index)
    all_requests_no_opt["shifted_timestamp"] = (
        all_requests_no_opt.index - request_start_delta
    )
    all_requests_no_opt.set_index("shifted_timestamp", inplace=True)
    average_latency_no_opt = (
        all_requests_no_opt["response_latency"].resample("1s").mean()
    )
    axs[0].plot(
        average_latency_no_opt.index,
        average_latency_no_opt.values,
        linestyle="-",
        label=labels[1],
        color=no_opt_color,
        linewidth=2,
    )
    # # Error bands
    # std_latency = all_requests_no_opt["response_latency"].resample("1s").std()
    # axs[0].fill_between(std_latency.index,
    #                 average_latency_no_opt.values - std_latency.values,  # Lower bound
    #                 average_latency_no_opt.values + std_latency.values,  # Upper bound
    #                 color=no_opt_color, alpha=0.2)  # Adjust alpha for transparency
    fig.legend(**legend_args)
    plt.show()


def graph_average_latency_comparison3(
    experiment_name: str,
    experiment_name_no_opt: str,
    experiment_name_no_opt2: str,
    title: str = "Request Latency with and without optimization",
    labels: tuple[str, str, str] = (
        "with optimization",
        "without optimization1",
        "without optimization2",
    ),
    show_read_ratio: bool = False,
    legend_args: dict = {"loc": "upper right"},
):
    clients_data, _, strategy_data = get_experiment_data(experiment_name)
    clients_data_no_opt, _, _ = get_experiment_data(experiment_name_no_opt)
    clients_data_no_opt2, _, _ = get_experiment_data(experiment_name_no_opt2)
    # Go from UTC time to experiment time
    all_requests = pd.concat(clients_data.values())
    relative_sync_time = min(all_requests.index)
    experiment_start = pd.Timestamp("20180606")
    for client_data in clients_data.values():
        client_data.index = experiment_start + (client_data.index - relative_sync_time)
    for client_data in clients_data_no_opt.values():
        client_data.index = experiment_start + (client_data.index - relative_sync_time)
    for client_data in clients_data_no_opt2.values():
        client_data.index = experiment_start + (client_data.index - relative_sync_time)
    strategy_data.index = experiment_start + (strategy_data.index - relative_sync_time)

    if show_read_ratio:
        fig, axs = create_read_ratio_figure(title, clients_data, strategy_data)
    else:
        fig, axs = create_base_figure(title, clients_data, strategy_data)

    # Combine client requests in experiment
    all_requests = pd.concat(clients_data.values())
    all_requests_no_opt = pd.concat(clients_data_no_opt.values())
    all_requests_no_opt2 = pd.concat(clients_data_no_opt2.values())
    # Moving average latency
    average_latency = all_requests["response_latency"].resample("1s").mean()
    axs[0].plot(
        average_latency.index,
        average_latency.values,
        linestyle="-",
        label=labels[0],
        color=opt_color,
        linewidth=2,
    )
    # Graph strat changes on latency line
    for change_time, strat_change in strategy_data[
        strategy_data["new_strat"]
    ].iterrows():
        for window in average_latency.rolling(2):
            if window.notnull().sum() != 2:
                continue
            start = window.index[0]
            end = window.index[1]
            if start <= change_time <= end:
                start_value = window.iloc[0]
                value_difference = window.iloc[1] - start_value
                interpolated_y = (
                    start_value
                    + value_difference * (change_time - start).total_seconds()
                )
                new_leader = int(strat_change["leader"])
                color = server_colors[new_leader]
                axs[0].plot(change_time, interpolated_y, "o", color=color)
    # Moving average latency no opt1
    request_start_delta = min(all_requests_no_opt.index) - min(all_requests.index)
    all_requests_no_opt["shifted_timestamp"] = (
        all_requests_no_opt.index - request_start_delta
    )
    all_requests_no_opt.set_index("shifted_timestamp", inplace=True)
    average_latency_no_opt = (
        all_requests_no_opt["response_latency"].resample("1s").mean()
    )
    axs[0].plot(
        average_latency_no_opt.index,
        average_latency_no_opt.values,
        linestyle="-",
        label=labels[1],
        color=no_opt_color,
        linewidth=2,
        alpha=0.5,
    )
    # Moving average latency no opt2
    request_start_delta2 = min(all_requests_no_opt2.index) - min(all_requests.index)
    all_requests_no_opt2["shifted_timestamp"] = (
        all_requests_no_opt2.index - request_start_delta2
    )
    all_requests_no_opt2.set_index("shifted_timestamp", inplace=True)
    average_latency_no_opt2 = (
        all_requests_no_opt2["response_latency"].resample("1s").mean()
    )
    axs[0].plot(
        average_latency_no_opt2.index,
        average_latency_no_opt2.values,
        linestyle="-",
        label=labels[2],
        color=no_opt2_color,
        linewidth=2,
        alpha=0.5,
    )
    fig.legend(**legend_args)
    # plt.savefig("read_ratio.svg", bbox_inches="tight")
    plt.show()


def graph_latency_estimation_comparison(experiment_name: str):
    clients_data, _, strategy_data = get_experiment_data(experiment_name)
    title = "Average request latency vs. optimizer predicted latency"
    fig, axs = create_base_figure(title, clients_data, strategy_data)
    print_strats(strategy_data)

    # Combine client requests in experiment
    all_requests = pd.concat(clients_data.values())
    # Moving average latency
    average_latency = all_requests["response_latency"].resample("500ms").mean()
    axs[0].plot(
        average_latency.index,
        average_latency.values,
        linestyle="-",
        label="with optimization",
        color=opt_color,
    )
    # Estimated average latency
    graph_estimated_latency(axs[0], strategy_data)

    fig.legend()
    plt.show()


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


def graph_average_latency_bar_chart(
    experiment_name: str,
    no_optimize_experiment_name: str,
    title: str = "Request Latency with and without optimization",
    labels: tuple[str, str] = ("with optimization", "without optimization"),
    legend_args: dict = {"loc": "upper left", "ncols": 1},
):
    clients_data, _, strategy_data = get_experiment_data(experiment_name)
    clients_data_no_opt, _, _ = get_experiment_data(no_optimize_experiment_name)

    # Combine client requests in experiment
    all_requests = pd.concat(clients_data.values())
    all_requests_no_opt = pd.concat(clients_data_no_opt.values())
    opt_avg, opt_std = (get_averages(all_requests), get_std_devs(all_requests))
    no_opt_avg, no_opt_std = (
        get_averages(all_requests_no_opt),
        get_std_devs(all_requests_no_opt),
    )

    bar_group_labels = [
        "Write Latency\nAverage",
        "Read Latency\nAverage",
        "Total Latency\nAverage",
    ]
    latency_means = {
        labels[0]: (opt_avg, opt_std, opt_color),
        labels[1]: (no_opt_avg, no_opt_std, no_opt_color),
    }
    print(latency_means)
    _, _ = create_base_barchart(latency_means, bar_group_labels, legend_args)
    plt.show()


def graph_average_latency_bar_chart3(
    experiment_name: str,
    experiment_name_no_opt: str,
    experiment_name_no_opt2: str,
    title: str = "Request Latency with and without optimization",
    labels: tuple[str, str, str] = (
        "with optimization",
        "without optimization1",
        "without optimization2",
    ),
    legend_args: dict = {"loc": "upper left", "ncols": 1},
):
    clients_data, _, strategy_data = get_experiment_data(experiment_name)
    clients_data_no_opt, _, _ = get_experiment_data(experiment_name_no_opt)
    clients_data_no_opt2, _, _ = get_experiment_data(experiment_name_no_opt2)

    # Combine client requests in experiment
    all_requests = pd.concat(clients_data.values())
    opt_avg, opt_std = (get_averages(all_requests), get_std_devs(all_requests))
    all_requests_no_opt = pd.concat(clients_data_no_opt.values())
    no_opt_avg, no_opt_std = (
        get_averages(all_requests_no_opt),
        get_std_devs(all_requests_no_opt),
    )
    all_requests_no_opt2 = pd.concat(clients_data_no_opt2.values())
    no_opt2_avg, no_opt2_std = (
        get_averages(all_requests_no_opt2),
        get_std_devs(all_requests_no_opt2),
    )

    bar_group_labels = [
        "Write Latency\nAverage",
        "Read Latency\nAverage",
        "Total Latency\nAverage",
    ]
    latency_means = {
        labels[0]: (opt_avg, opt_std, opt_color),
        labels[1]: (no_opt_avg, no_opt_std, no_opt_color),
        labels[2]: (no_opt2_avg, no_opt2_std, no_opt2_color),
    }
    _, _ = create_base_barchart(latency_means, bar_group_labels, legend_args)
    plt.show()


def graph_intro_bar_chart(
    title: str, legend_labels: tuple[str, str], bar_group_labels: tuple[str, str]
):
    baseline_latencies = []
    opt_latencies = []
    for experiment in ["intro-b", "intro-c"]:  # , "intro-d"]:
        baseline_experiment = experiment + "/baseline"
        clients_data, _, _ = get_experiment_data(baseline_experiment)
        latency = pd.concat(clients_data.values())["response_latency"].mean()
        baseline_latencies.append(latency)
        opt_experiment = experiment + "/opt"
        clients_data, _, _ = get_experiment_data(opt_experiment)
        latency = pd.concat(clients_data.values())["response_latency"].mean()
        opt_latencies.append(latency)
    latency_means = {
        legend_labels[0]: (opt_latencies, [None, None], "tab:orange"),
        legend_labels[1]: (baseline_latencies, [None, None], "tab:blue"),
    }
    _, _ = create_base_barchart(latency_means, list(bar_group_labels))
    plt.show()


def graph_read_strats_bar_chart(
    title: str,
    experiment: str,
    strats: list[str],
    labels: list[str],
    colors: list[str],
    legend_args: dict = {"loc": "upper right", "ncols": 1, "fontsize": 16},
):
    # "auto", ([avg for each group], [std for each group], "orange")
    # "BQR", ([avg for each city], [std for each city], "orange")
    latency_means = {}
    bar_group_labels = [""] * 5
    for strat, label, color in zip(strats, labels, colors):
        experiment_name = experiment + "/" + strat
        clients_data, _, _ = get_experiment_data(experiment_name)
        strat_avgs = [0.0] * 5
        strat_stds = [0.0] * 5
        for client, client_requests in clients_data.items():
            location = location_name(client_requests.attrs["location"])
            _, read_avg, _ = get_averages(client_requests)
            _, read_std, _ = get_std_devs(client_requests)
            strat_avgs[client - 1] = read_avg
            strat_stds[client - 1] = read_std
            bar_group_labels[client - 1] = location
        latency_means[label] = (strat_avgs, strat_stds, color)
    _, _ = create_base_barchart(
        latency_means, bar_group_labels, legend_args, error_bars=True
    )
    plt.show()


# def graph_local_bench():
#     experiment_name = "test-local"
#     # graph_server_data(experiment_name)
#     # graph_latency_estimation_comparison(experiment_name)
#     graph_cluster_latency(experiment_name)


def graph_round_robin_bench():
    experiment_directory = "round-robin"
    experiment_name = experiment_directory + "/optimize-True"
    experiment_name_no_opt = experiment_directory + "/optimize-False"
    # graph_server_data(experiment_name)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    title = "Request Latency With and Without Leader Placement Optimization"
    labels = ("AutoQuorum", "[Madrid, (2,2), RAW]")
    labels = ("AutoQuorum", "Static Madrid Leader")  # Presentation Labels
    legend_args = {
        "loc": "upper left",
        "bbox_to_anchor": (0.09, 0.99),
        "fontsize": 12,
        "ncols": 1,
    }
    graph_average_latency_comparison(
        experiment_name, experiment_name_no_opt, title, labels, legend_args
    )
    graph_average_latency_bar_chart(
        experiment_name, experiment_name_no_opt, title, labels
    )


# def graph_round_robin_bench():
#     experiment_directory = "round-robin-2"
#     experiment_name = experiment_directory + "/opt"
#     experiment_name_no_opt = experiment_directory + "/no-opt"
#     # graph_server_data(experiment_name)
#     # graph_latency_estimation_comparison(experiment_name)
#     # graph_cluster_latency(experiment_name)
#     # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
#     title = 'Request Latency With and Without Leader Placement Optimization'
#     labels = ("AutoQuorum", "[Madrid, (2,2), RAW]")
#     labels = ("AutoQuorum", "Static Madrid Leader") # Presentation Labels
#     legend_args = {"loc": 'upper left', "bbox_to_anchor": (0.09, 0.99), "fontsize": 12, "ncols": 1}
#     graph_average_latency_comparison(experiment_name, experiment_name_no_opt, title, labels, legend_args)
#     graph_average_latency_bar_chart(experiment_name, experiment_name_no_opt, title, labels)


def graph_shifting_load_bench():
    experiment_directory = "shifting-load-2"
    experiment_name = experiment_directory + "/opt"
    experiment_name_no_opt = experiment_directory + "/no-opt"
    # graph_server_data(experiment_name)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    title = "Request Latency With and Without Leader Placement Optimization"
    labels = ("AutoQuorum", "[Madrid, (2,2), RAW]")
    labels = ("AutoQuorum", "Static Madrid Leader")  # Presentation Labels
    legend_args = {"loc": "upper left", "bbox_to_anchor": (0.09, 0.99), "fontsize": 12}
    graph_average_latency_comparison(
        experiment_name, experiment_name_no_opt, title, labels, legend_args
    )
    graph_average_latency_bar_chart(
        experiment_name, experiment_name_no_opt, title, labels
    )


def graph_dynamic_flex_bench():
    experiment_directory = "dynamic-flex"
    experiment_name = experiment_directory + "/opt"
    experiment_name_no_opt = experiment_directory + "/no-opt"
    # graph_server_data(experiment_name_no_opt)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    graph_average_latency_comparison(experiment_name, experiment_name_no_opt)
    graph_average_latency_bar_chart(experiment_name, experiment_name_no_opt)


def graph_read_heavy_bench():
    experiment_directory = "read-heavy"
    experiment_name = experiment_directory + "/opt"
    experiment_name_no_opt = experiment_directory + "/raw"
    # graph_server_data(experiment_name)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    title = "Quorum Size Effect on 90-10 Read-Write Ratio Workload"
    labels = (
        "AutoQuorum: Leader=N. Virginia, ReadStrat=BQR, RQ=2, WQ=4",
        "StaticConfig: Leader=N. Virginia, ReadStrat=RAW, RQ=4, WQ=2",
    )
    graph_average_latency_comparison(
        experiment_name, experiment_name_no_opt, title, labels
    )
    graph_average_latency_bar_chart(
        experiment_name, experiment_name_no_opt, title, labels
    )


def graph_read_ratio_bench():
    experiment_directory = "read-ratio"
    experiment_name = experiment_directory + "/opt"
    experiment_name_no_opt = experiment_directory + "/small-read-quorum"
    experiment_name_no_opt2 = experiment_directory + "/small-write-quorum"
    # graph_server_data(experiment_name)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    # labels = ("AutoQuorum", "[NV, (2,4), BQR]", "[NV, (4,2), BQR]")
    labels = (
        "AutoQuorum",
        "Read-optimized Strategy",
        "Write-optimized Strategy",
    )  # Presentation labels
    # legend_args = {"loc": 'upper right', "bbox_to_anchor": (0.124, 0.88)}
    legend_args = {
        "loc": "upper right",
        "bbox_to_anchor": (0.98, 0.99),
        "ncols": 1,
        "fontsize": 11,
    }
    graph_average_latency_comparison3(
        experiment_name,
        experiment_name_no_opt,
        experiment_name_no_opt2,
        "",
        labels,
        show_read_ratio=True,
        legend_args=legend_args,
    )
    graph_average_latency_bar_chart3(
        experiment_name,
        experiment_name_no_opt,
        experiment_name_no_opt2,
        "",
        labels,
        legend_args=legend_args,
    )


def graph_intro_bench():
    experiment_directory = "intro-d"
    experiment_name = experiment_directory + "/opt"
    # graph_server_data(experiment_name)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    # legend_labels = ("StaticConfig: Leader=Dallas\n                     Quorum=Majority", "Optimal Configuration")
    legend_labels = (
        "Optimal Configuration",
        "Static Configuration",
    )  # Presentation labels
    bar_group_labels = ("Hotspot\nShift", "Node\nFailure")
    graph_intro_bar_chart("", legend_labels, bar_group_labels)
    # legend_labels = ("StaticConfig: Leader=N. Virginia\n                     Quorum=Majority", "Optimal Configuration")
    legend_labels = (
        "Optimal Configuration",
        "Static Configuration",
    )  # Presentation labels
    legend_args = {"loc": "upper right", "ncols": 1, "fontsize": 14}
    graph_average_latency_bar_chart(
        "intro-d-2/opt", "intro-d-2/baseline", "", legend_labels, legend_args
    )


def graph_read_strats_bench():
    experiment_directory = "read-strats-2"
    experiment_name = experiment_directory + "/bread"
    experiment_name_no_opt = experiment_directory + "/qread"
    experiment_name_no_opt2 = experiment_directory + "/raw"
    # graph_server_data(experiment_name)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    title = "Read Strategy Effect on 90-10 Read-Write Ratio Workload"
    # labels = ("[D, (2,4), BQR]", "[D, (2,4), QuorumRead]", "[NV, (4,2), RAW]")
    labels = (
        "Read-optimized Strat w/ BQR",
        "Read-optimized Strat w/ Quorum Read",
        "Write-optimized Strat",
    )  # Presentation labels
    # graph_average_latency_comparison3(experiment_name, experiment_name_no_opt, experiment_name_no_opt2, title, labels)
    legend_args = {"loc": "upper right", "ncols": 1, "fontsize": 14}
    graph_average_latency_bar_chart3(
        experiment_name,
        experiment_name_no_opt,
        experiment_name_no_opt2,
        title,
        labels,
        legend_args,
    )
    title = "Read Strategy Latency When Leader=Dallas, RQ=2, WQ=4"
    # labels = ["[D, (2,4), QuorumRead]", "[D, (2,4), BQR]"]
    labels = (
        "Read-optimized Strat w/ BQR",
        "Read-optimized Strat w/ Quorum Read",
    )  # Presentation labels
    strats = ["bread", "qread"]
    colors = ["tab:orange", "tab:blue"]
    legend_args = {"loc": "upper right", "ncols": 1, "fontsize": 16}
    graph_read_strats_bar_chart(
        title, experiment_directory, strats, labels, colors, legend_args
    )


def graph_mixed_strat_bench():
    experiment_directory = "mixed-strat-no-rinse"
    experiment_name = experiment_directory + "/opt"
    experiment_name_no_opt = experiment_directory + "/raw"
    experiment_name_no_opt2 = experiment_directory + "/bread"
    # graph_server_data(experiment_name_no_opt)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    # graph_average_latency_comparison3(experiment_name, experiment_name_no_opt, experiment_name_no_opt2, title, labels)
    title = "Global vs. Per-node Read Strategy Effect on 90-10 Read-Write Workload"
    # labels = ("[LA, (4,2), MIX]", "[LA, (4,2), RAW]", "[LA, (4,2), BQR]")
    labels = (
        "Mixed Read Strategy",
        "Only RAW Reads",
        "Only BQR Reads",
    )  # Presentation labels
    legend_args = {
        "loc": "upper left",
        "ncols": 1,
        "fontsize": 14,
    }  # "bbox_to_anchor": (0.124, 0.88)}
    graph_average_latency_bar_chart3(
        experiment_name,
        experiment_name_no_opt,
        experiment_name_no_opt2,
        title,
        labels,
        legend_args,
    )
    title = "Read Strategy Latency When Leader=LA, RQ=4, WQ=2"
    # labels = ["[LA, (4,2), RAW]", "[LA, (4,2), BQR]"]
    labels = ("Only RAW Reads", "Only BQR Reads")  # Presentation labels
    strats = ["raw", "bread"]
    colors = ["tab:blue", "tab:green"]
    legend_args = {"loc": "upper left", "ncols": 1, "fontsize": 16}
    graph_read_strats_bar_chart(
        title, experiment_directory, strats, labels, colors, legend_args
    )


def graph_reconfig_overhead_bench():
    experiment_directory = "reconfig-overhead"
    experiment_name_jc = experiment_directory + "/joint-consensus"
    experiment_name_ss = experiment_directory + "/stopsign"
    graph_server_data(experiment_name_ss)


def graph_test():
    experiment_name = "local-run"
    client_data, server_data, strat_data = get_experiment_data(
        experiment_name, dump_server_df=True
    )
    data = client_data[2]
    relative_sync_time = min(data.index)
    experiment_start = pd.Timestamp("20180606")
    data.index = experiment_start + (data.index - relative_sync_time)
    plt.plot(data.index, data.response_latency)
    plt.show()


def main():
    graph_test()
    # graph_local_bench()
    # graph_round_robin_bench()
    # graph_shifting_load_bench()
    # graph_dynamic_flex_bench()
    # graph_read_heavy_bench()
    # graph_read_ratio_bench()
    # graph_intro_bench()
    # graph_read_strats_bench()
    # graph_mixed_strat_bench()
    # graph_reconfig_overhead_bench()
    pass


if __name__ == "__main__":
    server_colors = {}
    server_locations = {}
    opt_color = "tab:orange"
    no_opt_color = "tab:blue"
    no_opt2_color = "tab:green"
    # opt_color = "tab:gray"
    # no_opt_color = "tab:orange"
    # no_opt2_color = "tab:blue"
    bar_colors = (opt_color, no_opt_color, no_opt2_color)
    main()
