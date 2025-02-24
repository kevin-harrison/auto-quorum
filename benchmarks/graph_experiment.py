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
    print("parsing", output_file)
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
    try:
        client_data = pd.read_csv(
            output_file,
            names=["request_time", "write", "response_time"],
            header=0,
            dtype={"write": "bool"},
        )
    except pd.errors.EmptyDataError:
        client_data = pd.DataFrame(columns=["request_time", "write", "response_time"])
    client_data["response_latency"] = (
        client_data.response_time - client_data.request_time
    )
    client_data = client_data.astype(dtype={"request_time": "datetime64[ms]"})
    client_data.set_index("request_time", inplace=True)
    with open(summary_file, "r") as file:
        client_summary = json.load(file)
    if client_summary["sync_time"] is not None:
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
        server_data = parse_server_log(server_log_file, dump_server_df)
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


def location_name(location: str, leader: bool = False) -> str:
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
    return name + "*" if leader else name


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
    # graph_strategy_changes(axs[0], strategy_data)
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
    # all_requests = pd.concat(clients_data.values())
    # total_request_rate = all_requests.resample("1s").count()
    # total_request_rate = total_request_rate.loc[
    #     total_request_rate.response_latency != 0
    # ]

    total_request_rate = pd.DataFrame()
    for requests in clients_data.values():
        request_rate = requests.resample("3s").count()
        total_request_rate = total_request_rate.add(request_rate, fill_value=0)
    total_request_rate = total_request_rate[
        (total_request_rate["response_latency"] != 0)
    ]
    fig.set_xlim(
        left=total_request_rate.index.min(), right=total_request_rate.index.max()
    )
    # TODO: doesn't work, need to call draw first?
    # fig.figure.canvas.draw() # doesn't work
    # plt.setp(fig.get_xticklabels()[0], visible=False)
    # # or
    # fig.set_xticks(fig.get_xticks()[1:])
    for id, requests in clients_data.items():
        # request_rate = requests.resample("3s").count()
        request_rate = requests.resample("3s").count() / total_request_rate
        request_rate.fillna(0, inplace=True)
        ma = request_rate.ewm(alpha=0.9).mean()
        color = location_color(requests.attrs["location"])
        label = location_name(requests.attrs["location"])
        fig.plot(
            request_rate.index,
            ma.response_latency,
            linestyle="-",
            color=color,
            linewidth=1,
        )
        fig.fill_between(
            request_rate.index, ma.response_latency, color=color, alpha=0.3
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
    ax,
    latency_means: dict,
    bar_group_labels: list[str],
    error_bars: bool = False,
):
    axis_label_size = 20
    axis_tick_size = 12
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
        # # Graph estimated read/write latencies
        # write_latency_estimate = server_metrics["operation_latency.write_latency"]
        # read_latency_estimate = server_metrics["operation_latency.read_latency"]
        # axs[0].plot(
        #     server_metrics.index[metric_skip:],
        #     write_latency_estimate[metric_skip:],
        #     linestyle="--",
        #     label=f"Estimated write latency",
        #     color="red",
        # )
        # axs[0].plot(
        #     server_metrics.index[metric_skip:],
        #     read_latency_estimate[metric_skip:],
        #     linestyle="--",
        #     label=f"Estimated read latency",
        #     color="pink",
        # )
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
    epoch_start = pd.Timestamp("20180606")
    all_requests = pd.concat(clients_data.values())
    start = min(all_requests.index)
    for client_data in clients_data.values():
        client_data.index = epoch_start + (client_data.index - start)
    strategy_data.index = epoch_start + (strategy_data.index - start)

    all_requests_no_opt = pd.concat(clients_data_no_opt.values())
    start = min(all_requests_no_opt.index)
    for client_data in clients_data_no_opt.values():
        client_data.index = epoch_start + (client_data.index - start)

    # # Go from UTC time to experiment time
    # all_requests = pd.concat(clients_data.values())
    # relative_sync_time = min(all_requests.index)
    # experiment_start = pd.Timestamp("20180606")
    # for client_data in clients_data.values():
    #     client_data.index = experiment_start + (client_data.index - relative_sync_time)
    # for client_data in clients_data_no_opt.values():
    #     client_data.index = experiment_start + (client_data.index - relative_sync_time)
    # strategy_data.index = experiment_start + (strategy_data.index - relative_sync_time)

    fig, axs = graph_average_latency_comparison_base(
        clients_data, clients_data_no_opt, strategy_data, title, labels, legend_args
    )
    plt.show()


def graph_average_latency_comparison_base(
    clients_data: dict[int, pd.DataFrame],
    clients_data_no_opt: dict[int, pd.DataFrame],
    strategy_data: pd.DataFrame,
    title: str = "Request Latency with and without optimization",
    labels: tuple[str, str] = ("with optimization", "without optimization"),
    legend_args: dict = {"loc": "upper right"},
):
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
        all_requests_no_opt["response_latency"].resample("500ms").mean()
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
    return fig, axs


def graph_average_latency_comparison_all(
    autoquorum_experiment_dir: str,
    other_experiments: list[tuple[str, str]],  # (name, dir)
    experiment_labels: dict[str, str],
    legend_args: dict,
    multileader_experiment: str | None = None,
):
    clients_data, _, strategy_data = get_experiment_data(autoquorum_experiment_dir)
    # Go from UTC time to experiment time
    epoch_start = pd.Timestamp("20180606")
    all_requests = pd.concat(clients_data.values())
    start = min(all_requests.index)
    for client_data in clients_data.values():
        client_data.index = epoch_start + (client_data.index - start)
    strategy_data.index = epoch_start + (strategy_data.index - start)
    all_requests = pd.concat(clients_data.values())
    print_strats(strategy_data)

    # Plot AutoQuorum data
    fig, axs = create_base_figure("", clients_data, strategy_data)
    # Moving average latency
    average_latency = all_requests["response_latency"].resample("3s").mean()
    axs[0].plot(
        average_latency.index,
        average_latency.values,
        linestyle="-",
        label="AutoQuorum",
        # marker=strat_markers["AutoQuorum"],
        color=strat_colors["AutoQuorum"],
        linewidth=2,
    )
    # Graph strat changes on latency line
    for change_time, strat_change in strategy_data[
        strategy_data["reconfigure"]
    ].iterrows():
        for window in average_latency.rolling(2):
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
                new_leader = int(strat_change["opt_strat.leader"])
                color = server_colors[new_leader]
                axs[0].plot(change_time, interpolated_y, "o", color=color)
    # # Graph strat changes on latency line
    # for change_time, strat_change in strategy_data[strategy_data["reconfigure"]].iterrows():
    #     for window in average_latency.rolling(2):
    #         if window.notnull().sum() != 2:
    #             continue
    #         start = window.index[0]
    #         end = window.index[1]
    #         if start <= change_time <= end:
    #             start_value = window.iloc[0]
    #             value_difference = window.iloc[1] - start_value
    #             interpolated_y = (
    #                 start_value
    #                 + value_difference * (change_time - start).total_seconds()
    #             )
    #             new_leader = int(strat_change["opt_strat.leader"])
    #             color = server_colors[new_leader]
    #             axs[0].plot(change_time, interpolated_y, "o", color=color)

    # Plot other experiment data
    for experiment_name, experiment_dir in other_experiments:
        experiment_clients_data, _, _ = get_experiment_data(experiment_dir)
        all_requests_other = pd.concat(experiment_clients_data.values())
        start = min(all_requests_other.index)
        all_requests_other.index = epoch_start + (all_requests_other.index - start)
        average_latency = all_requests_other["response_latency"].resample("3s").mean()
        axs[0].plot(
            average_latency.index,
            average_latency,
            linestyle="-",
            label=experiment_labels[experiment_name],
            color=strat_colors[experiment_name],
            # marker=strat_markers[experiment_name],
            linewidth=2,
            # alpha=0.5,
        )

    # Plot MultiLeader experiment data
    if multileader_experiment is not None:
        multileader_clients_data, _, _ = get_experiment_data(multileader_experiment)
        all_requests_multi = pd.concat(multileader_clients_data.values())
        start = min(all_requests_multi.index)
        all_requests_multi.index = epoch_start + (all_requests_multi.index - start)
        multi_latency = all_requests_multi["response_latency"].resample("3s").mean()
        axs[0].plot(
            multi_latency.index,
            multi_latency,
            linestyle="-",
            label="EPaxos fast path",
            color=strat_colors["EPaxos fast path"],
            # marker=strat_markers["EPaxos fast path"],
            linewidth=2,
            # alpha=0.5,
        )
        # # Slow path
        # axs[0].plot(
        #     multi_latency.index,
        #     2 * multi_latency,
        #     linestyle="--",
        #     label="EPaxos slow path",
        #     color=strat_colors["EPaxos slow path"],
        #     # marker=strat_markers["EPaxos slow path"],
        #     linewidth=2,
        #     # alpha=0.5,
        # )
    # fig.legend(ncols=2, loc="upper center", bbox_to_anchor=(0.5, 1.0), frameon=False)
    fig.legend(**legend_args)
    fig.tight_layout()


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
    legend_args: dict = {"loc": "upper right"},
    show_read_ratio: bool = False,
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

    if show_read_ratio == True:
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
    fig,
    experiments_data: list[tuple[str, str, str]],  # name, dir, label
):
    bar_group_labels = [
        "Write Latency\nAverage",
        "Read Latency\nAverage",
        "Total Latency\nAverage",
    ]
    latency_means = {}
    print(experiments_data)
    for strat, experiment_dir, label in experiments_data:
        clients_data, _, _ = get_experiment_data(experiment_dir)
        all_requests = pd.concat(clients_data.values())
        avg = get_averages(all_requests)
        std_dev = get_std_devs(all_requests)
        if label == "EPaxos":
            latency_means["EPaxos fast path"] = (
                avg,
                std_dev,
                strat_colors[strat],
                strat_hatches[strat],
            )
            slow_avg = (2 * a for a in avg)
            slow_std_dev = (2 * s for s in std_dev)
            latency_means["EPaxos slow path"] = (
                slow_avg,
                slow_std_dev,
                strat_colors["EPaxos slow path"],
                strat_hatches["EPaxos slow path"],
            )
        else:
            latency_means[label] = (
                avg,
                std_dev,
                strat_colors[strat],
                strat_hatches[strat],
            )
    create_base_barchart(fig, latency_means, bar_group_labels)


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
    fig,
    experiment: str,
    strats: list[str],
    labels: list[str],
):
    latency_means = {}
    bar_group_labels = [""] * 5
    for strat, label in zip(strats, labels):
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
        latency_means[label] = (
            strat_avgs,
            strat_stds,
            strat_colors[strat],
            strat_hatches[strat],
        )
    create_base_barchart(fig, latency_means, bar_group_labels, error_bars=True)


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


def graph_round_robin_bench_w_multileader():
    experiment_directory = "round-robin-w-multileader"
    experiment_name = experiment_directory + "/autoquorum"
    experiment_name_no_opt = experiment_directory + "/baseline"
    experiment_name_no_opt2 = experiment_directory + "/multileader"
    # graph_server_data(experiment_name)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    title = "Round Robin Benchmark"
    labels = {
        "AutoQuorum": "AutoQuorum",
        "no_opt": "Static Madrid Leader",
        "no_opt2": "MultiLeader",
    }
    colors = {
        "AutoQuorum": (autoquorum_color, autoquorum_marker),
        "no_opt": (wread_color, wread_marker),
        "no_opt2": (epaxos_color, epaxos_marker),
    }
    legend_args = {
        "loc": "upper left",
        "bbox_to_anchor": (0.095, 0.5),
        "fontsize": 12,
        "ncols": 2,
        "frameon": False,
    }
    graph_average_latency_comparison_all(
        experiment_name,
        [
            ("no_opt", experiment_name_no_opt)
        ],  # , ("no_opt2", experiment_name_no_opt2)],
        labels,
        colors,
        legend_args,
        multileader_experiment=experiment_name_no_opt2,
    )
    plt.savefig("logs/autoquorum-to-show/round-robin-3.svg", format="svg")
    plt.show()
    plt.close()
    # graph_average_latency_bar_chart(
    #     experiment_name, experiment_name_no_opt, title, labels
    # )


def graph_round_robin5_bench():
    experiment_directory = "round-robin-5"
    autoquorum_dir = experiment_directory + "/AutoQuorum"
    baseline_dir = experiment_directory + "/Baseline"
    multileader_dir = experiment_directory + "/MultiLeader-SuperMajority"
    labels = {"AutoQuorum": "AutoQuorum", "Baseline": "Static LA Leader"}
    legend_args = {
        "loc": "upper left",
        "bbox_to_anchor": (0.099, 0.99),
        "fontsize": 12,
        "ncols": 1,
        "frameon": False,
    }
    graph_average_latency_comparison_all(
        autoquorum_dir,
        [("Baseline", baseline_dir)],
        labels,
        legend_args,
        multileader_experiment=multileader_dir,
    )
    plt.savefig("logs/autoquorum-to-show/round-robin-5.svg", format="svg")
    plt.show()
    plt.close()


def graph_shifting_conditions_debug():
    period = 1
    opt = "baseline"
    opt = "autoquorum"
    experiment_dir = f"shifting-conditions/period-{period}/{opt}"
    graph_server_data(experiment_dir)


def graph_shifting_conditions_bench():
    # Stitch together periods into a single dataframe with continuous time
    all_data = []
    epoch_start = pd.Timestamp("20180606")
    for opt in [
        "baseline",
        "autoquorum",
        "multileader-supermajority",
        "multileader-majority",
    ]:
        period_time = epoch_start
        # for period in [1, 2, 3, 4]:
        for period in [1, 2, 4]:
            experiment_dir = f"shifting-conditions/period-{period}/{opt}"
            clients_data, _, _ = get_experiment_data(experiment_dir)
            period_dur = pd.Timedelta(
                seconds=clients_data[1].attrs["requests"][0]["duration_sec"]
            )
            requests = pd.concat(clients_data.values())
            period_start = min(requests.index)
            for id, data in clients_data.items():
                data.index = period_time + (data.index - period_start)
                data["period"] = period
                data["client"] = id
                data["cluster_type"] = opt
                all_data.append(data)
            period_time += period_dur
    df = pd.concat(all_data)
    df.dropna(inplace=True)

    plt.figure(figsize=(10, 6))
    resampled = df.groupby("cluster_type").resample("1000ms").mean(numeric_only=True)
    cluster_type_colors = {
        "baseline": "tab:blue",
        "autoquorum": "tab:orange",
    }
    cluster_type_labels = {
        "baseline": "Baseline",
        "autoquorum": "AutoQuorum",
    }
    for cluster_type, group in resampled.groupby("cluster_type"):
        request_times = group.index.get_level_values("request_time")
        response_latency = group["response_latency"]
        if cluster_type == "multileader-majority":
            continue
            plt.plot(
                request_times,
                response_latency,
                label="EPaxos fast path",
                linewidth=2,
                color=strat_colors["EPaxos fast path"],
            )
            plt.plot(
                request_times,
                2 * response_latency,
                label="EPaxos slow path",
                linewidth=2,
                linestyle="--",
                color=strat_colors["EPaxos slow path"],
            )
        elif cluster_type == "multileader-supermajority":
            plt.plot(
                request_times,
                response_latency,
                label="EPaxos fast path",
                linewidth=2,
                color=strat_colors["EPaxos fast path"],
            )
            # plt.plot(
            #     request_times,
            #     response_latency,
            #     label="FPaxos fast path",
            #     linewidth=2,
            #     color="tab:purple",
            # )
        else:
            plt.plot(
                request_times,
                response_latency,
                label=cluster_type_labels[cluster_type],
                linewidth=2,
                color=cluster_type_colors[cluster_type],
            )
    # # Estimate Fast Paxos
    # multileader_data = resampled.loc[
    #     ["multileader-majority", "multileader-supermajority"], :
    # ]
    # fpaxos_worst_case = multileader_data.groupby("request_time")[
    #     "response_latency"
    # ].sum()
    # print(fpaxos_worst_case)
    # plt.plot(
    #     fpaxos_worst_case.index,
    #     fpaxos_worst_case,
    #     label="FPaxos slow path",
    #     linewidth=2,
    #     color="tab:purple",
    #     linestyle="--",
    # )

    plt.xlabel("Experiment Time", fontsize=12)
    plt.ylabel("Average Request Latency (ms)", fontsize=12)
    plt.ylim(bottom=0)
    # plt.xlim(right=df.index.max())
    plt.legend(ncols=2, loc="upper center", bbox_to_anchor=(0.5, 1.12), frameon=False)
    plt.tight_layout()
    plt.savefig("logs/autoquorum-to-show/shifting-conditions.svg", format="svg")
    plt.show()
    plt.close()


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
    fig = graph_average_latency_comparison(
        experiment_name, experiment_name_no_opt, title, labels, legend_args
    )

    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    title = "Request Latency With and Without Leader Placement Optimization"
    fig.suptitle(title)
    graph_average_latency_bar_chart(ax, experiment_name, experiment_name_no_opt, labels)
    ax.legend(loc="upper left", bbox_to_anchor=(0.09, 0.99), fontsize=12)


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


def graph_read_strats_bench():
    experiment_directory = "read-strats"
    bread_dir = experiment_directory + "/BallotRead"
    qread_dir = experiment_directory + "/QuorumRead"
    wread_dir = experiment_directory + "/ReadAsWrite"
    autoquorum_dir = experiment_directory + "/AutoQuorum"
    epaxos_dir = experiment_directory + "/EPaxos"
    # # graph_server_data(experiment_name)
    # # graph_latency_estimation_comparison(experiment_name)
    # # graph_cluster_latency(experiment_name)
    # # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    # # graph_average_latency_comparison3(experiment_name, experiment_name_no_opt, experiment_name_no_opt2, title, labels)

    # Overview bar chart
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    fig.suptitle(f"Read Strategy Effect on 90% Read Ratio Workload")
    experiment_data = [
        # ("BallotRead", bread_dir, "[D, (2,4), DQR]"),
        # ("QuorumRead", qread_dir, "[D, (2,4), QuorumRead]"),
        # ("ReadAsWrite", wread_dir, "[D, (4,2), RAW]"),
        ("AutoQuorum", autoquorum_dir, "AutoQuorum"),
        ("EPaxos", epaxos_dir, "EPaxos"),
    ]
    # labels = ["Read-optimized Strat w/ DQR", "Read-optimized Strat w/ Quorum Read", "Write-optimized Strat"]
    graph_average_latency_bar_chart(ax, experiment_data)
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.02),
        ncol=3,
        borderaxespad=0,
        frameon=False,
    )
    plt.savefig("logs/autoquorum-to-show/read-strats-overview.svg", format="svg")
    plt.show()
    plt.close()

    # Breakdown bar chart
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    fig.suptitle(f"Read Strategy Effect on 90% Read Ratio Workload")
    strats = ["BallotRead", "QuorumRead"]
    labels = ["[D, (2,4), DQR]", "[D, (2,4), QuorumRead]"]
    graph_read_strats_bar_chart(ax, experiment_directory, strats, list(labels))
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.02),
        ncol=2,
        borderaxespad=0,
        frameon=False,
    )
    plt.savefig("logs/autoquorum-to-show/read-strats-breakdown.svg", format="svg")
    plt.show()
    plt.close()


def graph_read_strats_over_workload(compare_protocols: bool = False):
    if compare_protocols:
        strats = ["AutoQuorum", "EPaxos", "QuorumRead"]
    else:
        strats = ["QuorumRead", "BallotRead", "ReadAsWrite", "AutoQuorum"]
    labels = {"BallotRead": "DQR", "EPaxos": "EPaxos fast path"}
    experiment_dir = "read-strats"
    read_ratio_data = []
    for read_ratio in [0.1, 0.3, 0.5, 0.7, 0.9]:
        for strat in strats:
            run_directory = experiment_dir + f"/read-ratio-{read_ratio}/{strat}"
            clients_data, _, _ = get_experiment_data(run_directory)
            all_clients = pd.concat(clients_data)
            all_clients["read_ratio"] = read_ratio
            all_clients["strat"] = labels.get(strat) or strat
            read_ratio_data.append(all_clients)
    ratio_df = pd.concat(read_ratio_data)
    rates_data = []
    slow_rates = {}
    for load in [30, 90, 180, 210, 300]:
        # for rate in [1, 3, 5, 7, 9]:
        for strat in strats:
            run_directory = experiment_dir + f"/total-load-{load}/{strat}"
            clients_data, _, _ = get_experiment_data(run_directory)
            all_clients = pd.concat(clients_data)
            all_clients["load"] = load
            all_clients["strat"] = labels.get(strat) or strat
            if strat == "QuorumRead":
                slow = (all_clients["response_latency"] > 50).sum()
                slow_rates[load] = slow / all_clients["response_latency"].notna().sum()
            rates_data.append(all_clients)
    rates_df = pd.concat(rates_data)
    hotspots_data = []
    relative_us_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
    for hotspot in relative_us_loads:
        for strat in strats:
            run_directory = experiment_dir + f"/us-load-{hotspot}/{strat}"
            clients_data, _, _ = get_experiment_data(run_directory)
            for id, data in clients_data.items():
                data["client"] = id
            all_clients = pd.concat(clients_data.values())
            all_clients["hotspot"] = hotspot
            all_clients["strat"] = labels.get(strat) or strat
            hotspots_data.append(all_clients)
    hotspots_df = pd.concat(hotspots_data)

    fig, axes = plt.subplots(
        1, 3, figsize=(18, 6), constrained_layout=True, sharey=True
    )

    # Response Latency vs. Read Ratio
    grouped_ratio = (
        ratio_df.groupby(["strat", "read_ratio"])["response_latency"]
        .mean()
        .reset_index()
    )
    for strat in grouped_ratio["strat"].unique():
        strat_data = grouped_ratio[grouped_ratio["strat"] == strat]
        if strat == "QuorumRead" and compare_protocols:
            continue
        if strat == "EPaxos fast path":
            axes[0].plot(
                strat_data["read_ratio"],
                2 * strat_data["response_latency"],
                label="EPaxos slow path",
                linewidth=2,
                linestyle="--",
                marker=strat_markers["EPaxos slow path"],
                color=strat_colors["EPaxos slow path"],
            )
        axes[0].plot(
            strat_data["read_ratio"],
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    axes[0].set_xlabel("Read Ratio")
    axes[0].set_ylabel("Average Response Latency")
    axes[0].set_ylim(bottom=0, top=145)
    axes[0].grid(axis="y", linestyle="--")
    axes[0].set_title("Response Latency vs. Read Ratio")

    # Response Latency vs. Rate
    grouped_rate = (
        rates_df.groupby(["strat", "load"])["response_latency"].mean().reset_index()
    )
    for strat in grouped_rate["strat"].unique():
        strat_data = grouped_rate[grouped_rate["strat"] == strat]
        if strat == "QuorumRead" and compare_protocols:
            continue
        if strat == "EPaxos fast path":
            axes[1].plot(
                strat_data["load"].map(slow_rates),
                2 * strat_data["response_latency"],
                label="EPaxos slow path",
                linewidth=2,
                linestyle="--",
                marker=strat_markers["EPaxos slow path"],
                color=strat_colors["EPaxos slow path"],
            )
        axes[1].plot(
            strat_data["load"].map(slow_rates),
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    axes[1].set_xlabel("Concurrent Write Rate")
    axes[1].grid(axis="y", linestyle="--")
    axes[1].set_title("Response Latency vs. Concurrent Write Rate")
    axes[1].tick_params(axis="y", which="both", length=0)
    axes[1].legend(loc="upper center", ncol=3, frameon=False)

    # Response Latency vs. Hotspot
    grouped_rate = (
        hotspots_df.groupby(["strat", "hotspot"])["response_latency"]
        .mean()
        .reset_index()
    )
    for strat in grouped_rate["strat"].unique():
        strat_data = grouped_rate[grouped_rate["strat"] == strat]
        if strat == "QuorumRead" and compare_protocols:
            continue
        if strat == "EPaxos fast path":
            axes[2].plot(
                strat_data["hotspot"],
                2 * strat_data["response_latency"],
                label="EPaxos slow path",
                linewidth=2,
                linestyle="--",
                marker=strat_markers["EPaxos slow path"],
                color=strat_colors["EPaxos slow path"],
            )
        axes[2].plot(
            strat_data["hotspot"],
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    axes[2].set_xlabel("Relative US Load")
    axes[2].grid(axis="y", linestyle="--")
    axes[2].set_title("Response Latency vs. US Hotspot")
    axes[2].tick_params(axis="y", which="both", length=0)

    # Show the plot
    plt.tight_layout()
    if compare_protocols:
        plt.savefig(
            "logs/autoquorum-to-show/read-strats-varied-protocol.svg", format="svg"
        )
    else:
        plt.savefig(
            "logs/autoquorum-to-show/read-strats-varied-strat.svg", format="svg"
        )
    plt.show()
    plt.close()


def graph_mixed_strat_bench():
    experiment_directory = "mixed-strats"
    mixed_dir = experiment_directory + "/Mixed"
    wread_dir = experiment_directory + "/ReadAsWrite"
    bread_dir = experiment_directory + "/BallotRead"
    autoquorum_dir = experiment_directory + "/AutoQuorum"
    epaxos_dir = experiment_directory + "/EPaxos"
    # graph_server_data(experiment_name_no_opt)
    # graph_latency_estimation_comparison(experiment_name)
    # graph_cluster_latency(experiment_name)
    # graph_optimization_comparison(experiment_name, experiment_name_no_opt)
    # graph_average_latency_comparison3(experiment_name, experiment_name_no_opt, experiment_name_no_opt2, title, labels)

    # Overview bar chart
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 7))
    fig.suptitle(
        "Global vs. Per-node Read Strategy Effect on 90-10 Read-Write Workload"
    )
    experiment_data = [
        # ("Mixed", mixed_dir, "[LA, (4,2), MIX]"),
        ("ReadAsWrite", wread_dir, "Only RAW reads"),  # "[LA, (4,2), RAW]"),
        ("BallotRead", bread_dir, "Only DQR reads"),  # "[LA, (4,2), DQR]"),
        ("AutoQuorum", autoquorum_dir, "AutoQuorum"),
        # ("EPaxos", epaxos_dir, "EPaxos"),
    ]
    # labels = ("Mixed Read Strategy", "Only RAW Reads", "Only BQR Reads")
    graph_average_latency_bar_chart(ax, experiment_data)
    ax.legend(
        loc="upper left",
        ncols=1,
        fontsize=14,
        frameon=False,
    )
    plt.savefig("logs/autoquorum-to-show/mixed-strats-overview.svg", format="svg")
    plt.show()
    plt.close()

    # Breakdown bar chart
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    fig.suptitle("Read Strategy Latency When Leader=LA, RQ=4, WQ=2")
    # labels = ["[LA, (4,2), RAW]", "[LA, (4,2), DQR]", "AutoQuorum"]
    labels = ["Only RAW reads", "Only DQR reads", "AutoQuorum"]
    strats = ["ReadAsWrite", "BallotRead", "AutoQuorum"]
    graph_read_strats_bar_chart(ax, experiment_directory, strats, labels)
    ax.legend(
        loc="upper left",
        ncols=1,
        fontsize=14,
        frameon=False,
    )
    plt.savefig("logs/autoquorum-to-show/mixed-strats-breakdown.svg", format="svg")
    plt.show()
    plt.close()


def graph_mixed_strats_over_workload(compare_protocols: bool = False):
    if compare_protocols:
        strats = ["AutoQuorum", "EPaxos"]
    else:
        strats = ["BallotRead", "Mixed", "ReadAsWrite", "AutoQuorum"]
    labels = {"BallotRead": "DQR", "EPaxos": "EPaxos fast path"}
    experiment_dir = "mixed-strats"
    read_ratio_data = []
    for read_ratio in [0.1, 0.3, 0.5, 0.7, 0.9]:
        for strat in strats:
            run_directory = experiment_dir + f"/read-ratio-{read_ratio}/{strat}"
            clients_data, _, _ = get_experiment_data(run_directory)
            all_clients = pd.concat(clients_data)
            all_clients["read_ratio"] = read_ratio
            all_clients["strat"] = labels.get(strat) or strat
            read_ratio_data.append(all_clients)
    ratio_df = pd.concat(read_ratio_data)

    hotspots_data = []
    relative_eu_loads = [0.5, 0.6, 0.7, 0.8, 0.9]
    for hotspot in relative_eu_loads:
        for strat in strats:
            run_directory = experiment_dir + f"/eu-load-{hotspot}/{strat}"
            clients_data, _, _ = get_experiment_data(run_directory)
            for id, data in clients_data.items():
                data["client"] = id
            all_clients = pd.concat(clients_data.values())
            all_clients["hotspot"] = hotspot
            all_clients["strat"] = labels.get(strat) or strat
            hotspots_data.append(all_clients)
    hotspots_df = pd.concat(hotspots_data)

    fig, axes = plt.subplots(
        1, 2, figsize=(18, 6), constrained_layout=True, sharey=True
    )

    # Response Latency vs. Read Ratio
    grouped_ratio = (
        ratio_df.groupby(["strat", "read_ratio"])["response_latency"]
        .mean()
        .reset_index()
    )
    for strat in grouped_ratio["strat"].unique():
        strat_data = grouped_ratio[grouped_ratio["strat"] == strat]
        if strat == "EPaxos fast path":
            axes[0].plot(
                strat_data["read_ratio"],
                2 * strat_data["response_latency"],
                label="EPaxos slow path",
                linewidth=2,
                linestyle="--",
                marker=strat_markers["EPaxos slow path"],
                color=strat_colors["EPaxos slow path"],
            )
        axes[0].plot(
            strat_data["read_ratio"],
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    axes[0].set_xlabel("Read Ratio")
    axes[0].set_ylabel("Mean Response Latency")
    axes[0].set_ylim(bottom=0, top=160)
    axes[0].grid(axis="y", linestyle="--")
    axes[0].set_title("Response Latency vs. Read Ratio")

    # Response Latency vs. Hotspot
    grouped_rate = (
        hotspots_df.groupby(["strat", "hotspot"])["response_latency"]
        .mean()
        .reset_index()
    )
    for strat in grouped_rate["strat"].unique():
        strat_data = grouped_rate[grouped_rate["strat"] == strat]
        if strat == "EPaxos fast path":
            axes[1].plot(
                strat_data["hotspot"],
                2 * strat_data["response_latency"],
                label="EPaxos slow path",
                linewidth=2,
                linestyle="--",
                marker=strat_markers["EPaxos slow path"],
                color=strat_colors["EPaxos slow path"],
            )
        axes[1].plot(
            strat_data["hotspot"],
            strat_data["response_latency"],
            label=strat,
            linewidth=2,
            marker=strat_markers[strat],
            color=strat_colors[strat],
        )
    axes[1].set_xlabel("Relative EU Load")
    axes[1].grid(axis="y", linestyle="--")
    axes[1].set_title("Response Latency vs. EU Load")
    axes[1].tick_params(axis="y", which="both", length=0)
    axes[1].legend(loc="upper left", ncol=2, frameon=False)

    # Show the plot
    plt.tight_layout()
    if compare_protocols:
        plt.savefig(
            "logs/autoquorum-to-show/mixed-strats-varied-protocol.svg", format="svg"
        )
    else:
        plt.savefig(
            "logs/autoquorum-to-show/mixed-strats-varied-strats.svg", format="svg"
        )
    plt.show()
    plt.close()


def graph_even_load_bench():
    # experiment_directory = "even-load"
    # aq_data = experiment_directory + "/AutoQuorum"
    # clients_data, _, _ = get_experiment_data(aq_data)
    # aq_df = pd.concat(clients_data)
    # maj_data = experiment_directory + "/MultiLeader-Majority"
    # clients_data, _, _ = get_experiment_data(maj_data)
    # maj_df = pd.concat(clients_data)
    # super_maj_data = experiment_directory + "/MultiLeader-SuperMajority"
    # clients_data, _, _ = get_experiment_data(super_maj_data)
    # super_maj_df = pd.concat(clients_data)
    #
    # a = aq_df["response_latency"].mean()
    # b = maj_df["response_latency"].mean()
    # c = super_maj_df["response_latency"].mean()
    # print(a,b,c)
    # # Define conflict rate range (e.g., from 0 to 1 in increments of 0.1)
    # conflict_rates = np.linspace(0, 1, 20)
    #
    # # Compute the dependent variable
    # conflict_values = c + (conflict_rates * b)
    #
    # # Plot the graph
    # plt.figure(figsize=(8, 5))
    # plt.plot(conflict_rates, conflict_values, label="Epaxos", marker='o')
    # plt.axhline(y=a, color='r', linestyle='--', label="AutoQuorum")
    #
    # # Labels and title
    # plt.xlabel("Conflict Rate")
    # plt.ylabel("Response Latency")
    # plt.title("Response Latency vs Conflict Rate")
    # plt.ylim(bottom=0)
    # plt.legend()
    # plt.grid(True)
    #
    # # Show the plot
    # plt.show()

    experiment_directory = "even-load"
    aq_data = experiment_directory + "/AutoQuorum"
    aq_clients, _, _ = get_experiment_data(aq_data)
    maj_data = experiment_directory + "/MultiLeader-Majority"
    maj_clients, _, _ = get_experiment_data(maj_data)
    super_maj_data = experiment_directory + "/MultiLeader-SuperMajority"
    super_maj_clients, _, _ = get_experiment_data(super_maj_data)
    latency_means = {}
    bar_group_labels = [""] * len(aq_clients)
    # AutoQuorum latencies
    latencies = [0] * len(aq_clients)
    stds = [None] * len(aq_clients)
    leader = "us-east4-a"
    for client, client_requests in aq_clients.items():
        gcp_location = client_requests.attrs["location"]
        location = location_name(gcp_location, gcp_location == leader)
        aq_latency = client_requests["response_latency"].mean()
        latencies[client - 1] = aq_latency
        bar_group_labels[client - 1] = location
    latency_means["AutoQuorum"] = (
        latencies,
        stds,
        strat_colors["AutoQuorum"],
        strat_hatches["AutoQuorum"],
    )
    # EPaxos latencies
    conflict_rates = np.linspace(0, 1, 3)
    for conflict_rate in conflict_rates:
        latencies = [0] * len(maj_clients)
        stds = [None] * len(maj_clients)
        for client in maj_clients.keys():
            maj_latency = maj_clients[client]["response_latency"].mean()
            super_maj_latency = super_maj_clients[client]["response_latency"].mean()
            epaxos_latency = super_maj_latency + (conflict_rate * maj_latency)
            latencies[client - 1] = epaxos_latency
        latency_means[f"EPaxos {conflict_rate}"] = (
            latencies,
            stds,
            str(1 - conflict_rate),
            None,
        )

    for k, v in latency_means.items():
        print(k, v)
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    fig.suptitle("Even load benchmark", size=20)
    create_base_barchart(ax, latency_means, bar_group_labels, error_bars=False)
    ax.legend(
        bbox_to_anchor=(0.5, 1.10),
        loc="center",
        ncol=len(conflict_rates) + 1,
        fontsize=10,
        frameon=False,
    )
    plt.show()
    plt.close()


def graph_test():
    experiment_name = "local-run"
    client_data, server_data, strat_data = get_experiment_data(
        experiment_name, dump_server_df=True
    )
    data = client_data[1]
    relative_sync_time = min(data.index)
    experiment_start = pd.Timestamp("20180606")
    data.index = experiment_start + (data.index - relative_sync_time)
    print(data)
    plt.plot(data.index, data.response_latency)
    plt.show()


def main():
    # Not used
    # graph_test()
    # graph_local_bench()
    # graph_round_robin_bench()
    # graph_round_robin_bench_w_multileader()

    # graph_round_robin5_bench()
    #
    graph_shifting_conditions_bench()
    # graph_shifting_conditions_debug()
    #
    # graph_read_strats_bench()
    # graph_read_strats_over_workload()
    # graph_read_strats_over_workload(compare_protocols=True)
    #
    # graph_mixed_strat_bench()
    # graph_mixed_strats_over_workload()
    # graph_mixed_strats_over_workload(compare_protocols=True)

    # graph_even_load_bench()

    # TODO
    # graph_shifting_load_bench()
    # graph_read_heavy_bench()
    # graph_read_ratio_bench()
    pass


if __name__ == "__main__":
    server_colors = {}
    server_locations = {}
    opt_color = "tab:orange"
    no_opt_color = "tab:blue"
    no_opt2_color = "tab:green"

    autoquorum_color = "tab:orange"
    wread_color = "tab:blue"
    bread_color = "tab:green"
    qread_color = "tab:purple"
    mixed_color = "tab:cyan"
    epaxos_color = "tab:gray"
    epaxos_slow_color = "black"
    # epaxos_slow_color = plt.colormaps.get_cmap("tab20")(7)
    strat_colors = {
        "AutoQuorum": autoquorum_color,
        "Baseline": wread_color,
        "ReadAsWrite": wread_color,
        "RAW": wread_color,
        "DQR": bread_color,
        "BallotRead": bread_color,
        "QuorumRead": qread_color,
        "Mixed": mixed_color,
        "EPaxos": epaxos_color,
        "EPaxos fast path": epaxos_color,
        "EPaxos slow path": epaxos_slow_color,
    }

    autoquorum_marker = "s"
    wread_marker = "D"
    bread_marker = "o"
    qread_marker = "h"
    mixed_marker = "X"
    epaxos_marker = "^"
    epaxos_slow_marker = "v"
    strat_markers = {
        "AutoQuorum": autoquorum_marker,
        "Baseline": wread_marker,
        "ReadAsWrite": wread_marker,
        "RAW": wread_marker,
        "DQR": bread_marker,
        "BallotRead": bread_marker,
        "QuorumRead": qread_marker,
        "Mixed": mixed_marker,
        "EPaxos": epaxos_marker,
        "EPaxos fast path": epaxos_marker,
        "EPaxos slow path": epaxos_slow_marker,
    }

    autoquorum_hatch = "x"
    wread_hatch = "-"
    bread_hatch = "/"
    qread_hatch = "\\"
    mixed_hatch = "+"
    epaxos_hatch = "."
    epaxos_slow_hatch = "o"
    strat_hatches = {
        "AutoQuorum": autoquorum_hatch,
        "Baseline": wread_hatch,
        "ReadAsWrite": wread_hatch,
        "RAW": wread_hatch,
        "DQR": bread_hatch,
        "BallotRead": bread_hatch,
        "QuorumRead": qread_hatch,
        "Mixed": mixed_hatch,
        "EPaxos": epaxos_hatch,
        "EPaxos fast path": epaxos_hatch,
        "EPaxos slow path": epaxos_slow_hatch,
    }

    main()
