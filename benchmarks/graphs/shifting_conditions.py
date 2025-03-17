from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from experiments.experiment_data import ExperimentData
from graphs.colors import strat_colors

_experiment_dir = Path(__file__).parent.parent / "logs" / "shifting-conditions"


def shifting_conditions_data(iteration_path: str) -> ExperimentData:
    return ExperimentData(_experiment_dir / iteration_path)


def graph_shifting_conditions():
    df = get_shifting_conditions_data()

    plt.figure(figsize=(10, 6))
    resampled = df.groupby("cluster_type").resample("3000ms").mean(numeric_only=True)
    for cluster_type, group in resampled.groupby("cluster_type"):
        request_times = group.index.get_level_values("request_time")
        response_latency = group["response_latency"]
        if cluster_type == "MultiLeader-SuperMajority":
            plt.plot(
                request_times,
                response_latency,
                label="EPaxos fast path",
                linewidth=2,
                color=strat_colors["EPaxos fast path"],
            )
        else:
            plt.plot(
                request_times,
                response_latency,
                label=cluster_type,
                linewidth=2,
                color=strat_colors[cluster_type],
            )
    plt.xlabel("Experiment Time", fontsize=12)
    plt.ylabel("Average Request Latency (ms)", fontsize=12)
    plt.ylim(bottom=0)
    plt.legend(ncols=2, loc="upper center", bbox_to_anchor=(0.5, 1.12), frameon=False)
    plt.tight_layout()
    # plt.savefig("logs/autoquorum-to-show/shifting-conditions.svg", format="svg")
    plt.show()
    plt.close()


# Stitch together periods into a single dataframe with continuous time
def get_shifting_conditions_data() -> pd.DataFrame:
    all_data = []
    epoch_start = pd.Timestamp("20180606")
    for opt in ["AutoQuorum", "MultiLeader-SuperMajority", "Etcd"]:
        period_time = epoch_start
        for period in [1, 2, 4]:
            iteration_dir = _experiment_dir / f"period-{period}" / f"{opt}"
            exp_data = ExperimentData(iteration_dir)
            period_dur = get_period_duration(exp_data)
            client_data = [df for df in exp_data.client_data.values() if not df.empty]
            period_start = min(df.index.min() for df in client_data)
            for df in client_data:
                if df.empty:
                    continue
                df.index = period_time + (df.index - period_start)
                df["period"] = period
                df["cluster_type"] = opt
                all_data.append(df)
            period_time += period_dur
    return pd.concat(all_data)


def get_period_duration(exp_data: ExperimentData):
    configs = exp_data.experiment_summary["client_configs"][1]
    aq_config = configs.get("autoquorum_client_config")
    ml_config = configs.get("multileader_client_config")
    et_config = configs.get("etcd_client_config")
    config = aq_config or ml_config or et_config
    duration_config = config["requests"][0]["duration_sec"]
    return pd.Timedelta(seconds=duration_config)
