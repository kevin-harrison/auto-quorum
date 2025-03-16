import json
from pathlib import Path

import pandas as pd


class ExperimentData:
    experiment_summary: dict
    server_data: dict[int, pd.DataFrame]
    client_data: dict[int, pd.DataFrame]
    strategy_data: pd.DataFrame | None

    ZONE_NAMES: dict[str, str] = {
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
    ZONE_COLORS: dict[str, str] = {
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
    # Old mapping
    # color_mapping = {
    #     "us-west2-a" : "#FDBB3B",
    #     "us-south1-a": "#4CA98F",
    #     "us-east4-a": "#9A6DB8",
    #     "europe-west2-a": "#4CA98F",
    #     "europe-west4-a": "#FF6478",
    #     "europe-west10-a": "#9A6DB8",
    #     "europe-southwest1-a": "#5E5E5E",
    # }

    def __init__(self, experiment_path: Path) -> None:
        experiment_summary_file = experiment_path / "experiment-summary.json"
        with open(experiment_summary_file, "r") as exp_summary:
            exp_summary = json.load(exp_summary)
            exp_summary["server_configs"] = {
                int(k): v for k, v in exp_summary["server_configs"].items()
            }
            exp_summary["client_configs"] = {
                int(k): v for k, v in exp_summary["client_configs"].items()
            }
        self.experiment_path = experiment_path
        self.experiment_summary = exp_summary
        self.server_data = self._create_server_dfs()
        self.client_data = self._create_client_dfs()
        self.strategy_data = self._create_strategy_df()

    def get_server_location(self, server_id: int) -> str:
        server_config = self.experiment_summary["server_configs"][server_id]
        server_zone = server_config["instance_config"]["zone"]
        name = ExperimentData.ZONE_NAMES.get(server_zone)
        if name is None:
            raise ValueError(f"Don't have name for location {server_zone}")
        return name

    def get_client_location(self, client_id: int) -> str:
        client_config = self.experiment_summary["client_configs"][client_id]
        client_zone = client_config["instance_config"]["zone"]
        name = ExperimentData.ZONE_NAMES.get(client_zone)
        if name is None:
            raise ValueError(f"Don't have name for location {client_zone}")
        return name

    def get_server_color(self, server_id: int) -> str:
        server_config = self.experiment_summary["server_configs"][server_id]
        server_zone = server_config["instance_config"]["zone"]
        name = ExperimentData.ZONE_COLORS.get(server_zone)
        if name is None:
            raise ValueError(f"Don't have color for location {server_zone}")
        return name

    def get_client_color(self, client_id: int) -> str:
        client_config = self.experiment_summary["client_configs"][client_id]
        client_zone = client_config["instance_config"]["zone"]
        name = ExperimentData.ZONE_COLORS.get(client_zone)
        if name is None:
            raise ValueError(f"Don't have color for location {client_zone}")
        return name

    def show_initial_cluster_strategy(self):
        aq_config = self.experiment_summary.get("autoquorum_cluster_config")
        ml_config = self.experiment_summary.get("multileader_cluster_config")
        config = aq_config or ml_config
        if config is not None:
            printable_config = {k: v for k, v in config.items() if k != "node_addrs"}
            try:
                display(printable_config)
            except NameError:
                print(printable_config)
        else:
            print("No initial cluster strategy")

    def show_reconfigurations(self):
        if self.strategy_data is not None:
            reconfigurations = self.strategy_data[
                self.strategy_data["reconfigure"]
            ].drop(columns=["reconfigure", "leader", "operation_latency"])
            pd.set_option("display.max_colwidth", 500)
            try:
                display(reconfigurations)
            except NameError:
                print(reconfigurations)
        else:
            print("NO RECONFIGURATIONS")

    def normalize_to_experiment_start(self):
        epoch_start = pd.Timestamp("20180606")
        experiment_start = min(df.index.min() for df in self.client_data.values())
        for client_data in self.client_data.values():
            client_data.index = epoch_start + (client_data.index - experiment_start)
        for server_data in self.server_data.values():
            server_data.index = epoch_start + (server_data.index - experiment_start)
        if self.strategy_data is not None:
            self.strategy_data.index = epoch_start + (
                self.strategy_data.index - experiment_start
            )

    def _create_server_dfs(self, dump_df: bool = False) -> dict[int, pd.DataFrame]:
        server_data = {}
        for id, config in self.experiment_summary["server_configs"].items():
            if config["experiment_output"] is None:
                continue
            output_file = self.experiment_path / config["experiment_output"]
            server_df = pd.read_json(output_file, lines=True)
            server_df = server_df.astype(dtype={"timestamp": "datetime64[ms]"})
            server_df.set_index("timestamp", inplace=True)
            if dump_df:
                df_file = output_file.parent / f"server-{id}.df"
                with open(df_file, "w") as f:
                    with pd.option_context(
                        "display.max_rows", None, "display.max_columns", None
                    ):
                        print(server_df.to_string(), file=f)
            server_data[id] = server_df
        return server_data

    def _create_client_dfs(self) -> dict[int, pd.DataFrame]:
        client_data = {}
        for id, config in self.experiment_summary["client_configs"].items():
            output_file = self.experiment_path / config["experiment_output"]
            try:
                client_df = pd.read_csv(
                    output_file,
                    names=["request_time", "write", "response_time"],
                    header=0,
                    dtype={"write": "bool"},
                )
            except pd.errors.EmptyDataError:
                client_df = pd.DataFrame(
                    columns=["request_time", "write", "response_time"]
                )
            client_df["response_latency"] = (
                client_df.response_time - client_df.request_time
            )
            client_df = client_df.astype(dtype={"request_time": "datetime64[ms]"})
            client_df.set_index("request_time", inplace=True)
            client_data[id] = client_df
        return client_data

    def _create_strategy_df(self) -> pd.DataFrame | None:
        strategy_cols = [
            "reconfigure",
            "leader",
            "curr_strat_latency",
            "opt_strat_latency",
            "curr_strat",
            "opt_strat",
            "operation_latency",
        ]
        strategy_data = []
        for id, server_df in self.server_data.items():
            server_strat_data = server_df.loc[server_df["leader"] == id][strategy_cols]
            strategy_data.append(server_strat_data)
        if len(strategy_data) > 0:
            strategy_df = pd.concat(strategy_data)
            strategy_df.sort_values(by="timestamp", inplace=True)
            return strategy_df
        else:
            return None
