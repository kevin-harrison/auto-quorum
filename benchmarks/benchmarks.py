import argparse
from pathlib import Path

from experiments.base_experiment import BaseExperiment
from experiments.even_load_experiment import EvenLoadExperiment
from experiments.mixed_strats_experiment import MixedStratsExperiment
from experiments.read_strats_experiment import ReadStratsExperiment
from experiments.round_robin_experiment import RoundRobinExperiment
from experiments.shifting_conditions_experiment import ShiftingConditionsExperiment

EXPERIMENTS: dict[str, type[BaseExperiment]] = {
    "mixed-strats": MixedStratsExperiment,
    "even-load": EvenLoadExperiment,
    "read-strats": ReadStratsExperiment,
    "round-robin": RoundRobinExperiment,
    "shifting-conditions": ShiftingConditionsExperiment,
}


# Parse CLI arguments into an experiment initialization
def build_experiment_arg_parser():
    parser = argparse.ArgumentParser(
        description="Run experiments from the command line."
    )
    subparsers = parser.add_subparsers(dest="experiment", required=True)
    for name, experiment_class in EXPERIMENTS.items():
        subparser = subparsers.add_parser(
            name, help=f"Run {experiment_class.__name__}."
        )
        subparser.add_argument(
            "--cluster-type",
            type=str,
            choices=experiment_class.CLUSTER_TYPES,
            required=True,
            help="Type of cluster to use.",
        )
        subparser.add_argument(
            "--experiment-dir",
            type=lambda d: Path(d).resolve(),
            default=None,
            help="Directory for experiment logs. If not provided, the class default is used.",
        )
        subparser.add_argument(
            "--destroy-instances",
            action="store_true",
            help="Destroy instances after the experiment.",
        )
        if experiment_class.EXPERIMENT_TYPES is not None:
            subparser.add_argument(
                "--experiment-type",
                type=str,
                choices=experiment_class.EXPERIMENT_TYPES,
                required=True,
                help="Subtype of experiment to run.",
            )
    return parser


# Run experiments from a CLI
def main():
    experiment_arg_parser = build_experiment_arg_parser()
    args = experiment_arg_parser.parse_args()
    experiment_class = EXPERIMENTS[args.experiment]

    # Build constructor kwargs dynamically.
    kwargs = {
        "cluster_type": args.cluster_type,
        "experiment_dir": args.experiment_dir,
        "destroy_instances": args.destroy_instances,
    }
    if experiment_class.EXPERIMENT_TYPES is not None:
        kwargs["experiment_type"] = args.experiment_type

    # Instantiate and run the experiment.
    experiment = experiment_class(**kwargs)
    experiment.run()


if __name__ == "__main__":
    main()
