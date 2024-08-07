import json
import argparse
from geoglows_ecflow.workflow.comfies.sdeploy import sdeploy


def main(config_path: str, params_values={}, dry_run=False) -> None:
    """Create the workflow definition.

    Args:
        config_path (str): Path to the configuration file.
        params_values (dict): Param/value dict; these override params from
            config file
        dry_run (bool): If True, no actual files will be created in target dir;
            useful for testing

    Returns:
        None
    """
    sdeploy(config_path, params_values=params_values, dry_run=dry_run)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Create the workflow definition.")
    parser.add_argument(
        "--config", "-c", required=True, help="Path to the configuration file."
    )
    parser.add_argument(
        "--params",
        "-p",
        required=False,
        help="Param/value dict; these override params from config file.",
    )
    parser.add_argument(
        "--dry",
        "-d",
        required=False,
        help="If True, no actual files will be created in target dir.",
    )
    
    args = parser.parse_args()
    config = args.config
    params = json.loads(args.params) if args.params else {}
    dry = args.dry if args.dry else False

    main(config, params, dry)
