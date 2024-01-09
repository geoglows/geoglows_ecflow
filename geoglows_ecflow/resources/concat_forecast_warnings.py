import os
import glob
import sys
import logging
import pandas as pd
import argparse


def concat_warnings(workdir: str) -> None:
    """
    Concatenates the warnings from each VPU into 1 parquet of all warnings data

    Args:
        workdir: Path to daily forecast workspace

    Returns:
        None
    """
    date = os.path.basename(workdir)
    warnings_files = glob.glob(os.path.join(workdir, "output", "*", "forecastwarnings_*.parquet"))
    output_file = os.path.join(workdir, f"forecastwarnings_{date}.parquet")

    logging.info(f"Found {len(warnings_files)} warnings files")
    if len(warnings_files) == 0:
        logging.info("No warnings files found")
        logging.info("Skipping Warnings File Concatenation")
        return

    logging.info(f"Concatenating warnings files to {output_file}")

    (
        pd.concat([pd.read_parquet(x) for x in warnings_files])
        .reset_index(drop=True)
        .to_parquet(output_file)
    )
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "workspace",
        nargs=1,
        help="Path to the daily workspace directory, named in YYYYMMDDHH "
        "format, containing (1) *.runoff.nc IFS forecast files, "
        "(2) an output directory of routed discharge netcdfs, "
        "(3) symlinks to the rapid inputs and return periods directories",
    )
    args = parser.parse_args()
    workspace = args.workspace[0]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    concat_warnings(workspace)
    exit(0)
