import argparse
import glob
import json
import os

import numpy as np
import pandas as pd
import xarray as xr


def netcdf_forecasts_to_zarr(workspace: str) -> None:
    """
    Converts the netcdf forecast files to zarr.

    Args:
        workspace (str): Path to rapid_run.json base directory.
    """
    with open(os.path.join(workspace, "rapid_run.json"), "r") as f:
        data = json.load(f)
        # Get rapid output path
        rapid_output = data["output_dir"]
        date = data["date"]

    # Get list of forecast files
    forecast_qout_list = glob.glob(
        os.path.join(rapid_output, f"Qout_*.nc")
    )

    vpu_nums = sorted(
        set(
            [os.path.basename(x).split("_")[1] for x in forecast_qout_list]
        )
    )

    def _concat_vpu_forecasts(vpu) -> xr.Dataset:
        ens_list = sorted(list(set(map(
            lambda x: int(x.split("_")[-1].split(".")[0]),
            forecast_qout_list
        ))))

        return xr.concat(
            [
                xr.open_dataset(x).drop_vars(
                    ["crs", "lat", "lon", "time_bnds", "Qout_err"]
                )
                for x in sorted(
                    glob.glob(
                        os.path.join(rapid_output, f"Qout_{vpu}_*.nc")
                    )
                )
            ],
            pd.Index(ens_list, name="ensemble"),
            fill_value=np.nan,
        )

    all_vpu_ds = xr.combine_nested(
        [_concat_vpu_forecasts(vpu) for vpu in vpu_nums],
        concat_dim="rivid",
        fill_value=np.nan,
    )

    (
        all_vpu_ds.chunk(
            {"time": -1, "rivid": "auto", "ensemble": -1}
        ).to_zarr(
            os.path.join(rapid_output, f"Qout_{date}.zarr"),
            consolidated=True,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "workspace",
        nargs=1,
        help="Path to the suite home directory.",
    )
    args = parser.parse_args()
    workspace = args.workspace[0]

    netcdf_forecasts_to_zarr(workspace)
