import os
import sys
import json
import pandas as pd
import xarray as xr
from glob import glob


def netcdf_forecasts_to_zarr(suite_home: str, vpu: str) -> None:
    """Converts the netcdf forecast files to zarr.

    Args:
        suite_home (str): Path to the suite home directory.
        vpu (str): VPU code.
    """
    with open(os.path.join(suite_home, "rapid_run.json"), "r") as f:
        data = json.load(f)
        # Get rapid output path
        rapid_output = data["output_dir"]
        date = data["date"]

        # Get list of forecast files
        forecast_nc_list = glob(os.path.join(rapid_output, f"Qout_{vpu}*.nc"))

        # Sort list based on ensemble number
        sort_key = lambda x: int(os.path.basename(x)[:-3].split("_")[-1])
        forecast_nc_list.sort(key=sort_key)

        # Get list of netcdf ensemble datasets
        ens_list = [
            xr.open_dataset(forecast_nc)
            .drop("crs")
            .drop("Qout_err")
            .drop("lat")
            .drop("lon")
            .drop("time_bnds")
            for forecast_nc in forecast_nc_list
        ]

        # Concatenate all ensemble datasets
        # ens_list is sorted by ensemble number (from 1 to 52)
        combined_ens_dataset = xr.concat(
            ens_list, pd.Index(list(range(1, 53)), name="ensemble")
        )

        # Set 'Qout' fillvalue
        combined_ens_dataset["Qout"] = combined_ens_dataset["Qout"].where(
            combined_ens_dataset["Qout"].notnull(), other=-9999
        )

        # Chunk ensemble dataset along time dimension
        chunk_sizes = {
            "time": combined_ens_dataset.variables["time"].shape[0],
            "rivid": "auto",
        }
        combined_ens_dataset = combined_ens_dataset.chunk(chunk_sizes)

        # Create zarr output path
        zarr_output_path = os.path.join(rapid_output, f"Qout_{vpu}_{date}.zarr")

        # Write ensemble dataset to zarr
        combined_ens_dataset.to_zarr(zarr_output_path, mode="w")


if __name__ == "__main__":
    suite_home = sys.argv[1]
    vpu = sys.argv[2]
    netcdf_forecasts_to_zarr(suite_home, vpu)
