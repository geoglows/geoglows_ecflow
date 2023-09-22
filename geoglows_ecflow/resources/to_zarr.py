import os
from glob import glob
import pandas as pd
import xarray as xr


def main(
    base_dir: str,
    vpucode: str,
    date: str,
) -> None:
    """Convert the forecast files to zarr format.

    Args:
        base_dir (str): Path to the base directory where rapid output is stored.
        vpucode (str): VPU code.
        date (str): Date of the forecast.
    """
    forecast_dir = os.path.join(base_dir, vpucode, date)
    forecast_nc_list = sorted(
        glob(os.path.join(forecast_dir, "Qout*.nc")),
        reverse=True
    )

    ens_index = []
    ens_list = []
    for forecast_nc in forecast_nc_list:
        ens_index.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
        ens_list.append(xr.open_dataset(forecast_nc))

    ens_dataset = xr.concat(ens_list, pd.Index(ens_index, name='ensemble'))
    ens_dataset.to_zarr(os.path.join(base_dir, f'{vpucode}_{date}.zarr'))
