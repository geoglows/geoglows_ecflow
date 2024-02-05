import argparse
import glob
import json
import logging
import os
import sys

import dask
import numpy as np
import xarray as xr
from numcodecs import Blosc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    stream=sys.stdout,
)


def netcdf_forecasts_to_zarr(workspace: str) -> None:
    """
    Converts the netcdf forecast files to zarr.

    Args:
        workspace (str): Path to rapid_run.json base directory.
    """
    with open(os.path.join(workspace, "rapid_run.json"), "r") as f:
        data = json.load(f)
        rapid_output = data["output_dir"]
        date = data["date"]

    vpu_nums = sorted(
        set([os.path.basename(x).split("_")[1] for x in glob.glob(os.path.join(rapid_output, f"Qout_*_52.nc"))])
    )

    qout_1_51_files = sorted([os.path.join(rapid_output, f"Qout_{vpu}.nc") for vpu in vpu_nums])
    qout_52_files = sorted(glob.glob(os.path.join(rapid_output, f"Qout_*_52.nc")))
    with dask.config.set(**{
        'array.slicing.split_large_chunks': False,
        # set the max chunk size to 5MB
        'array.chunk-size': '10MB',
        # use the threads scheduler
        'scheduler': 'threads',
        # set the maximum memory target usage to 90% of total memory
        'distributed.worker.memory.target': 0.90,
        # do not allow spilling to disk
        'distributed.worker.memory.spill': False,
        # specify the amount of resources to allocate to dask workers
        # 'distributed.worker.resources': {
        #     'memory': 2e9,  # 1e9=1GB, this is the amount per worker
        #     'cpu': os.cpu_count(),  # num CPU per worker
        # }
    }):
        logging.info("Opening ensembles 1-51 datasets")
        with xr.open_mfdataset(qout_1_51_files, combine="nested", concat_dim="rivid") as ds151:
            logging.info("Assigning the ensemble coordinate variable")
            ds151 = ds151.assign_coords(ensemble=np.arange(1, 52))
            logging.info("Opening ensemble 52 dataset")
            with xr.open_mfdataset(qout_52_files, combine="nested", concat_dim="rivid") as ds52:
                logging.info("Assigning the ensemble coordinate variable")
                ds52 = ds52.assign_coords(ensemble=52)

                logging.info("Concatenating 1-51 and 52 datasets")
                ds = xr.concat([ds151, ds52], dim="ensemble")

                logging.info("Configuring compression")
                compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
                encoding = {'Qout': {"compressor": compressor}}
                logging.info("Writing to zarr")
                (
                    ds
                    .drop_vars(["crs", "lat", "lon", "time_bnds", "Qout_err"])
                    .chunk({
                        "time": -1,
                        "rivid": "auto",
                        "ensemble": -1
                    })
                    .to_zarr(
                        os.path.join(rapid_output, f"Qout_{date}.zarr"),
                        consolidated=True,
                        encoding=encoding,
                    )
                )
                logging.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "workspace",
        nargs=1,
        help="Path to the suite home directory.",
    )
    args = parser.parse_args()
    netcdf_forecasts_to_zarr(workspace=args.workspace[0])
