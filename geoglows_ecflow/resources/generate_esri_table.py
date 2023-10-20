import argparse
import glob
import logging
import os
import subprocess as sp
import netCDF4 as nc
import pandas as pd
import xarray as xr

import sys


def postprocess_vpu_forecast_directory(
    rapid_output: str,
    returnperiods: str,
    vpu: int or str,
    nces_exec: str = "nces",
):
    # creates file name for the csv file
    date_string = os.path.basename(os.path.dirname(rapid_output))  # should be a date in YYYYMMDDHH format
    style_table_file_name = (
        f"mapstyletable_{vpu}_{date_string}.parquet"
    )
    if os.path.exists(os.path.join(rapid_output, style_table_file_name)):
        logging.info(f"Style table already exists: {style_table_file_name}")
        return
    logging.info(f"Creating style table: {style_table_file_name}")

    # calls NCO's nces function to calculate ensemble statistics for the max,
    # mean, and min Qout. * ens([1 - 9] | [1 - 4][0 - 9] | 5[0 - 1])\.nc
    logging.info("Calling NCES statistics")

    findstr = " ".join(
        [
            x
            for x in glob.glob(os.path.join(rapid_output, f"Qout_{vpu}_*.nc"))
            if "_52." not in x
        ]
    )
    output_filename = os.path.join(rapid_output, f"nces_avg_{vpu}.nc")
    ncesstr = f"{nces_exec} -O --op_typ=avg -o {output_filename}"
    sp.call(f"{ncesstr} {findstr}", shell=True)

    # read the date and COMID lists from one of the netcdfs
    with xr.open_dataset(
        glob.glob(os.path.join(rapid_output, f"nces_avg_{vpu}.nc"))[0]
    ) as ds:
        comids = ds["rivid"][:].values
        dates = pd.to_datetime(ds["time"][:].values)
        mean_flows = ds["Qout"][:].values.round(2)

    mean_flow_df = pd.DataFrame(mean_flows, columns=comids, index=dates)

    # limit both dataframes to the first 10 days
    mean_flow_df = mean_flow_df[
        mean_flow_df.index <= mean_flow_df.index[0] + pd.Timedelta(days=10)
    ]

    # creating pandas dataframe with return periods
    rp_path = os.path.join(returnperiods, f"returnperiods_{vpu}.nc")
    logging.info(f"Return Period Path {rp_path}")
    with nc.Dataset(rp_path, "r") as rp_ncfile:
        rp_df = pd.DataFrame(
            {
                "return_2": rp_ncfile.variables["rp2"][:],
                "return_5": rp_ncfile.variables["rp5"][:],
                "return_10": rp_ncfile.variables["rp10"][:],
                "return_25": rp_ncfile.variables["rp25"][:],
                "return_50": rp_ncfile.variables["rp50"][:],
                "return_100": rp_ncfile.variables["rp100"][:],
            },
            index=rp_ncfile.variables["rivid"][:],
        )

    mean_thickness_df = pd.DataFrame(columns=comids, index=dates, dtype=int)
    mean_thickness_df[:] = 1
    mean_thickness_df[mean_flow_df >= 20] = 2
    mean_thickness_df[mean_flow_df >= 250] = 3
    mean_thickness_df[mean_flow_df >= 1500] = 4
    mean_thickness_df[mean_flow_df >= 10000] = 5
    mean_thickness_df[mean_flow_df >= 30000] = 6

    mean_ret_per_df = pd.DataFrame(columns=comids, index=dates, dtype=int)
    mean_ret_per_df[:] = 0
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_2"], axis=1)] = 2
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_5"], axis=1)] = 5
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_10"], axis=1)] = 10
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_25"], axis=1)] = 25
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_50"], axis=1)] = 50
    mean_ret_per_df[mean_flow_df.gt(rp_df["return_100"], axis=1)] = 100

    mean_flow_df = mean_flow_df.stack().to_frame().rename(columns={0: "mean"})
    mean_thickness_df = (
        mean_thickness_df.stack().to_frame().rename(columns={0: "thickness"})
    )
    mean_ret_per_df = (
        mean_ret_per_df.stack().to_frame().rename(columns={0: "ret_per"})
    )

    # merge all dataframes
    for df in [mean_thickness_df, mean_ret_per_df]:
        mean_flow_df = mean_flow_df.merge(
            df, left_index=True, right_index=True
        )
    mean_flow_df.index.names = ["timestamp", "comid"]
    mean_flow_df = mean_flow_df.reset_index()
    mean_flow_df["mean"] = mean_flow_df["mean"].round(1)
    mean_flow_df.loc[mean_flow_df["mean"] < 0, "mean"] = 0
    mean_flow_df["thickness"] = mean_flow_df["thickness"].astype(int)
    mean_flow_df["ret_per"] = mean_flow_df["ret_per"].astype(int)
    mean_flow_df.to_parquet(os.path.join(rapid_output, style_table_file_name))
    return


# runs function on file execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "rapid_output",
        nargs=1,
        help="Path to the forecast output directory for a single VPU which "
        "contains subdirectories with date names",
    )
    parser.add_argument(
        "returnperiods",
        nargs=1,
        help="Path to base directory for return periods nc files.",
    )
    parser.add_argument(
        "vpu",
        nargs=1,
        help="id number of vpu to process"
    )
    parser.add_argument(
        "ncesexec",
        nargs=1,
        help="Path to the nces executable or recognized cli command if "
        "installed in environment. Should be 'nces' if installed in "
        "environment using conda",
    )

    args = parser.parse_args()
    rapid_output = args.rapid_output[0]
    returnperiods = args.returnperiods[0]
    vpu = args.vpu[0]
    nces = args.ncesexec[0]

    logging.basicConfig(
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout
    )

    postprocess_vpu_forecast_directory(rapid_output, returnperiods, vpu, nces)
