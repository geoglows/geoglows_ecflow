import sys
import glob
import os
import subprocess as sp

import netCDF4 as nc
import pandas as pd
import xarray as xr


def extract_summary_table(
    workspace: str, nces_exec: str, rp_base_dir: str
) -> None:
    """Extracts the summary table from the forecast output directory.

    Args:
        workspace (str): Path to the forecast output directory for a single VPU
            which contains subdirectories with date names
        nces_exec (str): Path to the nces executable or recognized cli command
            if installed in environment. Should be 'nces' if installed in
            environment using conda
        rp_base_dir (str): Path to directory containing the return period nc
            file for a single vpu.

    """
    # creates file name for the csv file
    date_string = os.path.split(workspace)[1].replace(".", "")
    region_name = os.path.basename(os.path.split(workspace)[0])
    file_name = f"summary_table_{region_name}_{date_string}.parquet"
    if os.path.exists(file_name):
        return

    # calls NCO's nces function to calculate ensemble statistics
    # calculates the max, mean, and min
    for stat in ["max", "avg", "min"]:
        findstr = 'find {0} -name "Qout*.nc"'.format(workspace)
        filename = os.path.join(workspace, "nces.{0}.nc".format(stat))
        ncesstr = "{0} -O --op_typ={1} {2}".format(nces_exec, stat, filename)
        args = " | ".join([findstr, ncesstr])
        sp.call(args, shell=True)

    # creating pandas dataframe with return periods
    rp_path = glob.glob(os.path.join(rp_base_dir, f"returnperiods*.nc*"))[0]
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

    # read the date and COMID lists from one of the netcdfs
    with xr.open_dataset(
        glob.glob(os.path.join(workspace, "nces.avg.nc"))[0]
    ) as ds:
        comids = ds["rivid"][:].values
        dates = ds["time"][:].values
        mean_flows = ds["Qout"][:].values.round(2)

    # read the max and avg flows
    with nc.Dataset(os.path.join(workspace, "nces.max.nc")) as ds:
        max_flows = ds["Qout"][:].round(2)

    mean_flow_df = pd.DataFrame(mean_flows, columns=comids, index=dates)
    max_flow_df = pd.DataFrame(max_flows, columns=comids, index=dates)

    mean_color_df = pd.DataFrame(columns=comids, index=dates, dtype=str)
    mean_color_df[:] = "blue"
    mean_color_df[mean_flow_df.gt(rp_df["return_2"], axis=1)] = "yellow"
    mean_color_df[mean_flow_df.gt(rp_df["return_10"], axis=1)] = "red"
    mean_color_df[mean_flow_df.gt(rp_df["return_50"], axis=1)] = "purple"

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
    max_flow_df = max_flow_df.stack().to_frame().rename(columns={0: "max"})
    mean_color_df = (
        mean_color_df.stack().to_frame().rename(columns={0: "color"})
    )
    mean_thickness_df = (
        mean_thickness_df.stack().to_frame().rename(columns={0: "thickness"})
    )
    mean_ret_per_df = (
        mean_ret_per_df.stack().to_frame().rename(columns={0: "ret_per"})
    )

    # merge all dataframes
    summary_table_df = pd.concat(
        [
            mean_flow_df,
            max_flow_df,
            mean_color_df,
            mean_thickness_df,
            mean_ret_per_df,
        ],
        axis=1,
    )
    summary_table_df.index.names = ["timestamp", "comid"]
    summary_table_df = summary_table_df.reset_index()
    summary_table_df.to_parquet(os.path.join(workspace, file_name))
    return


if __name__ == "__main__":
    extract_summary_table(sys.argv[1], sys.argv[2], sys.argv[3])
