import datetime
import glob
import logging
import os
import sys
import argparse
import numpy as np
import pandas as pd
import xarray as xr
import netCDF4 as nc
import dask
from numcodecs import Blosc



def merge_forecast_qout_files(rapid_output: str, vpu: str | int):
    # list the forecast files
    prediction_files = sorted(
        glob.glob(os.path.join(rapid_output, f"Qout_{vpu}_*.nc"))
    )

    # merge them into a single file joined by ensemble number
    ensemble_index_list = []
    qout_datasets = []
    for forecast_nc in prediction_files:
        ensemble_index_list.append(
            int(os.path.basename(forecast_nc)[:-3].split("_")[-1])
        )
        qout_datasets.append(xr.open_dataset(forecast_nc).Qout)
    return xr.concat(
        qout_datasets, pd.Index(ensemble_index_list, name="ensemble")
    )


def check_for_return_period_flow(
    largeflows_df, forecasted_flows_df, stream_order, rp_data
):
    max_flow = max(forecasted_flows_df["means"])

    # temporary dates
    date_r5 = ""
    date_r10 = ""
    date_r25 = ""
    date_r50 = ""
    date_r100 = ""

    # retrieve return period flow levels from dataframe
    r2 = float(rp_data["rp2"].values[0])
    r5 = float(rp_data["rp5"].values[0])
    r10 = float(rp_data["rp10"].values[0])
    r25 = float(rp_data["rp25"].values[0])
    r50 = float(rp_data["rp50"].values[0])
    r100 = float(rp_data["rp100"].values[0])

    # then compare the timeseries to the return period thresholds
    if max_flow >= r2:
        date_r2 = get_time_of_first_exceedance(forecasted_flows_df, r2)
    # if the flow is not larger than the smallest return period, return the
    # dataframe without appending anything
    else:
        return largeflows_df

    # check the rest of the return period flow levels
    if max_flow >= r5:
        date_r5 = get_time_of_first_exceedance(forecasted_flows_df, r5)
    if max_flow >= r10:
        date_r10 = get_time_of_first_exceedance(forecasted_flows_df, r10)
    if max_flow >= r25:
        date_r25 = get_time_of_first_exceedance(forecasted_flows_df, r25)
    if max_flow >= r50:
        date_r50 = get_time_of_first_exceedance(forecasted_flows_df, r50)
    if max_flow >= r100:
        date_r100 = get_time_of_first_exceedance(forecasted_flows_df, r100)

    new_row = pd.DataFrame(
        {
            "comid": rp_data.index[0],
            "stream_order": stream_order,
            "max_forecasted_flow": round(max_flow, 2),
            "date_exceeds_return_period_2": date_r2,
            "date_exceeds_return_period_5": date_r5,
            "date_exceeds_return_period_10": date_r10,
            "date_exceeds_return_period_25": date_r25,
            "date_exceeds_return_period_50": date_r50,
            "date_exceeds_return_period_100": date_r100,
        },
        index=[0],
    )

    largeflows_df = pd.concat([largeflows_df, new_row], ignore_index=True)

    return largeflows_df


def get_time_of_first_exceedance(forecasted_flows_df, flow):
    # replace the flows that are too small (don't exceed the return period)
    forecasted_flows_df[forecasted_flows_df.means < flow] = np.nan
    daily_flows = forecasted_flows_df.dropna()
    return daily_flows["times"].values[0]


def postprocess_vpu(
    vpu,
    rapid_input,
    rapid_output,
    return_periods_dir,
    forecast_records,
):
    # build the propert directory paths

    # make the pandas dataframe to store the summary info
    largeflows = pd.DataFrame(
        columns=[
            "comid",
            "stream_order",
            "max_forecasted_flow",
            "date_exceeds_return_period_2",
            "date_exceeds_return_period_5",
            "date_exceeds_return_period_10",
            "date_exceeds_return_period_25",
            "date_exceeds_return_period_50",
            "date_exceeds_return_period_100",
        ]
    )

    # merge the most recent forecast files into a single xarray dataset
    logging.info("  merging forecasts")

    merged_forecasts = xr.open_dataset(
        os.path.join(rapid_output, f"nces_avg_{vpu}.nc")
    )

    # collect the times and comids from the forecasts
    logging.info("  reading info from forecasts")
    times = pd.to_datetime(pd.Series(merged_forecasts.time))
    comids = pd.Series(merged_forecasts.rivid)
    tomorrow = times[0] + pd.Timedelta(days=1)
    year = times[0].strftime("%Y")

    # read the return period file
    logging.info("  reading return period file")
    return_period_file = os.path.join(
        return_periods_dir, f"returnperiods_{vpu}.nc"
    )
    return_period_data = xr.open_dataset(return_period_file).to_dataframe()

    # read the list of large streams
    logging.info("  creating dataframe of large streams")
    streams_file_path = os.path.join(rapid_input, "master_table.parquet")
    streams_df = pd.read_parquet(streams_file_path)
    large_vpu_streams_df = streams_df[
        (streams_df["VPUCode"] == int(vpu)) & ((streams_df["strmOrder"] >= 3))
    ]

    # get the list of comids
    large_list = large_vpu_streams_df["LINKNO"].tolist()

    # store the first day flows in a huge array
    logging.info("  beginning to iterate over the comids")
    first_day_flows = []

    # now process the mean flows for each river in the vpu
    for comid in comids:
        # compute the timeseries of average flows
        means = merged_forecasts.sel(rivid=comid).Qout.values.flatten()

        # put it in a dataframe with the times series
        forecasted_flows = (
            times.to_frame(name="times")
            .join(pd.Series(means, name="means"))
            .dropna()
        )
        # select flows in 1st day and save them to the forecast record
        first_day_flows.append(
            forecasted_flows[forecasted_flows.times < tomorrow][
                "means"
            ].tolist()
        )

        # if stream order is larger than 2, check if it needs to be included on
        # the return periods summary csv
        if comid in large_list:
            order = large_vpu_streams_df[
                large_vpu_streams_df["LINKNO"] == comid
            ]["strmOrder"].values
            rp_data = return_period_data[return_period_data.index == comid]
            largeflows = check_for_return_period_flow(
                largeflows, forecasted_flows, order, rp_data
            )

    # add the forecasted flows to the forecast records file for this vpu
    logging.info("  updating the forecast records file")
    try:
        update_forecast_records(
            vpu, forecast_records, rapid_output, year, first_day_flows, times
        )
    except Exception as e:
        logging.info("  unexpected error updating the forecast records")
        logging.info(e)

    largeflows = (
        largeflows.merge(
            streams_df[["lat", "lon", "LINKNO"]],
            how="inner",
            left_on="comid",
            right_on="LINKNO",
        )
        .drop(columns=["comid"])
        .replace({"": np.nan})
    )
    largeflows.to_parquet(
        os.path.join(rapid_output, f"forecastwarnings_{vpu}.parquet")
    )

    return


def update_forecast_records(
    vpu, forecast_records, qout_dir, year, first_day_flows, times
):
    if not os.path.exists(forecast_records):
        os.mkdir(forecast_records)

    record_path = os.path.join(
        forecast_records, f"forecastrecord_{vpu}_{year}.nc"
    )

    # if there isn't a forecast record for this year, make one
    if not os.path.exists(record_path):
        # using a forecast file as a reference
        reference = glob.glob(os.path.join(qout_dir, f"Qout_{vpu}_*.nc"))[0]
        reference = nc.Dataset(reference)
        # make a new record file
        record = nc.Dataset(record_path, "w")
        # copy the right dimensions and variables
        record.createDimension("time", None)
        record.createDimension("rivid", reference.dimensions["rivid"].size)
        record.createVariable(
            "time", reference.variables["time"].dtype, dimensions=("time",)
        )
        record.createVariable(
            "lat", reference.variables["lat"].dtype, dimensions=("rivid",)
        )
        record.createVariable(
            "lon", reference.variables["lon"].dtype, dimensions=("rivid",)
        )
        record.createVariable(
            "rivid", reference.variables["rivid"].dtype, dimensions=("rivid",)
        )
        record.createVariable(
            "Qout",
            reference.variables["Qout"].dtype,
            dimensions=("time", "rivid"),
            fill_value=np.nan,
        )
        # and also prepopulate the lat, lon, and rivid fields
        record.variables["rivid"][:] = reference.variables["rivid"][:]
        record.variables["lat"][:] = reference.variables["lat"][:]
        record.variables["lon"][:] = reference.variables["lon"][:]

        # set the time variable attributes
        record.variables["time"].setncattr(
            "units", f"hours since {year}0101 00:00:00"
        )

        # calculate the number of 3-hourly timesteps that will occur this year
        # and store them in the time variable
        date = datetime.datetime(
            year=int(year), month=1, day=1, hour=0, minute=0, second=0
        )
        end = int(year) + 1
        timesteps = 0
        while date.year < end:
            date += datetime.timedelta(hours=3)
            timesteps += 1
        record.variables["time"][:] = [i * 3 for i in range(timesteps)]
        record.close()

    # open the record netcdf
    logging.info("  writing first day flows to forecast record netcdf")
    record_netcdf = nc.Dataset(record_path, mode="a")

    # figure out the right times
    startdate = datetime.datetime(
        year=int(year), month=1, day=1, hour=0, minute=0, second=0
    )
    record_times = [
        startdate + datetime.timedelta(hours=int(i))
        for i in record_netcdf.variables["time"][:]
    ]
    start_time_index = record_times.index(times[0])
    end_time_index = start_time_index + len(first_day_flows[0])
    # convert all those saved flows to a np array and write to the netcdf
    first_day_flows = np.asarray(first_day_flows)
    record_netcdf.variables["Qout"][
        start_time_index:end_time_index, :
    ] = first_day_flows

    # save and close the netcdf
    record_netcdf.sync()
    record_netcdf.close()
    netcdf_forecast_record_to_zarr(record_path)

    return
def netcdf_forecast_record_to_zarr(record_path) -> None:
    """
    Converts the netcdf forecast record to zarr.

    Args:
        record_path (str): Path to the forecast_record netcdf file.
    """
    
    logging.info("Converting the forecast record to zarr")
    zarr_path = record_path.replace(".nc", ".zarr")
    record_nc = xr.open_dataset(record_path)
    
    with dask.config.set(**{
        'array.slicing.split_large_chunks': False,
        # set the max chunk size to 5MB
        'array.chunk-size': '40MB',
        # use the threads scheduler
        'scheduler': 'threads',
        # set the maximum memory target usage to 90% of total memory
        'distributed.worker.memory.target': 0.80,
        # do not allow spilling to disk
        'distributed.worker.memory.spill': False,
        # specify the amount of resources to allocate to dask workers
        'distributed.worker.resources': {
            'memory': 3e9,  # 1e9=1GB, this is the amount per worker
            'cpu': os.cpu_count(),  # num CPU per worker
        }
    }):
    #set compressing information
        logging.info("Configuring compression")
        
        #if we get rid of dask, we can get rid of the compressor
        #the compressor throws an error for version 3 so specify version 2
        compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
        encoding = {'Qout': {"compressor": compressor}}
        
        logging.info("Writing to zarr")
        (
            record_nc
            .drop_vars(["lat", "lon"])
                    .chunk({
                        "time": -1,
                        "rivid": "auto"
                    })
                    .to_zarr(
                        zarr_path,
                        consolidated=True,
                        encoding=encoding,
                        mode = 'w',
                        zarr_version=2
                    )
                )

        record_nc.close()
    logging.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "workspace",
        nargs=1,
        help="path to the daily workspace directory",
    )
    parser.add_argument(
        "vpu",
        nargs=1,
        help="VPU number",
    )
    parser.add_argument(
        "output_dir",
        nargs=1,
        help="path to the forecast records output directory",
    )

    args = parser.parse_args()
    workspace = args.workspace[0]
    vpu = args.vpu[0]
    rapid_input = os.path.join(workspace, "input")
    rapid_output = os.path.join(workspace, "output")
    returnperiods = os.path.join(workspace, "return_periods_dir")
    forecast_records = args.output_dir[0]
    rapid_output = os.path.join(workspace, "output")

    # start logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    postprocess_vpu(
        vpu, rapid_input, rapid_output, returnperiods, forecast_records
    )

    logging.info("Finished at " + datetime.datetime.now().strftime("%c"))
