import datetime
import glob
import logging
import os
import sys
import re

import numpy as np
import pandas as pd
import xarray
import netCDF4 as nc


def merge_forecast_qout_files(rapidio_vpu_output):
    # pick the most recent date, append to the file path
    dates = []
    for elem in os.listdir(rapidio_vpu_output):
        regex = "\d{8}\.\d{2}"
        match = re.search(regex, elem)
        if match:
            dates.append(match.group())

    recent_date = sorted(dates)[-1]
    qout_dir = os.path.join(rapidio_vpu_output, recent_date)

    # list the forecast files
    prediction_files = sorted(glob.glob(os.path.join(qout_dir, "Qout*.nc")))

    # merge them into a single file joined by ensemble number
    ensemble_index_list = []
    qout_datasets = []
    for forecast_nc in prediction_files:
        ensemble_index_list.append(
            int(os.path.basename(forecast_nc)[:-3].split("_")[-1])
        )
        qout_datasets.append(xarray.open_dataset(forecast_nc).Qout)
    return (
        xarray.concat(
            qout_datasets, pd.Index(ensemble_index_list, name="ensemble")
        ),
        qout_dir,
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
        date_r2 = get_time_of_first_exceedence(forecasted_flows_df, r2)
    # if the flow is not larger than the smallest return period, return the
    # dataframe without appending anything
    else:
        return largeflows_df

    # check the rest of the return period flow levels
    if max_flow >= r5:
        date_r5 = get_time_of_first_exceedence(forecasted_flows_df, r5)
    if max_flow >= r10:
        date_r10 = get_time_of_first_exceedence(forecasted_flows_df, r10)
    if max_flow >= r25:
        date_r25 = get_time_of_first_exceedence(forecasted_flows_df, r25)
    if max_flow >= r50:
        date_r50 = get_time_of_first_exceedence(forecasted_flows_df, r50)
    if max_flow >= r100:
        date_r100 = get_time_of_first_exceedence(forecasted_flows_df, r100)

    try:
        lat = float(rp_data["lat"].values)
        lon = float(rp_data["lon"].values)
    except:
        lat = ""
        lon = ""

    new_row = pd.DataFrame(
        {
            "comid": rp_data.index[0],
            "stream_order": stream_order,
            "stream_lat": lat,
            "stream_lon": lon,
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


def get_time_of_first_exceedence(forecasted_flows_df, flow):
    # replace the flows that are too small (don't exceed the return period)
    forecasted_flows_df[forecasted_flows_df.means < flow] = np.nan
    daily_flows = forecasted_flows_df.dropna()
    return daily_flows["times"].values[0]


def postprocess_vpu(vpu, rapidio, historical_sim, forecast_records):
    # build the propert directory paths
    rapidio_input = os.path.join(rapidio, "input")
    rapidio_vpu_output = os.path.join(rapidio, "output", vpu)

    # make the pandas dataframe to store the summary info
    largeflows = pd.DataFrame(
        columns=[
            "comid",
            "stream_order",
            "stream_lat",
            "stream_lon",
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
    merged_forecasts, qout_dir = merge_forecast_qout_files(rapidio_vpu_output)

    # collect the times and comids from the forecasts
    logging.info("  reading info from forecasts")
    times = pd.to_datetime(pd.Series(merged_forecasts.time))
    comids = pd.Series(merged_forecasts.rivid)
    tomorrow = times[0] + pd.Timedelta(days=1)
    year = times[0].strftime("%Y")

    # read the return period file
    logging.info("  reading return period file")
    return_period_file = os.path.join(
        historical_sim, vpu, f"returnperiods_{vpu}.nc"
    )
    return_period_data = xarray.open_dataset(return_period_file).to_dataframe()

    # read the list of large streams
    logging.info("  creating dataframe of large streams")
    streams_file_path = os.path.join(rapidio_input, "master_table.parquet")
    streams_df = pd.read_parquet(streams_file_path)
    large_vpu_streams_df = streams_df[
        (streams_df["VPUCode"] == int(vpu)) & ((streams_df["strmOrder"] >= 2))
    ]

    # get the list of comids
    large_list = large_vpu_streams_df["TDXHydroLinkNo"].tolist()

    # store the first day flows in a huge array
    logging.info("  beginning to iterate over the comids")
    first_day_flows = []

    # now process the mean flows for each river in the vpu
    for comid in comids:
        # compute the timeseries of average flows
        means = np.array(merged_forecasts.sel(rivid=comid)).mean(axis=0)
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
                large_vpu_streams_df["TDXHydroLinkNo"] == comid
            ]["strmOrder"].values
            rp_data = return_period_data[return_period_data.index == comid]
            largeflows = check_for_return_period_flow(
                largeflows, forecasted_flows, order, rp_data
            )

    # add the forecasted flows to the forecast records file for this vpu
    logging.info("  updating the forecast records file")
    try:
        update_forecast_records(
            vpu, forecast_records, qout_dir, year, first_day_flows, times
        )
    except Exception as excp:
        logging.info("  unexpected error updating the forecast records")
        logging.info(excp)

    # now save the return periods summary csv to the right output directory
    largeflows.to_csv(
        os.path.join(qout_dir, "forecasted_return_periods_summary.csv"),
        index=False,
    )

    return


def update_forecast_records(
    vpu, forecast_records, qout_dir, year, first_day_flows, times
):
    breakpoint()
    record_path = os.path.join(forecast_records, vpu)
    if not os.path.exists(record_path):
        os.mkdir(record_path)
    record_path = os.path.join(record_path, f"forecast_record-{year}-{vpu}.nc")

    # if there isn't a forecast record for this year, make one
    if not os.path.exists(record_path):
        # using a forecast file as a reference
        reference = glob.glob(os.path.join(qout_dir, "Qout*.nc"))[0]
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
            dimensions=("rivid", "time"),
        )
        # and also prepopulate the lat, lon, and rivid fields
        record.variables["rivid"][:] = reference.variables["rivid"][:]
        record.variables["lat"][:] = reference.variables["lat"][:]
        record.variables["lon"][:] = reference.variables["lon"][:]

        # set the time variable attributes so that the
        record.variables["time"].setncattr(
            "units", "hours since {0}0101 00:00:00".format(year)
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
        :, start_time_index:end_time_index
    ] = first_day_flows

    # save and close the netcdf
    record_netcdf.sync()
    record_netcdf.close()

    return


if __name__ == "__main__":
    """
    arg1 = path to the rapid-io directory where the input and output directory
        are located. You need the input directory because thats where the
        master_table.parquet file is located. outputs contain the forecst
        outputs
    arg2 = path to directory where the historical data are stored. the folder
        that contains 1 folder for each vpu.
    arg3 = path to the directory where the 1day forecasts are saved. the folder
        that contains 1 folder for each vpu.
    arg4 = path to the logs directory
    """
    # accept the arguments
    rapidio = sys.argv[1]
    historical_sim = sys.argv[2]
    forecast_records = sys.argv[3]
    logs_dir = sys.argv[4]

    # list of vpu to be processed based on their forecasts
    vpu_list = os.listdir(os.path.join(rapidio, "output"))

    # start logging
    start = datetime.datetime.now()
    log = os.path.join(
        logs_dir, "postprocess_forecasts-" + start.strftime("%Y%m%d")
    )
    logging.basicConfig(filename=log, filemode="w", level=logging.INFO)
    logging.info(
        "postprocess_flow_forecasts.py initiated " + start.strftime("%c")
    )

    for vpu in vpu_list:
        try:
            # log start messages
            logging.info("")
            logging.info("WORKING ON VPU: " + vpu)
            logging.info(
                "  elapsed time: " + str(datetime.datetime.now() - start)
            )
            # attempt to postprocess the vpu
            postprocess_vpu(vpu, rapidio, historical_sim, forecast_records)
        except Exception as e:
            logging.info(e)
            logging.info(
                "      VPU failed at " + datetime.datetime.now().strftime("%c")
            )

    logging.info("")
    logging.info("Finished at " + datetime.datetime.now().strftime("%c"))
    logging.info("Total elapsed time: " + str(datetime.datetime.now() - start))
