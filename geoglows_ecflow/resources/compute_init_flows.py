import os
import sys
import json
import datetime
import numpy as np
import netCDF4 as nc
from glob import glob
from RAPIDpy.dataset import RAPIDDataset
from RAPIDpy.helper_functions import csv_to_list
from geoglows_ecflow.resources.helper_functions import (
    create_logger,
    find_current_rapid_output,
)


class StreamSegment(object):
    def __init__(
        self,
        stream_id,
        down_id,
        up_id_array,
        init_flow=0,
        station=None,
        station_flow=None,
        station_distance=None,
        natural_flow=None,
    ):
        self.stream_id = stream_id
        self.down_id = down_id  # downstream segment id
        self.up_id_array = (
            up_id_array  # array of atream ids for upstream segments
        )
        self.init_flow = init_flow
        self.station = station
        self.station_flow = station_flow
        self.station_distance = (
            station_distance  # number of tream segments to station
        )
        self.natural_flow = natural_flow


class StreamNetworkInitializer(object):
    def __init__(self, connectivity_file, gage_ids_natur_flow_file=None):
        # files
        self.connectivity_file = connectivity_file
        self.gage_ids_natur_flow_file = gage_ids_natur_flow_file
        # variables
        self.stream_segments = []
        self.outlet_id_list = []
        self.stream_undex_with_usgs_station = []
        self.stream_id_array = None

        # generate the network
        self._generate_network_from_connectivity()

        # add gage id and natur flow to network
        if gage_ids_natur_flow_file != None:
            if (
                os.path.exists(gage_ids_natur_flow_file)
                and gage_ids_natur_flow_file
            ):
                self._add_gage_ids_natur_flow_to_network()

    def compute_init_flows_from_past_forecast(
        self, forecasted_streamflow_files
    ):
        """
        Compute initial flows from the past ECMWF forecast ensemble
        """
        if forecasted_streamflow_files:
            # get list of COMIDS
            print(
                "Computing initial flows from the past ECMWF forecast ..."
            )
            with RAPIDDataset(forecasted_streamflow_files[0]) as qout_nc:
                (
                    comid_index_list,
                    reordered_comid_list,
                    ignored_comid_list,
                ) = qout_nc.get_subset_riverid_index_list(self.stream_id_array)
            print("Extracting data ...")
            reach_prediciton_array = np.zeros(
                (
                    len(self.stream_id_array),
                    len(forecasted_streamflow_files),
                    1,
                )
            )
            # get information from datasets
            for file_index, forecasted_streamflow_file in enumerate(
                forecasted_streamflow_files
            ):
                try:
                    ensemble_index = int(
                        os.path.basename(forecasted_streamflow_file)
                        .split(".")[0]
                        .split("_")[-1]
                    )
                    try:
                        # Get hydrograph data from ECMWF Ensemble
                        with RAPIDDataset(
                            forecasted_streamflow_file
                        ) as predicted_qout_nc:
                            time_length = predicted_qout_nc.size_time
                            if not predicted_qout_nc.is_time_variable_valid():
                                # data is raw rapid output
                                data_values_2d_array = (
                                    predicted_qout_nc.get_qout_index(
                                        comid_index_list, time_index=1
                                    )
                                )
                            else:
                                # data is CF compliant and ouput has time=0
                                if ensemble_index == 52:
                                    if time_length == 125:
                                        data_values_2d_array = (
                                            predicted_qout_nc.get_qout_index(
                                                comid_index_list, time_index=24
                                            )
                                        )
                                    else:
                                        data_values_2d_array = (
                                            predicted_qout_nc.get_qout_index(
                                                comid_index_list, time_index=4
                                            )
                                        )
                                else:
                                    if time_length == 85:
                                        data_values_2d_array = (
                                            predicted_qout_nc.get_qout_index(
                                                comid_index_list, time_index=8
                                            )
                                        )
                                    else:
                                        data_values_2d_array = (
                                            predicted_qout_nc.get_qout_index(
                                                comid_index_list, time_index=4
                                            )
                                        )
                    except Exception:
                        print(
                            "Invalid ECMWF forecast file {0}".format(
                                forecasted_streamflow_file
                            )
                        )
                        continue
                    # organize the data
                    for comid_index, comid in enumerate(reordered_comid_list):
                        reach_prediciton_array[comid_index][
                            file_index
                        ] = data_values_2d_array[comid_index]
                except Exception as e:
                    print(e)
                    # pass

            print("Analyzing data ...")
            for index in range(len(self.stream_segments)):
                try:
                    # get where comids are in netcdf file
                    data_index = np.where(
                        reordered_comid_list
                        == self.stream_segments[index].stream_id
                    )[0][0]
                    self.stream_segments[index].init_flow = np.mean(
                        reach_prediciton_array[data_index]
                    )
                except Exception:
                    # stream id not found in list. Adding zero init flow ...
                    self.stream_segments[index].init_flow = 0
                    pass
                    continue

            print("Initialization Complete!")

    def write_init_flow_file(self, out_file, log=create_logger(__name__)):
        """
        Write initial flow file
        """
        print("Writing to initial flow file: {0}".format(out_file))
        if out_file.endswith(".csv"):
            with open(out_file, "w") as init_flow_file:
                for stream_index, stream_segment in enumerate(
                    self.stream_segments
                ):
                    if stream_segment.station_flow != None:
                        init_flow_file.write(
                            "{}\n".format(stream_segment.station_flow)
                        )
                    else:
                        init_flow_file.write(
                            "{}\n".format(stream_segment.init_flow)
                        )
        else:
            init_flows_array = np.zeros(len(self.stream_segments))
            for stream_index, stream_segment in enumerate(
                self.stream_segments
            ):
                try:
                    if stream_segment.station_flow != None:
                        init_flows_array[
                            stream_index
                        ] = stream_segment.station_flow
                    else:
                        init_flows_array[
                            stream_index
                        ] = stream_segment.init_flow
                except IndexError:
                    log.warning("Stream index not found.")
            with nc.Dataset(
                out_file, "w", format="NETCDF3_CLASSIC"
            ) as init_flow_file:
                init_flow_file.createDimension("Time", 1)
                init_flow_file.createDimension(
                    "rivid", len(self.stream_segments)
                )
                var_Qout = init_flow_file.createVariable(
                    "Qout",
                    "f8",
                    (
                        "Time",
                        "rivid",
                    ),
                )
                var_Qout[:] = init_flows_array

    def _generate_network_from_connectivity(self):
        """
        Generate river network from connectivity file
        """
        print("Generating river network from connectivity file ...")
        connectivity_table = csv_to_list(self.connectivity_file)
        self.stream_id_array = np.array(
            [row[0] for row in connectivity_table], dtype=np.int64
        )
        # add each stream segment to network
        for connectivity_info in connectivity_table:
            stream_id = int(connectivity_info[0])
            downstream_id = int(connectivity_info[1])
            # add outlet to list of outlets if downstream id is zero
            if downstream_id == 0:
                self.outlet_id_list.append(stream_id)

            self.stream_segments.append(
                StreamSegment(
                    stream_id=stream_id,
                    down_id=downstream_id,
                    up_id_array=connectivity_info[
                        2 : 2 + int(connectivity_info[2])
                    ],
                )
            )


def _cleanup_past_qinit(input_directory):
    """
    Removes past qinit files.

    :param input_directory:
    :return:
    """
    past_init_flow_files = glob(os.path.join(input_directory, "Qinit_*"))
    for past_init_flow_file in past_init_flow_files:
        try:
            os.remove(past_init_flow_file)
        except:
            pass


def compute_init_rapid_flows(
    prediction_files, input_directory, forecast_date_timestep
):
    """Gets mean of all 52 ensembles 12-hrs in future and prints to netcdf (nc)
        or CSV (csv) format as initial flow Qinit_file (BS_opt_Qinit). The
        assumptions are that Qinit_file is ordered the same way as
        rapid_connect_file if subset of list, add zero where there is no flow.
    """
    # remove old init files for this basin
    _cleanup_past_qinit(input_directory)
    current_forecast_date = datetime.datetime.strptime(
        forecast_date_timestep[:11], "%Y%m%d.%H"
    )
    current_forecast_date_string = current_forecast_date.strftime("%Y%m%dt%H")
    init_file_location = os.path.join(
        input_directory, "Qinit_%s.nc" % current_forecast_date_string
    )
    # check to see if exists and only perform operation once
    if prediction_files:
        sni = StreamNetworkInitializer(
            connectivity_file=os.path.join(
                input_directory, "rapid_connect.csv"
            )
        )
        sni.compute_init_flows_from_past_forecast(prediction_files)
        sni.write_init_flow_file(init_file_location)
    else:
        print("No current forecasts found. Skipping ...")


def compute_all_rapid_init_flows(ecflow_home: str, vpu: str) -> None:
    with open(os.path.join(ecflow_home, "rapid_run.json"), "r") as f:
        data = json.load(f)
        rapid_input_dir = data["input_dir"]
        rapid_output_dir = data["output_dir"]
        for date in data["dates"].keys():
            # Initialize flows for next run
            input_directory = os.path.join(rapid_input_dir, vpu)

            forecast_directory = os.path.join(rapid_output_dir, vpu, date)

            if os.path.exists(forecast_directory):
                basin_files = find_current_rapid_output(
                    forecast_directory, vpu
                )
                try:
                    compute_init_rapid_flows(
                        basin_files, input_directory, date
                    )
                except Exception as ex:
                    print(ex)
                    pass


if __name__ == "__main__":
    ecflow_home = sys.argv[1]
    vpu = sys.argv[2]
    compute_all_rapid_init_flows(ecflow_home, vpu)