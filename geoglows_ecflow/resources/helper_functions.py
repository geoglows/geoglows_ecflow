# Original code by Alan Snow, 2017 (released under BSD 3-Clause License)
# See: spt_compute (https://github.com/erdc/spt_compute)
# Updated by Michael Souffront, 2023

import os
import sys
import re
import datetime
import numpy as np
import netCDF4 as nc
import logging as log
from glob import glob
from RAPIDpy.dataset import RAPIDDataset


def create_logger(
    name: str,
    level: str = 'INFO',
    log_file: str | None = None
) -> log.Logger:
    # Create a logger
    logger = log.getLogger(name)
    logger.setLevel(level)

    if log_file:
        # Create file handler
        handler = log.FileHandler(log_file)
    else:
        handler = log.StreamHandler(sys.stdout)

        handler.setLevel(level)
        handler.setFormatter(
            log.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )

        # Add the handler to the logger
        logger.addHandler(handler)

    return logger


def get_date_timestep_from_forecast_dir(
    forecast_dir: str,
    log: log.Logger = create_logger(__name__)
) -> str:
    """Gets the datetime from a forecast directory.

    Args:
        forecast_dir (str): Path representing forecast directory.
        log (log.Logger, optional): Logger. Defaults to log.

    Returns:
        A string representing the datetime in the format "%Y%m%d.%H".

    Raises:
        AttributeError: If forecast_dir is not found or in the expected format.
    """
    # Get the forecast datetime from the forecast directory
    forecast_date_timestep = os.path.basename(forecast_dir)
    log.info(f'Forecast timestep {forecast_date_timestep}')

    # Parse forecast datetime
    date_time_regex = '\d{8}\.\d{2}'
    try:
        match = re.search(date_time_regex, forecast_date_timestep)
        date_time_str = match.group()
        return date_time_str
    except AttributeError:
        raise AttributeError("No datetime match found.")


def get_valid_vpucode_list(input_directory: str) -> list[str]:
    """
    Get a list of vpucodes from the input directory.

    Args:
        input_directory (str): Path to the rapid input directory.

    Returns:
        list[str]: List of valid directories (vpucodes).
    """
    valid_input_directories = []
    # Append to valid_input_directories if directory, skip if not
    for directory in os.listdir(input_directory):
        if os.path.isdir(os.path.join(input_directory, directory)):
            valid_input_directories.append(directory)
        else:
            print(f"{directory} not a directory. Skipping...")
    return valid_input_directories


def find_current_rapid_output(forecast_directory, vpucode):
    """
    Finds the most current files output from RAPID
    """
    if os.path.exists(forecast_directory):
        basin_files = glob(os.path.join(forecast_directory,
                                        f"Qout_{vpucode}_{1}_*.nc"))
        if len(basin_files) > 0:
            return basin_files
    # there are none found
    return None


def get_ensemble_number_from_forecast(forecast_name: str) -> int:
    """Gets the datetimestep from forecast.

    Args:
        forecast_name (str): Path to or name of the forecast file.
            E.g. "1.runoff.nc".

    Returns:
        int: The ensemble number.
    """
    forecast_split = os.path.basename(forecast_name).split(".")
    if forecast_name.endswith(".205.runoff.grib.runoff.netcdf"):
        ensemble_number = int(forecast_split[2])
    else:
        ensemble_number = int(forecast_split[0])
    return ensemble_number


def case_insensitive_file_search(directory: str, pattern: str) -> str:
    """Looks for file with pattern with case insensitive search.

    Args:
        directory (str): Path to directory to search.
        pattern (str): Pattern to search for.

    Returns:
        (str): Path to file with pattern.
    """
    try:
        file_path = os.path.join(
            directory,
            [filename for filename in os.listdir(directory)
                if re.search(pattern, filename, re.IGNORECASE)][0]
        )

        return file_path
    except IndexError:
        print(f"{pattern} not found")
        raise


def _cleanup_past_qinit(input_directory):
    """
    Removes past qinit files.

    :param input_directory:
    :return:
    """
    past_init_flow_files = glob(os.path.join(input_directory, 'Qinit_*'))
    for past_init_flow_file in past_init_flow_files:
        try:
            os.remove(past_init_flow_file)
        except:
            pass


def compute_initial_rapid_flows(prediction_files, input_directory, forecast_date_timestep):
    """
    Gets mean of all 52 ensembles 12-hrs in future and prints to netcdf (nc) or CSV (csv) format as initial flow
    Qinit_file (BS_opt_Qinit)
    The assumptions are that Qinit_file is ordered the same way as rapid_connect_file
    if subset of list, add zero where there is no flow
    """
    #remove old init files for this basin
    _cleanup_past_qinit(input_directory)
    current_forecast_date = datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H")
    current_forecast_date_string = current_forecast_date.strftime("%Y%m%dt%H")
    init_file_location = os.path.join(input_directory,'Qinit_%s.nc' % current_forecast_date_string)
    #check to see if exists and only perform operation once
    if prediction_files:
        sni = StreamNetworkInitializer(connectivity_file=os.path.join(input_directory,'rapid_connect.csv'))
        sni.compute_init_flows_from_past_forecast(prediction_files)
        sni.write_init_flow_file(init_file_location)
    else:
        print("No current forecasts found. Skipping ...")


class StreamNetworkInitializer(object):
    def __init__(self, connectivity_file, gage_ids_natur_flow_file=None):
        #files
        self.connectivity_file = connectivity_file
        self.gage_ids_natur_flow_file = gage_ids_natur_flow_file
        #variables
        self.stream_segments = []
        self.outlet_id_list = []
        self.stream_undex_with_usgs_station = []
        self.stream_id_array = None

        #generate the network
        self._generate_network_from_connectivity()

        #add gage id and natur flow to network
        if gage_ids_natur_flow_file != None:
            if os.path.exists(gage_ids_natur_flow_file) and gage_ids_natur_flow_file:
                self._add_gage_ids_natur_flow_to_network()


    def compute_init_flows_from_past_forecast(self, forecasted_streamflow_files):
        """
        Compute initial flows from the past ECMWF forecast ensemble
        """
        if forecasted_streamflow_files:
            #get list of COMIDS
            print("Computing initial flows from the past ECMWF forecast ensemble ...")
            with RAPIDDataset(forecasted_streamflow_files[0]) as qout_nc:
                comid_index_list, reordered_comid_list, ignored_comid_list = qout_nc.get_subset_riverid_index_list(self.stream_id_array)
            print("Extracting data ...")
            reach_prediciton_array = np.zeros((len(self.stream_id_array),len(forecasted_streamflow_files),1))
            #get information from datasets
            for file_index, forecasted_streamflow_file in enumerate(forecasted_streamflow_files):
                try:
                    ensemble_index = int(os.path.basename(forecasted_streamflow_file).split(".")[0].split("_")[-1])
                    try:
                        #Get hydrograph data from ECMWF Ensemble
                        with RAPIDDataset(forecasted_streamflow_file) as predicted_qout_nc:
                            time_length = predicted_qout_nc.size_time
                            if not predicted_qout_nc.is_time_variable_valid():
                                #data is raw rapid output
                                data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list,
                                                                                        time_index=1)
                            else:
                                #the data is CF compliant and has time=0 added to output
                                if ensemble_index == 52:
                                    if time_length == 125:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list,
                                                                                                time_index=24)
                                    else:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list,
                                                                                                time_index=4)
                                else:
                                    if time_length == 85:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list,
                                                                                                time_index=8)
                                    else:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list,
                                                                                                time_index=4)
                    except Exception:
                        print("Invalid ECMWF forecast file {0}".format(forecasted_streamflow_file))
                        continue
                    #organize the data
                    for comid_index, comid in enumerate(reordered_comid_list):
                        reach_prediciton_array[comid_index][file_index] = data_values_2d_array[comid_index]
                except Exception as e:
                    print(e)
                    #pass

            print("Analyzing data ...")
            for index in range(len(self.stream_segments)):
                try:
                    #get where comids are in netcdf file
                    data_index = np.where(reordered_comid_list==self.stream_segments[index].stream_id)[0][0]
                    self.stream_segments[index].init_flow = np.mean(reach_prediciton_array[data_index])
                except Exception:
                    #stream id not found in list. Adding zero init flow ...
                    self.stream_segments[index].init_flow = 0
                    pass
                    continue

            print("Initialization Complete!")


    def write_init_flow_file(self, out_file):
        """
        Write initial flow file
        """
        print("Writing to initial flow file: {0}".format(out_file))
        if out_file.endswith(".csv"):
            with open(out_file, 'w') as init_flow_file:
                for stream_index, stream_segment in enumerate(self.stream_segments):
                    if stream_segment.station_flow != None:
                        init_flow_file.write("{}\n".format(stream_segment.station_flow))
                    else:
                        init_flow_file.write("{}\n".format(stream_segment.init_flow))
        else:
            init_flows_array = np.zeros(len(self.stream_segments))
            for stream_index, stream_segment in enumerate(self.stream_segments):
                try:
                    if stream_segment.station_flow != None:
                        init_flows_array[stream_index] = stream_segment.station_flow
                    else:
                        init_flows_array[stream_index] = stream_segment.init_flow
                except IndexError:
                    log('stream index not found', "WARNING")
            with nc.Dataset(out_file, 'w', format="NETCDF3_CLASSIC") as init_flow_file:
                init_flow_file.createDimension('Time', 1)
                init_flow_file.createDimension('rivid', len(self.stream_segments))
                var_Qout = init_flow_file.createVariable('Qout', 'f8', ('Time', 'rivid',))
                var_Qout[:] = init_flows_array
