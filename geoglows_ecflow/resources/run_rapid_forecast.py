import sys
import os
import datetime
import json
from tempfile import TemporaryDirectory
from glob import glob
from RAPIDpy import RAPID
from geoglows_ecflow.resources.helper_functions import (
    create_logger,
    get_ensemble_number_from_forecast,
    case_insensitive_file_search,
    get_valid_vpucode_list,
    compute_initial_rapid_flows,
    find_current_rapid_output,
)
from shutil import move, rmtree
from basininflow.inflow import create_inflow_file


with open(os.path.join(str(sys.argv[1]), "rapid_run.json"), "r") as f:
    data = json.load(f)
    job_id = sys.argv[2]

    date = data[job_id]["date"]
    runoff = data[job_id]["runoff"]
    vpucode = data[job_id]["vpu"]
    rapid_input_directory = data[job_id]["input_dir"]
    master_rapid_outflow_file = data[job_id]["output_file"]
    initialize_flows = data[job_id]["init_flows"]

    job_name = job_id.replace("job", f"job_{date}")
    rapid_executable_location = sys.argv[3]
    mp_execute_directory = sys.argv[4]
    subprocess_forecast_log_dir = sys.argv[5]

    rapid_logger = create_logger(
        "rapid_run_logger",
        "DEBUG",
        os.path.join(subprocess_forecast_log_dir, f"{job_name}.log"),
    )

    rapid_logger.info(f"Preparing {job_name} directories.")
    execute_directory = os.path.join(mp_execute_directory, job_name)
    output_base_dir = os.path.dirname(master_rapid_outflow_file)
    try:
        if not os.path.exists(execute_directory):
            os.mkdir(execute_directory)
    except OSError as e:
        raise OSError(f"Failed to create {execute_directory}: {e}")

    try:
        if not os.path.exists(output_base_dir):
            os.makedirs(output_base_dir)
    except OSError as e:
        raise OSError(f"Failed to create {output_base_dir}: {e}")

    time_start_all = datetime.datetime.utcnow()
    rapid_logger.info(f"Creating inflow files for {job_name}.")

    os.chdir(execute_directory)
    ens_number = get_ensemble_number_from_forecast(runoff)

    # prepare ECMWF file for RAPID
    rapid_logger.info(
        f"Running RAPID downscaling for date: {date}, "
        f"vpu: {vpucode}, ensemble: {ens_number}."
    )

    # set up RAPID manager
    try:
        rapid_connect_file = case_insensitive_file_search(
            rapid_input_directory, r"rapid_connect\.csv"
        )
        riv_bas_id_file = case_insensitive_file_search(
            rapid_input_directory, r"riv_bas_id.*?\.csv"
        )
        comid_lat_lon_z_file = case_insensitive_file_search(
            rapid_input_directory, r"comid_lat_lon_z.*?\.csv"
        )
        weight_table = case_insensitive_file_search(
            rapid_input_directory, r"weight_ifs_48r1.*?\.csv"
        )
        k_file = case_insensitive_file_search(rapid_input_directory, r"k\.csv")
        x_file = case_insensitive_file_search(rapid_input_directory, r"x\.csv")
    except Exception as e:
        rapid_logger.critical(f"input file not found: {e}")
        raise

    rapid_manager = RAPID(
        rapid_executable_location=rapid_executable_location,
        rapid_connect_file=rapid_connect_file,
        riv_bas_id_file=riv_bas_id_file,
        k_file=k_file,
        x_file=x_file,
        ZS_dtM=3 * 60 * 60,  # RAPID internal loop time interval
    )

    # check for forcing flows
    try:
        Qfor_file = case_insensitive_file_search(
            rapid_input_directory, r"qfor\.csv"
        )
        for_tot_id_file = case_insensitive_file_search(
            rapid_input_directory, r"for_tot_id\.csv"
        )
        for_use_id_file = case_insensitive_file_search(
            rapid_input_directory, r"for_use_id\.csv"
        )

        rapid_manager.update_parameters(
            Qfor_file=Qfor_file,
            for_tot_id_file=for_tot_id_file,
            for_use_id_file=for_use_id_file,
            ZS_dtF=3 * 60 * 60,  # forcing time interval
            BS_opt_for=True,
        )
    except Exception:
        rapid_logger.info("Forcing files not found. Skipping forcing ...")
        pass

    rapid_manager.update_reach_number_data()

    outflow_file_name = os.path.join(
        execute_directory, f"Qout_{vpucode}_{ens_number}.nc"
    )

    qinit_file = ""
    BS_opt_Qinit = False
    if initialize_flows:
        # Look for qinit files for the past 3 days;
        # Try seasonal average file if not
        for day in [24, 48, 72]:
            past_date = (
                datetime.datetime.strptime(date[:11], "%Y%m%d.%H")
                - datetime.timedelta(hours=day)
            ).strftime("%Y%m%dt%H")
            qinit_file = os.path.join(
                rapid_input_directory, f"Qinit_{past_date}.nc"
            )
            BS_opt_Qinit = qinit_file and os.path.exists(qinit_file)
            if BS_opt_Qinit:
                break

        if not BS_opt_Qinit:
            print(
                "Qinit file not found. "
                "Trying to initialize from Seasonal Averages ..."
            )
            try:
                qinit_file = glob(
                    os.path.join(rapid_input_directory, "seasonal_qinit*.nc")
                )[0]
                BS_opt_Qinit = qinit_file and os.path.exists(qinit_file)
            except Exception as e:
                print(f"Failed to initialize from Seasonal Averages: {e}.")
                print(f"WARNING: {qinit_file} not found. Not initializing ...")
                qinit_file = ""

    # with TemporaryDirectory() as temp_dir:
    if True:
        temp_dir = "/home/michael/geoglows_ecflow/data/rapid_io"
        inflow_dir = os.path.join(temp_dir, "inflows")

        create_inflow_file(
            lsm_data=runoff,
            input_dir=rapid_input_directory,
            inflow_dir=inflow_dir,
            weight_table=weight_table,
            comid_lat_lon_z=comid_lat_lon_z_file,
            cumulative=True,
        )

        inflow_file_path = case_insensitive_file_search(
            inflow_dir, rf"m3_{vpucode}.*?\.nc"
        )

        try:
            interval_3hr = 3 * 60 * 60  # 3 hours
            duration_3hr = 360 * 60 * 60  # 360 hours (15 days)
            rapid_manager.update_parameters(
                ZS_TauR=interval_3hr,  # time step of runoff data
                ZS_dtR=15 * 60,  # internal routing time step
                ZS_TauM=duration_3hr,  # total simulation time
                ZS_dtM=interval_3hr,  # RAPID internal loop time interval
                ZS_dtF=interval_3hr,  # forcing time interval
                Vlat_file=inflow_file_path,
                Qout_file=outflow_file_name,
                Qinit_file=qinit_file,
                BS_opt_Qinit=BS_opt_Qinit,
            )
            rapid_manager.run()
        except Exception as e:
            rapid_logger.critical(f"Failed to run RAPID: {e}.")
            raise

        time_stop_all = datetime.datetime.utcnow()
        delta_time = time_stop_all - time_start_all
        rapid_logger.info(f"Total time to compute: {delta_time}")

    node_rapid_outflow_file = os.path.join(
        execute_directory, os.path.basename(master_rapid_outflow_file)
    )

    move(node_rapid_outflow_file, master_rapid_outflow_file)
    rmtree(execute_directory)

    # # TODO: Extract to a separate init flows task
    # # get list of correclty formatted rapid input directories in rapid directory
    # rapid_io_files_location = rapid_input_directory.split('/input')[0]
    # rapid_input_directories = get_valid_vpucode_list(os.path.join(rapid_io_files_location, "input"))

    # for rapid_input_dir in rapid_input_directories:
    #     # initialize flows for next run
    #     input_directory = os.path.join(rapid_io_files_location,
    #                                    'input',
    #                                    rapid_input_dir)

    #     forecast_directory = os.path.join(rapid_io_files_location,
    #                                       'output',
    #                                       rapid_input_dir,
    #                                       date)

    #     if os.path.exists(forecast_directory):
    #         basin_files = find_current_rapid_output(forecast_directory, vpucode)
    #         try:
    #             compute_initial_rapid_flows(basin_files, input_directory, date)
    #         except Exception as ex:
    #             print(ex)
    #             pass
