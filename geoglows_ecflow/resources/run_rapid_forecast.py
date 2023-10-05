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
)
from shutil import move, rmtree
from basininflow.inflow import create_inflow_file


def rapid_forecast_exec(
    ecflow_home: str,
    job_id: str,
    rapid_executable_location: str,
    mp_execute_directory: str,
    subprocess_forecast_log_dir: str,
) -> None:
    with open(os.path.join(ecflow_home, "rapid_run.json"), "r") as f:
        data = json.load(f)
        for date, job_list in data["dates"].items():
            # Get job name
            job_name = job_id.replace("job", f"job_{date}")

            # Get job
            job = [job.get(job_id, {}) for job in job_list if job_id in job][0]

            # Check if job is empty
            if not job:
                raise ValueError(f"Job {job_id} not found.")

            runoff = job["runoff"]
            vpucode = job["vpu"]
            rapid_input_directory = job["input_dir"]
            master_rapid_outflow_file = job["output_file"]
            initialize_flows = job["init_flows"]

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
                k_file = case_insensitive_file_search(
                    rapid_input_directory, r"k\.csv"
                )
                x_file = case_insensitive_file_search(
                    rapid_input_directory, r"x\.csv"
                )
            except Exception as e:
                rapid_logger.critical(f"input file not found: {e}")
                raise

            # Create RAPID manager (Init RAPID class)
            rapid_manager = RAPID(
                rapid_executable_location=rapid_executable_location,
                rapid_connect_file=rapid_connect_file,
                riv_bas_id_file=riv_bas_id_file,
                k_file=k_file,
                x_file=x_file,
                ZS_dtM=3 * 60 * 60,  # Assume 3hr time step
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
                rapid_logger.info(
                    "Forcing files not found. Skipping forcing ..."
                )
                pass

            rapid_manager.update_reach_number_data()

            outflow_file_name = os.path.join(
                execute_directory, f"Qout_{vpucode}_{ens_number}.nc"
            )

            # Get qinit file
            qinit_file = ""
            BS_opt_Qinit = False
            if initialize_flows:
                # Look for qinit files for the past 3 days;
                # Try seasonal average file if not
                for day in [24, 48, 72]:
                    past_date = (
                        datetime.datetime.strptime(date, "%Y%m%d.%H")
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
                            os.path.join(
                                rapid_input_directory, "seasonal_qinit*.nc"
                            )
                        )[0]
                        BS_opt_Qinit = qinit_file and os.path.exists(
                            qinit_file
                        )
                    except Exception:
                        print(
                            f"Failed to initialize from Seasonal Averages."
                        )
                        print(
                            f"WARNING: {qinit_file} not found. "
                            "Not initializing ..."
                        )
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
                    file_label=ens_number,
                )

                # Get forecast chronometry
                interval = 3 if ens_number < 52 else 1
                duration = 360 if ens_number < 52 else 240

                start_date = date.split(".")[0]
                end_date = (
                    datetime.datetime.strptime(start_date, "%Y%m%d")
                    + datetime.timedelta(hours=duration)
                ).strftime("%Y%m%d")

                # Get inflow file path
                inflow_file_path = os.path.join(
                    inflow_dir,
                    f"m3_{vpucode}_{start_date}_{end_date}_{ens_number}.nc",
                )

                try:
                    rapid_manager.update_parameters(
                        ZS_TauR=interval * 60 * 60,
                        ZS_dtR=15 * 60,
                        ZS_TauM=duration * 60 * 60,
                        ZS_dtM=interval * 60 * 60,
                        ZS_dtF=interval * 60 * 60,
                        Vlat_file=inflow_file_path,
                        Qout_file=outflow_file_name,
                        Qinit_file=qinit_file,
                        BS_opt_Qinit=BS_opt_Qinit,
                    )

                    # run RAPID
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
            # rmtree(execute_directory)


if __name__ == "__main__":
    ecflow_home = sys.argv[1]
    job_id = sys.argv[2]
    rapid_executable_location = sys.argv[3]
    mp_execute_directory = sys.argv[4]
    subprocess_forecast_log_dir = sys.argv[5]
    rapid_forecast_exec(
        ecflow_home,
        job_id,
        rapid_executable_location,
        mp_execute_directory,
        subprocess_forecast_log_dir,
    )
