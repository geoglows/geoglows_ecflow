import os
import argparse
import datetime
import json
from glob import glob
from .RAPIDpy.rapid import RAPID
from geoglows_ecflow.resources.helper_functions import (
    create_logger,
    get_ensemble_number_from_forecast,
    case_insensitive_file_search,
)
from shutil import move
from basininflow.inflow import create_inflow_file


def rapid_forecast_exec(
    workspace: str,
    job_id: str,
    rapid_executable_location: str,
    mp_execute_directory: str,
    subprocess_forecast_log_dir: str,
) -> None:
    """Runs GEOGLOWS RAPID forecast.

    Args:
        workspace (str): Path to rapid_run.json.
        job_id (str): Job ID.
        rapid_executable_location (str): Path to RAPID executable.
        mp_execute_directory (str): Path intermediate directory for RAPID.
        subprocess_forecast_log_dir (str): Path to RAPID log directory.
    """
    if not os.path.exists(mp_execute_directory):
        os.mkdir(mp_execute_directory)
    if not os.path.exists(subprocess_forecast_log_dir):
        os.mkdir(subprocess_forecast_log_dir)

    with open(os.path.join(workspace, "rapid_run.json"), "r") as f:
        data = json.load(f)
        date = data["date"]

        # Get job
        job = data.get(job_id, {})

        # Check if job is empty
        if not job:
            raise ValueError(f"Job {job_id} not found.")

        runoff = job["runoff"]
        vpucode = job["vpu"]
        rapid_vpu_input_dir = job["input_dir"]
        master_rapid_outflow_file = job["output_file"]
        initialize_flows = job["init_flows"]

        rapid_logger = create_logger(
            "rapid_run_logger",
            "DEBUG",
            os.path.join(subprocess_forecast_log_dir, f"{job_id}.log"),
        )

        rapid_logger.info(f"Preparing {job_id} directories.")
        execute_directory = os.path.join(mp_execute_directory, job_id)
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
        rapid_logger.info(f"Creating inflow files for {job_id}.")

        os.chdir(execute_directory)
        ens_number = get_ensemble_number_from_forecast(runoff)

        # prepare ECMWF file for RAPID
        rapid_logger.info(
            f"Running RAPID downscaling for vpu: {vpucode}, " f"ensemble: {ens_number}."
        )

        # set up RAPID manager
        try:
            rapid_connect_file = case_insensitive_file_search(
                rapid_vpu_input_dir, r"rapid_connect\.csv"
            )
            riv_bas_id_file = case_insensitive_file_search(
                rapid_vpu_input_dir, r"riv_bas_id.*?\.csv"
            )
            comid_lat_lon_z_file = case_insensitive_file_search(
                rapid_vpu_input_dir, r"comid_lat_lon_z.*?\.csv"
            )
            weight_table = case_insensitive_file_search(
                rapid_vpu_input_dir, r"weight_ifs_48r1.*?\.csv"
            )
            k_file = case_insensitive_file_search(rapid_vpu_input_dir, r"k\.csv")
            x_file = case_insensitive_file_search(rapid_vpu_input_dir, r"x\.csv")
        except Exception as e:
            rapid_logger.critical(f"input file not found: {e}")
            raise

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
            Qfor_file = case_insensitive_file_search(rapid_vpu_input_dir, r"qfor\.csv")
            for_tot_id_file = case_insensitive_file_search(
                rapid_vpu_input_dir, r"for_tot_id\.csv"
            )
            for_use_id_file = case_insensitive_file_search(
                rapid_vpu_input_dir, r"for_use_id\.csv"
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

        # Get qinit file
        qinit_file = ""
        BS_opt_Qinit = False
        if initialize_flows:
            # Look for qinit files for the past 3 days;
            # Try seasonal average file if not
            for day in [24, 48, 72]:
                past_date = (
                    datetime.datetime.strptime(date, "%Y%m%d%H")
                    - datetime.timedelta(hours=day)
                ).strftime("%Y%m%d%H")
                qinit_file = os.path.join(rapid_vpu_input_dir, f"Qinit_{past_date}.nc")
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
                        os.path.join(rapid_vpu_input_dir, "seasonal_qinit*.nc")
                    )[0]
                    BS_opt_Qinit = qinit_file and os.path.exists(qinit_file)
                except Exception:
                    print("Failed to initialize from Seasonal Averages.")
                    print(f"WARNING: {qinit_file} not found. " "Not initializing ...")
                    qinit_file = ""

        # Create inflow directory
        inflow_dir = os.path.join(workspace, "inflows")
        if not os.path.exists(inflow_dir):
            os.mkdir(inflow_dir)

        # Create inflow
        create_inflow_file(
            lsm_data=runoff,
            input_dir=rapid_vpu_input_dir,
            inflow_dir=inflow_dir,
            weight_table=weight_table,
            comid_lat_lon_z=comid_lat_lon_z_file,
            cumulative=True,
            file_label=ens_number,
        )

        # Get forecast chronometry
        interval = 3 if ens_number < 52 else 1
        duration = 360 if ens_number < 52 else 240

        # Get inflow file path
        inflow_file_path = case_insensitive_file_search(
            inflow_dir, rf"m3_{vpucode}.*_{ens_number}\.nc"
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


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "workspace",
        nargs=1,
        help="Path to suite home directory",
    )
    argparser.add_argument(
        "job_id",
        nargs=1,
        help="Job ID",
    )
    argparser.add_argument(
        "rapid_executable_location",
        nargs=1,
        help="Path to RAPID executable",
    )

    args = argparser.parse_args()
    workspace = args.workspace[0]
    job_id = args.job_id[0]
    rapid_executable_location = args.rapid_executable_location[0]
    mp_execute_directory = os.path.join(workspace, "execute")
    subprocess_forecast_log_dir = os.path.join(workspace, "subprocess")

    rapid_forecast_exec(
        workspace,
        job_id,
        rapid_executable_location,
        mp_execute_directory,
        subprocess_forecast_log_dir,
    )
