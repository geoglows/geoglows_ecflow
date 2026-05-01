import argparse
import datetime
import json
import os
import sys
from glob import glob

import river_route as rr

from geoglows_ecflow.resources.helper_functions import (
    create_logger,
    get_ensemble_number_from_forecast,
)


# Member 52 is the deterministic high-resolution (HRES) forecast, smuggled
# into the same per-(vpu, member) parallelization as ENS members 1-51. HRES
# is delivered at 1h resolution over 10 days; ENS at 3h over 15 days.
HRES_MEMBER = 52
ENS_INTERVAL_S = 3 * 3600
HRES_INTERVAL_S = 1 * 3600
ENS_DURATION_S = 360 * 3600
HRES_DURATION_S = 240 * 3600
DT_ROUTING_S = 15 * 60


def _find_state_init(vpu_input_dir: str, date: str) -> str | None:
    """Find prior-cycle Qinit at 24/48/72h lookback, then seasonal fallback."""
    # Forecasts run at 00 and 12 UTC; the 24/48/72h lookback tolerates one or
    # two missed cycles before falling through to a seasonal climatology.
    # Qinit_<past>.parquet is written by the previous cycle's
    # compute_init_flows step (ensemble mean). The per-member Qfinal files
    # written by this script are diagnostic artifacts and are not read here.
    base = datetime.datetime.strptime(date, "%Y%m%d%H")
    for hrs in (24, 48, 72):
        past = (base - datetime.timedelta(hours=hrs)).strftime("%Y%m%d%H")
        cand = os.path.join(vpu_input_dir, f"Qinit_{past}.parquet")
        if os.path.exists(cand):
            return cand
    seasonal = sorted(glob(os.path.join(vpu_input_dir, "seasonal_qinit*.parquet")))
    return seasonal[0] if seasonal else None


def river_route_forecast_exec(workspace: str, job_id: str, log_dir: str) -> None:
    with open(os.path.join(workspace, "rapid_run.json"), "r") as f:
        data = json.load(f)

    job = data.get(job_id)
    if not job:
        raise ValueError(f"Job '{job_id}' not found in rapid_run.json")

    date = data["date"]
    runoff = job["runoff"]
    vpu = job["vpu"]
    vpu_input_dir = job["input_dir"]
    discharge_file = job["output_file"]
    initialize_flows = job["init_flows"]

    logger = create_logger(
        "river_route_logger", "INFO", os.path.join(log_dir, f"{job_id}.log")
    )

    ens = get_ensemble_number_from_forecast(runoff)
    is_hres = ens == HRES_MEMBER
    interval_s = HRES_INTERVAL_S if is_hres else ENS_INTERVAL_S
    duration_s = HRES_DURATION_S if is_hres else ENS_DURATION_S

    state_init = _find_state_init(vpu_input_dir, date) if initialize_flows else None
    state_final = os.path.join(vpu_input_dir, f"Qfinal_{date}_{ens}.parquet")

    logger.info(f"Routing vpu={vpu} ens={ens} runoff={runoff}")
    logger.info(f"  state_init={state_init or '(none)'}")
    logger.info(f"  state_final={state_final}")
    logger.info(f"  discharge={discharge_file}")

    rr.RapidMuskingum(
        params_file=os.path.join(vpu_input_dir, "params.parquet"),
        grid_runoff_files=[runoff],
        grid_weights_file=os.path.join(vpu_input_dir, "weights.nc"),
        discharge_files=[discharge_file],
        channel_state_init_file=state_init,
        channel_state_final_file=state_final,
        dt_routing=DT_ROUTING_S,
        dt_total=duration_s,
        dt_discharge=interval_s,
        start_datetime=(
            f"{date[:4]}-{date[4:6]}-{date[6:8]} {date[8:10]}:00:00"
        ),
        # ECMWF runoff is accumulated since forecast start; the default
        # ("incremental") would silently produce wrong day-1 values.
        grid_accumulation_type="cumulative",
        # Pin the netCDF schema to RAPID's names ("Qout" / "rivid") so
        # downstream NCO/zarr/day_one consumers stay unaffected by the
        # router swap. river-route's own defaults are "Q" / "river_id".
        var_discharge="Qout",
        var_river_id="rivid",
        progress_bar=False,
        log_level="INFO",
    ).route()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run river-route forecast for one (vpu, ens) job.")
    parser.add_argument("workspace", help="Path to directory containing rapid_run.json")
    parser.add_argument("job_id", help="Job key inside rapid_run.json (e.g. job_<vpu>_<mem>)")
    args = parser.parse_args(argv)

    log_dir = os.path.join(args.workspace, "subprocess")
    os.makedirs(log_dir, exist_ok=True)
    river_route_forecast_exec(args.workspace, args.job_id, log_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
