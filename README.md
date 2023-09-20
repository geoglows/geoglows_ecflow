# ECFLOW RAPID workflow for GEOGloWS

![GEOGloWS VPUCode Coverage](images/geoglows_vpucode_coverage.png)
*Coverage of GEOGloWS VPUCode basins. Source: [Riley Hales](mailto:rchales@byu.edu).*

## Installation

```bash
cd geoglows_ecflow/
pip install .
```

## Non-Python Dependencies

- ecflow>=5.11.3
- nco>=5.1.8
- ksh>=2020.0.0

## geoglows_ecflow configuration file (config.yml)

```yaml
# ecflow variables
python_exec: /path/to/python
ecflow_home: /path/to/ecflow
ecflow_bin: /path/to/ecflow_client  # Required for local run
local_run: false
ecflow_entities:
  suite:
    name: geoglows_forecast
    logs: /path/to/ecf_out
  family:
    name: ensemble_family
    pyscript: run_ecflow.py
    suite: geoglows_forecast
  task:
    - name: prep_task
      pyscript: iprep_ecf.py
      variables:
        - PYSCRIPT
        - IO_LOCATION
        - RUNOFF_LOCATION
        - ECF_FILES
      suite: geoglows_forecast
    - name: plain_table_task
      pyscript: spt_extract_plain_table.py
      variables:
        - PYSCRIPT
        - OUT_LOCATION
        - LOG_FILE
        - NCES_EXEC
        - ERA_TYPE
      suite: geoglows_forecast
    - name: day_one_forecast_task
      pyscript: day_one_forecast.py
      variables:
        - PYSCRIPT
        - IO_LOCATION
        - ERA_LOCATION
        - FORECAST_RECORDS_DIR
        - LOG_DIR
      suite: geoglows_forecast
    - name: ens_member
      variables:
        - PYSCRIPT
        - ECF_FILES
        - JOB_INDEX
        - RAPID_EXEC
        - EXEC_DIR
        - SUBPROCESS_DIR
      suite: geoglows_forecast

# rapid variables
rapid_exec: /path/to/rapid_exec
rapid_exec_dir: /path/to/rapid_exec_dir
rapid_subprocess_dir: /path/to/rapid_subprocess_dir
rapid_io: /path/to/rapid_io
runoff_dir: /path/to/runoff_dir
era_type: era5
era_dir: /path/to/era5_dir

# nco variables
nces_exec: /path/to/nces_exec
```

## Custom ecflow server start (local_ecflow_start.sh)

```bash
#!/bin/bash
export ECF_HOME=/path/to/ecflow/home
export ECF_BIN=/path/to/ecflow_client
export ECF_PORT=2500
export ECF_HOST=localhost

ecflow_start.sh -d $ECF_HOME
```
