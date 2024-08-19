# ECFLOW RAPID workflow for GEOGloWS

![GEOGloWS VPUCode Coverage](images/geoglows_vpucode_coverage.png)
*Coverage of GEOGloWS VPUCode basins. Source: [Riley Hales](mailto:rchales@byu.edu).*

## Installation

```bash
cd geoglows_ecflow/
pip install .
```

```bash
# development installation
cd geoglows_ecflow/
pip install -e .
```

## Non-Python Dependencies

- rapid>=20210423
- ecflow>=5.11.3
- nco>=5.1.8
- ksh>=2020.0.0

## geoglows_ecflow configuration file (config.cfg)

```python
    name = 'suite_name'
    srcroot = "/path/to/source"
    first_date = first_barrier = 'YYYYMMDD'
    vpu_list = []
    mars_bond_id='251'
    staticdata = '/path/to/assets'
    workroot = f'/path/to/workroot'
    mode = 'test'  # suite mode ('rd':research, 'test':test, 'prod':production)
    expver = 'geoglows'
    exparch = '/path/to/archive'
    iniexparch = '/path/to/init_archive'
    mars_workers = '3'
    script_extension = '.ecf'

    # suite's source code
    source = dict(
    root = srcroot,
    builder = 'geoglows_ecflow.workflow.builders.builder',
    includes = 'scripts/troika:suites/scripts/tems:{includes}',
    scripts = 'scripts/tems:{scripts}'
    )

    # deploy location
    target = dict(
        root = "/path/to/deploy_location",
    )

    # where to run computations
    jobs = dict(
        manager = dict(
            name='troika',
        ),
        root = '/path/to/job_root',
        limit = 26,
        destinations = dict(
            default = dict(
                host = '%SCHOST:ab%',
                bkup_host = '%SCHOST_BKUP%',
                user = 'user_name',
                queue = 'nf',
                account = 'ECACCOUNT',
                sthost = 'sthost',
            ),
            parallel = dict(
                host = '%SCHOST:ab%',
                bkup_host = '%SCHOST_BKUP%',
                user = user,
                queue = 'nf',
                ncpus = '12',
                mem = '1000',
            )
        )
    )

    # --------------------------------------------
    # Configuration of EFAS software packages
    # which are installed together with the suite.
    # --------------------------------------------
    packages = dict(
        model = dict(
            srcdir = 'git+https://github.com/c-h-david/rapid.git@20210423',
        ),

        petsc = dict(
            srcdir = srcroot + 'petsc_reqs',
        ),

        scripts = dict(
            srcdir = srcroot + 'scripts',
        ),
    )
```

## AWS configuration file (aws_config.yml)

```yaml
# aws credentials
aws_access_key_id: AWS_ACCESS_KEY_ID
aws_secret_access_key: AWS_SECRET_ACCESS_KEY

# aws s3 bucket
bucket_forecast_archive: S3_BUCKET_NAME
bucket_maptable_archive: S3_BUCKET_NAME
```

## Custom ecflow server start (local_ecflow_start.sh)

```bash
#!/bin/bash
export ECF_PORT=2500
export ECF_HOST=localhost

ecflow_start.sh -d /path/to/ecflow_home
```

## Local run example

```Python
import subprocess
from geoglows_ecflow import geoglows_forecast_job, client

# Start server
subprocess.run(['bash', '/path/to/local_server_start.sh'])

# Create definition
geoglows_forecast_job.create("/path/to/config.cfg")

# Add definition to server
client.add_definition("/path/to/definition.def", "<HOST>:<PORT>")

# Begin definition
client.begin("definition_name")
```
