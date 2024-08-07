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
    import os
    from datetime import datetime, timedelta
    from datetime import datetime, timedelta
    first_date=(datetime.now() - timedelta(1)).strftime('%Y%m%d')
    vpu_list = ["125", "718"]

    user = os.environ['USER']
    home = os.environ['HOME']
    account = os.environ['ECACCOUNT']
    sthost='%STHOST:/ec/ws2'

    # Config file for production version of GEOGLOWS suite

    # ***root - not really configuration params but
    # defined for convenience to avoid repetition.
    # common root directory with package and suite sources
    srcroot  = f"/home/{user}/path/to/workflow"
    # root directory with datasets

    mars_bond_id='251'

    # where is the suite's source code
    source = dict(
    root = srcroot,
    builder = 'builders.builder',
    includes = 'scripts/troika:suites/scripts/tems:{includes}',
    scripts = 'scripts/tems:{scripts}'
    )

    # suite name
    name = f'egeoglows_{user}'

    # where to deploy the suite
    target = dict(
    root = f'/home/{user}/path/to/target/' + name,
    )

    # where to run computations
    jobs = dict(
    manager = dict(
        name='troika',
    ),
    root = f'{home}/ecflow_server/ecf_home',
    limit = 26,
    destinations = dict(
        default = dict(
        host = '%SCHOST:ab%',
        bkup_host = '%SCHOST_BKUP%',
        user = user,
        queue = 'nf',
        account = account,
        sthost = sthost,
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

    # Static Data
    staticdata = f'/hpcperm/{user}/geoglows/{name}/assets'
    # workroot directory of Geoglows project on the cluster
    workroot = f'/hpcperm/{user}/geoglows/{name}'

    # suite mode ('rd':research, 'test':test, 'prod':production)
    mode          = 'test'

    # ID of this experment
    expver         = 'geoglows'
    exparch        = f'ec:/{user}/geoglows/{name}' # Path on ECFS for Archiving
    iniexparch     = f'ec:/emos/geoglows/geoglows' # Path on ECFS for init suite Archiving


    mars_workers = '3'
    # initial dates
    first_date    = first_date
    first_barrier = first_date

    script_extension='.ecf'

    # --------------------------------------------
    # Configuration of EFAS software packages
    # which are installed together with the suite.
    # --------------------------------------------

    # configurations of various packages
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

    rapidpy = dict(
        srcdir = 'git+https://github.com/geoglows/RAPIDpy@v2.7.0'
    ),

    geoglows_ecflow = dict(
        srcdir = 'git+https://github.com/geoglows/geoglows_ecflow@v2.2.1'
    ),

    rtree = dict(
        srcdir = 'git+https://github.com/Toblerity/rtree@0.9.4'
    ),

    basininflow = dict(
        srcdir = 'git+https://github.com/geoglows/basininflow@v0.14.0'
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

## Run tests

```bash
cd geoglows_ecflow/
pip install -e .[test]

# run tests
pytests tests/

# run tests with coverage
pytests --cov geoglows_ecflow tests/
```
