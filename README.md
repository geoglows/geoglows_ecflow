# ECFLOW workflow for GEOGloWS streamflow forecasting

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

- ecflow>=5.11.3,<5.17
- nco>=5.1.8
- ksh>=2020.0.0

## geoglows_ecflow configuration file (config.yaml)

The deployment configuration is a plain YAML file. Copy
[`config.example.yaml`](config.example.yaml) to `config.yaml` and edit the
values for your environment (`config.yaml` is gitignored so secrets stay out
of version control). Values like `%SCHOST:ab%` are ecFlow variables passed
through verbatim, and `{includes}`/`{scripts}` are sdeploy search-path
placeholders.

```yaml
name: suite_name
mode: test                 # 'test' or 'prod'
first_date: "YYYYMMDD"
first_barrier: "YYYYMMDD"
vpu_list: []
ens_members: 51
mars_workers: 3
script_extension: ".ecf"

expver: geoglows
exparch: /path/to/archive
iniexparch: /path/to/init_archive
staticdata: /path/to/assets
workroot: /path/to/workroot

# suite's source code
source:
  root: /path/to/source
  builder: geoglows_ecflow.workflow.builders.builder
  includes: "scripts/troika:suites/scripts/tems:{includes}"
  scripts: "scripts/tems:{scripts}"

# deploy location
target:
  root: /path/to/deploy_location

# where to run computations
jobs:
  manager:
    name: troika
  root: /path/to/job_root
  limit: 26
  destinations:
    default:
      host: "%SCHOST:ab%"
      bkup_host: "%SCHOST_BKUP%"
      user: user_name
      queue: nf
      account: ECACCOUNT
      sthost: sthost
    parallel:
      host: "%SCHOST:ab%"
      bkup_host: "%SCHOST_BKUP%"
      user: user_name
      queue: nf
      ncpus: "12"
      mem: "1000"

# GEOGloWS software packages installed alongside the suite
packages:
  scripts:
    srcdir: /path/to/source/scripts
```

## Troika job submission

The suite submits jobs through [troika](https://github.com/ecmwf/troika), ECMWF's
job-submission tool. On Atos, troika and its site configuration are provided by the
system. To run **locally**, install the optional `troika` dependency and point the
suite at a small local troika config.

Install with the troika extra:

```bash
pip install .[troika]
```

Create a local troika config (copy [`troika.example.yml`](troika.example.yml) to
`troika.yml`) that runs jobs as plain local processes. The **site name must match the
`host`** used in the config's job destinations:

```yaml
sites:
  localhost:
    type: direct        # run the job directly (no SLURM/PBS)
    connection: local   # on this machine (no ssh)
```

Then add `executable` and `config` to the `jobs.manager` block of your `config.yaml`:

```yaml
jobs:
  manager:
    name: troika
    executable: /path/to/troika    # output of `which troika`
    config: /path/to/troika.yml
  # ...
  destinations:
    default:
      host: localhost              # must match the site name in troika.yml
      user: your_user
```

With that, deploying and running the suite (see *Local run example*) submits every task
through troika as a local process.

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

Generate the suite definition (via CLI or Python):

```bash
gdeploy --config /path/to/config.yaml
```

```python
from geoglows_ecflow.workflow.create import main
main("/path/to/config.yaml")
```

Start a local ecflow server, then load and begin the suite:

```bash
bash /path/to/local_ecflow_start.sh
```

```python
from geoglows_ecflow import client

client.add_definition("/path/to/deploy_dir/suite.def", "localhost:2500")
client.begin("suite_name", "localhost:2500")
```
