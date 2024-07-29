# Header file for parallel tasks.
# For now only defines 'par_ncpus' variable.
#
# Define 'par_ncpus' variable, which is the number
# of CPU cores available for the task. This variable
# may be used inside the task if something in the
# task needs to know this number.
#
# Where does this number come from?:
#
# If 'sjob' is used as a job submitter, this number is defined
# in the ~/.comfies/sjob/<destination> by adding 'export SJOB_NCPUS=...'
# to the 'directives' list.
#
# If 'trimurti' is used, this number is defined as
# jobs.destinations.<destname>.ncpus parameter in the deployment
# config file. When the suite is deployed, 'sdeploy' will add
# %%NCPUS%% ecFlow variable in the suite definition.
# Then, in qsub.h, there is "export TRIMURTI_NCPUS=%%NCPUS:1%%"
#
# Tasks should refer to "par_ncpus" variable rather than $NCPUS
# or %%NCPUS%% variables.
%includeonce <gnuparallel.h>

par_ncpus=${SJOB_NCPUS:-${TRIMURTI_NCPUS:-1}}
