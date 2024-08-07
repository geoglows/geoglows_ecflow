#SBATCH --output=%ECF_JOBOUT%
#SBATCH --error=%ECF_JOBOUT%
#SBATCH --job-name=%FAMILY1:NOT_DEF%_%TASK%
#SBATCH --qos=%QUEUE%
#SBATCH --account=%ACCOUNT%
#SBATCH --mem-per-cpu=%MEM:12800%M
#SBATCH --cpus-per-task=%NCPUS:1%
#SBATCH --export=STHOST=%STHOST%
#SBATCH --gres=ssdtmp:5G
export TROIKA_CPUS=%NCPUS:1%
echo "Using SCRATCHDIR as TMPDIR"
export TMPDIR=$SCRATCHDIR

cd $TMPDIR
