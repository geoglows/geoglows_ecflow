module load conda/22.11.1-2
set +eu

#export PATH=$suite_libdir/conda:$suite_libdir/conda/bin:$PATH
#source $suite_libdir/conda/etc/profile.d/conda.sh
#conda config --append envs_dirs $suite_libdir/virtualenv

_CONDA_SET_GEOTIFF_CSV=""
GDAL_DATA=$suite_libdir/virtualenvs/rapid/share/gdal
GDAL_DRIVER_PATH=$suite_libdir/virtualenvs/rapid/lib/gdalplugins
GEOTIFF_CSV=''
PROJ_LIB=$suite_libdir/virtualenvs/rapid/share/proj
conda config --get
conda activate $suite_libdir/virtualenvs/rapid
set -eu