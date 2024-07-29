%includeonce <suite.h>

mkdir -p $suite_libdir/GDAL_DATA

module unload gdal || :
module load gdal/3.0.4
