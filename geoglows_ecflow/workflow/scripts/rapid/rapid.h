# Rapid.h

%includeonce <netcdf.h>

export TACC_NETCDF_LIB=$NETCDF4_DIR/lib
export TACC_NETCDF_INC=$NETCDF4_DIR/include
export PETSC_DIR=$suite_libdir/petsc/petsc-3.13.0
export PETSC_ARCH='linux-gcc-c'
export LD_LIBRARY_PATH=$TACC_NETCDF_LIB
export PATH=$PATH:/$PETSC_DIR/$PETSC_ARCH/bin
export PATH=$suite_dir/bin:$PATH
