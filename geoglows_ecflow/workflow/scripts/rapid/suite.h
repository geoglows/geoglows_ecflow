# suite.h -- defines suite-level variables, parameters

# workspace directory on GPFS
suite_dir=<?config.get('workroot')?>

# where are suite libraries, scripts installed
suite_libdir=$suite_dir/lib

# where are suite libraries, scripts installed
suite_workdir=$suite_dir/workdir

# suite staticdata
suite_staticdata=<?config.get('staticdata')?>

# suite experiment version
suite_expver=<?config.get('expver', default='0001')?>

# suite archiving directory
suite_archdir=<?config.get('exparch')?>

# suite archiving directory
suite_iniarchdir=<?config.get('iniexparch')?>

# suite era5 expver
suite_era5_expver=<?config.get('era5_expver',default='0001')?>

# are we running test? prod?
suite_mode=<?config.get('mode',default='TEST')?>

%includeonce <mars.h>
