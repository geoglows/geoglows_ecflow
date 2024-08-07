%includeonce <suite.h>
%includeonce <python3.h>
%includeonce <conda.h>

set +u
export PYTHONPATH=$suite_libdir/virtualenvs/rapid/lib/python3.10/site-packages/:$suite_dir/lib/python:$PYTHONPATH
export LD_LIBRARY_PATH=$suite_libdir/virtualenvs/rapid/lib
set -u
