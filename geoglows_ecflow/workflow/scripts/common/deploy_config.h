# Fetch the deploy config file from the deployment host.
#
# TODO: sdeploy should re-generate 'deploy_config.h'
# each time it is executed. This way 'deploy_config.h'
# is always up-to-date, if e.g. one is constantly switching
# between different config files when deploying the (same) suite.

<!
import os
import platform
deploy_config_host = platform.node()
deploy_config_path = os.path.abspath(config.origin)
!>

deploy_config_host=<?deploy_config_host?>
deploy_config_path=<?deploy_config_path?>

if [[ -f $deploy_config_path ]]; then
    # deployment config is accessible - it means we
    # either deploy on the same host where the sources
    # are or the filesystem is mounted on both source
    # and target host.
    :
else
    # config is not accessible on the target machine;
    # we need to copy it from the source host to the
    # temp dir on the target machine
    deploy_config_tmppath=$PWD/${deploy_config_path##*/}
    scp -o StrictHostKeyChecking=no -o BatchMode=yes \
           $deploy_config_host:$deploy_config_path $deploy_config_tmppath
    deploy_config_path=$deploy_config_tmppath
fi

cat $deploy_config_path

# tell 'coget' and 'copp' utilities where
# to find the config file.
export CO_CONFIG_PATH=$deploy_config_path
