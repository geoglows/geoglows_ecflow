import argparse
import os
import subprocess

if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "workspace",
        nargs=1,
        help="Path to suite home directory",
    )
    workspace = argparser.parse_args().workspace[0]
    # get the path to the bash script
    bash_script_path = os.path.join(os.path.dirname(__file__), 'shell_scripts', 'concat_vpu_ensembles.sh')
    # execute the script
    subprocess.call(f'bash {bash_script_path} -d {workspace}')
