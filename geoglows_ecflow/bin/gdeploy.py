#!/usr/bin/env python3

import sys
import argparse
import json
from geoglows_ecflow.workflow.create import main

# Create Argument Parser
parser = argparse.ArgumentParser(description="Create the workflow definition.", prog="gdeploy")

parser.add_argument(
    "--config", "-c", required=True, help="Path to the configuration file."
)

parser.add_argument(
    "--params",
    "-p",
    required=False,
    help="Param/value dict; these override params from config file.",
)

parser.add_argument(
    "--dry",
    "-d",
    required=False,
    help="If True, no actual files will be created in target dir.",
)

# Parse Arguments
args = parser.parse_args()
config = args.config
params = json.loads(args.params) if args.params else {}
dry = args.dry if args.dry else False

# Run geoglows_ecflow.workflow.create.main
sys.exit(main(config, params, dry))
