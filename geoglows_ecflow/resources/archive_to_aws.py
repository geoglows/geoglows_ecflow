import os
import argparse
import json
import yaml
import boto3


def upload_to_s3(ecf_files: str, aws_config_file: str, vpu: str):
    """Uploads GEOGloWS forecast output to AWS.

    Args:
        ecf_files (str): Path to suite home directory.
        aws_config_file (str): Path to AWS config file.
        vpu (str): VPU code.
    """
    with open(aws_config_file, "r") as f:
        config = yaml.safe_load(f)
        ACCESS_KEY_ID = config["aws_access_key_id"]
        SECRET_ACCESS_KEY = config["aws_secret_access_key"]

        # Create an S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )

    with open(os.path.join(ecf_files, "rapid_run.json"), "r") as f:
        # Get data
        data = json.load(f)
        date = data["date"]
        rapid_output_path = data["output_dir"]

        # Upload multiple files to S3
        bucket_name = config["bucket_name"]
        base_name = f"Qout_{vpu}_{date}.zarr"
        zarr_file = os.path.join(rapid_output_path, base_name)

        for root, _, files in os.walk(zarr_file):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                object_key = f"{base_name}{file_path.split(base_name)[1]}"
                s3.upload_file(file_path, bucket_name, object_key)


if __name__ == "__main__":
    # Get ecflow home directory
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "ecf_files",
        nargs=1,
        help="Path to suite home directory",
    )
    argparser.add_argument(
        "aws_config_file",
        nargs=1,
        help="Path to AWS config file",
    )
    argparser.add_argument(
        "vpu",
        nargs=1,
        help="VPU code",
    )

    args = argparser.parse_args()
    ecf_files = args.ecf_files[0]
    aws_config_file = args.aws_config_file[0]
    vpu = args.vpu[0]

    upload_to_s3(ecf_files, aws_config_file, vpu)
