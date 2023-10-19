import os
import sys
import json
import yaml
import boto3


def upload_to_s3(ecflow_home: str, config_file: str, vpu: str):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
        ACCESS_KEY_ID = config["aws_access_key_id"]
        SECRET_ACCESS_KEY = config["aws_secret_access_key"]

        # Create an S3 client
        s3 = boto3.client(
            "s3",
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )

    with open(os.path.join(ecflow_home, "rapid_run.json"), "r") as f:
        data = json.load(f)
        # Get rapid output path
        rapid_output_path = data["output_dir"]

        # Loop through each date
        for date in data["dates"].keys():
            # Upload multiple files to S3
            bucket_name = config["bucket_name"]
            base_name = f"Qout_{vpu}_{date}.zarr"
            zarr_file = os.path.join(rapid_output_path, vpu, date, base_name)

            for root, _, files in os.walk(zarr_file):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    object_key = f"{base_name}{file_path.split(base_name)[1]}"
                    s3.upload_file(file_path, bucket_name, object_key)


if __name__ == "__main__":
    # Get ecflow home directory
    ecflow_home = sys.argv[1]
    config_file = sys.argv[2]
    vpu = sys.argv[3]
    upload_to_s3(ecflow_home, config_file, vpu)
