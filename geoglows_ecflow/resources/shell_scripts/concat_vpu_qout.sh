#!/bin/bash

# accept the rapid output directory path as the argument passed with

cd rapid_output_directory || exit

# Initialize an array to store VPU numbers
declare -a vpu_numbers=()
# Read the output of the command line by line and store VPU numbers in the array
while IFS= read -r vpu_number; do
    vpu_numbers+=("$vpu_number")
done < <(ls -1 Qout_*.nc | awk -F_ '{print $2}' | sort -u)

for vpu_number in "${vpu_numbers[@]}"; do
    echo "Processing VPU number: $vpu_number"
    # concatenate along a new dimension which is called record by default
    ncecat $(ls -1 Qout_${vpu_number}_*.nc | grep -v Qout_${vpu_number}_52.nc | sort -V) -O Qout_${vpu_number}.nc
    # rename record to ens inplace
    ncrename -d record,ensemble Qout_${vpu_number}.nc
    # remove the files Qout_${vpu_number}_*.nc
    rm Qout_${vpu_number}_*.nc
done
