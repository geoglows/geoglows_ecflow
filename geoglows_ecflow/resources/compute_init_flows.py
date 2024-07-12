import argparse
import os

import netCDF4 as nc
import xarray as xr


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "workspace",
        nargs=1,
        help="Path to rapid_run.json base directory.",
    )
    parser.add_argument(
        "vpu",
        nargs=1,
        help="vpu number to process.",
    )

    args = parser.parse_args()
    workspace = args.workspace[0]
    vpu = args.vpu[0]

    ymd = os.getenv('YMD', None)
    output_file = os.path.join(workspace, 'input', vpu, f'Qinit_{ymd}.nc')

    with xr.open_dataset(os.path.join(workspace, "output", f"nces_avg_{vpu}.nc")) as average_flows:
        qinit_values = average_flows.Qout[7, :]
        river_ids = average_flows.rivid[:]
        init_date = average_flows.time[7].strftime('%Y%M%d %X')

    with nc.Dataset(output_file, "w", format="NETCDF3_CLASSIC") as qinit_nc:
        # create dimensions
        qinit_nc.createDimension("time", 1)
        qinit_nc.createDimension("rivid", river_ids.shape[0])

        qout_var = qinit_nc.createVariable("Qout", "f8", ("time", "rivid"))
        qout_var[:] = qinit_values
        qout_var.long_name = "instantaneous river water discharge downstream of each river reach"
        qout_var.units = "m3 s-1"
        qout_var.coordinates = "lon lat"
        qout_var.grid_mapping = "crs"
        qout_var.cell_methods = "time: point"

        # rivid
        rivid_var = qinit_nc.createVariable("rivid", "i4", ("rivid",))
        rivid_var[:] = river_ids
        rivid_var.long_name = "unique identifier for each river reach"
        rivid_var.units = "1"
        rivid_var.cf_role = "timeseries_id"

        # time
        time_var = qinit_nc.createVariable("time", "i4", ("time",))
        time_var[:] = 0
        time_var.long_name = "time"
        time_var.standard_name = "time"
        time_var.units = f'seconds since {init_date}'  # Must be seconds
        time_var.axis = "T"
        time_var.calendar = "gregorian"

        # longitude
        lon_var = qinit_nc.createVariable("lon", "f8", ("rivid",))
        lon_var[:] = 0
        lon_var.long_name = "longitude of a point related to each river reach"
        lon_var.standard_name = "longitude"
        lon_var.units = "degrees_east"
        lon_var.axis = "X"

        # latitude
        lat_var = qinit_nc.createVariable("lat", "f8", ("rivid",))
        lat_var[:] = 0
        lat_var.long_name = "latitude of a point related to each river reach"
        lat_var.standard_name = "latitude"
        lat_var.units = "degrees_north"
        lat_var.axis = "Y"

        # crs
        crs_var = qinit_nc.createVariable("crs", "i4")
        crs_var.grid_mapping_name = "latitude_longitude"
        crs_var.epsg_code = "EPSG:4326"  # WGS 84
        crs_var.semi_major_axis = 6378137.0
        crs_var.inverse_flattening = 298.257223563

        # add global attributes
        qinit_nc.Conventions = "CF-1.6"
        qinit_nc.featureType = "timeSeries"
