from pathlib import Path
import xarray as xr
import pandas as pd
from odc.geo.xr import assign_crs
from rioxarray import open_rasterio

from typing import Optional
from xarray import DataArray

from sdc.products import _ancillary as anc
from sdc.products import _query as query


def load_mswep(bounds: tuple[float, float, float, float],
               time_range: Optional[tuple[str, str]] = None,
               time_pattern: Optional[str] = None
               ) -> DataArray:
    """
    Loads the MSWEP (Multi-Source Weighted-Ensemble Precipitation) data product for an 
    area of interest.
    
    Parameters
    ----------
    bounds: tuple of float
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
        Will be used to filter the STAC Catalog for intersecting STAC Collections.
    time_range : tuple of str, optional
        The time range in the format (start_time, end_time) to filter STAC Items by.
        Defaults to None, which will load all STAC Items in the filtered STAC
        Collections.
    time_pattern : str, optional
        Time pattern to parse the time range. Only needed if it deviates from the
        default: '%Y-%m-%d'.
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the MSWEP data.
    
    Notes
    -----
    The MSWEP data is available as daily precipitation estimates at 0.1° resolution.
    For more product details, see: https://www.gloh2o.org/mswep
    """
    nc_files = query.filter_mswep_nc(directory=anc.get_catalog_path(product='mswep'),
                                     time_range=time_range,
                                     time_pattern=time_pattern)
    ds_list = []
    for nc in nc_files:
        _ds = xr.open_dataset(nc)
        ds_list.append(_ds)
    
    ds = xr.concat(ds_list, dim="time")
    ds = ds.rename({'lon': 'longitude', 'lat': 'latitude'})
    ds = assign_crs(ds, crs=4326)
    del ds.attrs['history']
    
    ds = ds.sel(longitude=slice(bounds[0], bounds[2]),
                latitude=slice(bounds[3], bounds[1]))
    if time_range is not None:
        ds = ds.sel(time=slice(time_range[0], time_range[1]))
    
    return ds.precipitation


def load_chirps(bounds: tuple[float, float, float, float],
                time_range: Optional[tuple[str, str]] = None,
                time_pattern: Optional[str] = None
                ) -> DataArray:
    files = query.filter_chirps(directory=anc.get_catalog_path(product='chirps'),
                                time_range=time_range,
                                time_pattern=time_pattern)
    files = [Path(f) for f in files]
    
    da_list = []
    for file_path in files:
        da = open_rasterio(file_path)
        year, month = file_path.stem.split("chirps-v3.0.")[1].split(".")
        date_str = f"{year}-{month}-01"
        da = da.assign_coords(time=pd.to_datetime(date_str))
        da_list.append(da)

    da_list = sorted(da_list, key=lambda x: x.time.values)
    da = xr.concat(da_list, dim='time').squeeze()
    da = da.where(da !=-9999.)
    da = da.rename({'x': 'longitude', 'y': 'latitude'})
    da = assign_crs(da, crs=4326)
    da = da.sel(longitude=slice(bounds[0], bounds[2]),
                latitude=slice(bounds[3], bounds[1]))
    return da
