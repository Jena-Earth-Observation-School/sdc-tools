import xarray as xr
from odc.geo.xr import assign_crs

from typing import Optional, Tuple
from xarray import DataArray

import sdc.utils as utils
from sdc.products import _query as query


def load_mswep(bounds: Tuple[float, float, float, float],
               time_range: Optional[Tuple[str, str]] = None,
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
    nc_files = query.filter_mswep_nc(directory=utils.get_catalog_path(product='mswep'),
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
