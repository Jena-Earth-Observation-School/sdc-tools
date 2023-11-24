from pystac import Catalog
from odc.stac import load as odc_stac_load

from typing import Optional, Tuple
from xarray import Dataset

import sdc.utils as utils
from sdc.products import _query as query


def load_s1_rtc(bounds: Tuple[float, float, float, float],
                time_range: Optional[Tuple[str, str]] = None,
                time_pattern: Optional[str] = None
                ) -> Dataset:
    """
    Loads the Sentinel-1 RTC data product for an area of interest.
    
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
    Dataset
        An xarray Dataset containing the Sentinel-1 RTC data.
    
    Notes
    -----
    The Sentinel-1 RTC data is sourced from the Digital Earth Africa STAC Catalog.
    For more product details, see:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-1_specs.html
    """
    product = 's1_rtc'
    bands = ['vv', 'vh', 'area']
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(utils.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='float32',
                       **utils.common_params())
    
    return ds


def separate_asc_desc(ds: Dataset) -> Tuple[Dataset, Dataset]:
    """
    Separates a Dataset into ascending and descending orbits.
    
    Parameters
    ----------
    ds: Dataset
        An xarray Dataset containing data that can be separated into ascending and
        descending orbits. E.g. Sentinel-1 RTC.
    
    Returns
    -------
    tuple of Dataset
        Two xarray Datasets containing the ascending and descending orbit data,
        respectively.
    """
    ds_copy = ds.copy(deep=True)
    try:
        ds_copy_asc = ds_copy.where(ds_copy['sat:orbit_state'] == 'ascending',
                                    drop=True)
        ds_copy_desc = ds_copy.where(ds_copy['sat:orbit_state'] == 'descending',
                                     drop=True)
    except KeyError:
        _vars = list(ds_copy.data_vars)
        ds_copy_asc = ds_copy.copy(deep=True)
        ds_copy_asc = ds_copy_asc.drop_vars(_vars)
        ds_copy_desc = ds_copy_asc.copy(deep=True)
        for v in _vars:
            ds_copy_asc[v] = ds_copy[v].where(ds_copy[v].time.dt.hour > 12, drop=True)
            ds_copy_desc[v] = ds_copy[v].where(ds_copy[v].time.dt.hour < 12, drop=True)
    return ds_copy_asc, ds_copy_desc