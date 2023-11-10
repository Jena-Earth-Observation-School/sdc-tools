import numpy as np
import xarray as xr
from pystac import Catalog
from odc.stac import load as odc_stac_load

from typing import Optional, Tuple, List
from xarray import Dataset, DataArray
from pystac import Item

import sdc.utils as utils
import sdc.query as query


def load_s1_rtc(bounds: Tuple[float, float, float, float],
                time_range: Optional[Tuple[str, str]] = None,
                time_pattern: str = '%Y-%m-%d',
                apply_mask: bool = True
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
        The pattern used to parse the time strings of `time_range`. Defaults to
        '%Y-%m-%d'.
    apply_mask : bool, optional
        Whether to apply a valid-data mask to the data. Defaults to True.
        The mask is created from the `mask` band of the product.
    
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
    
    common_params = utils.common_params()
    if not apply_mask:
        common_params['chunks']['time'] = -1
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='float32',
                       **common_params)
    
    if apply_mask:
        valid = _mask(items=items, bounds=bounds, common_params=common_params)
        ds = xr.where(valid, ds, np.nan)
        ds = ds.chunk({'time': -1,
                       'latitude': common_params['chunks']['latitude'],
                       'longitude': common_params['chunks']['longitude']})
    
    return ds


def _mask(items: List[Item],
          bounds: Tuple[float, float, float, float],
          common_params: dict
          ) -> DataArray:
    """
    Creates a valid-data mask from the `mask` band of Sentinel-1 RTC data.
    
    An overview table of the data mask classes can be found in Table 3:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-1_specs.html
    """
    ds = odc_stac_load(items=items, bands='mask', bbox=bounds, dtype='uint8',
                       **common_params)
    mask = (ds.mask == 1)
    return mask


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
