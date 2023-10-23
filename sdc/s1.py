from pystac import Catalog
import numpy as np

from typing import Optional, Tuple, Any
from xarray import Dataset, DataArray
from pystac import Item

import sdc.utils as utils
import sdc.query as query


def load_s1_rtc(bounds: Tuple[float, float, float, float],
                time_range: Optional[Tuple[str, str]] = None,
                time_pattern: Optional[str] = '%Y-%m-%d',
                apply_mask: bool = True) -> Dataset:
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
    The Sentinel-2 L2A data is sourced from the Digital Earth Africa STAC Catalog.
    For more product details, see:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-1_specs.html
    """
    product = 's1_rtc'
    measurements = ('vv', 'vh', 'area', 'angle')
    dtype = np.dtype("float32")
    params = utils.common_params()
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(utils.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    # https://github.com/gjoseph92/stackstac/issues/20
    items = utils.convert_asset_hrefs(list_stac_obj=items, href_type='absolute')
    
    # Turn into dask-based xarray.Dataset
    stackstac_params = {'items': items, 'assets': list(measurements), 'bounds': bounds,
                        'dtype': dtype, 'fill_value': np.nan}
    stackstac_params.update(params)
    da = utils.stackstac_wrapper(params=stackstac_params)
    ds = utils.dataarray_to_dataset(da=da)
    
    if apply_mask:
        params['bounds'] = bounds
        valid = _mask(items=items, params=params)
        ds = ds.where(valid, drop=True)
    
    return ds


def _mask(items: list[Item],
          params: dict[str, Any]) -> DataArray:
    """
    Creates a valid-data mask from the `mask` band of Sentinel-1 RTC data.
    
    Parameters
    ----------
    items : list[Item]
        A list of STAC Items from Sentinel-1 RTC data.
    params : dict[str, Any]
        Parameters to pass to `stackstac.stack`
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the valid-data mask.
    
    Notes
    -----
    An overview table of the data mask classes can be found in Table 3:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-1_specs.html
    """
    stackstac_params = {'items': items, 'assets': ['mask'], 'dtype': np.dtype("uint8"),
                        'fill_value': 0}
    stackstac_params.update(params)
    da = utils.stackstac_wrapper(params=stackstac_params)
    
    mask = da.sel(band='mask')
    mask = (mask == 1)
    return mask.compute()


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
