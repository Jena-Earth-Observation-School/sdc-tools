import numpy as np

from typing import Optional
from xarray import DataArray, Dataset
import xarray as xr
from numpy import ndarray

from sdc.load import load_product


def groupby_acq_slices(ds: Dataset, use_flox=True) -> Dataset:
    """
    Groups acquisition slices of all data variables in a Dataset by calculating the mean
    for each rounded 1-hour time interval.
    
    Parameters
    ----------
    ds : Dataset
        The Dataset to be grouped.
    use_flox : bool, optional
        Whether to use the `flox` backend for the operation. Defaults to True.
    
    Returns
    -------
    ds_copy : Dataset
        The grouped Dataset.
    """
    ds_copy = ds.copy(deep=True)
    ds_copy.coords['time'] = ds_copy.time.dt.round('1h')
    with xr.set_options(use_flox=use_flox):
        ds_copy = ds_copy.groupby('time').mean(skipna=True)
    if len(np.unique(ds.time.dt.date)) < len(ds_copy.time):
        print("Warning: Might have missed to group some acquisition slices!")
    return ds_copy


def mask_from_vec(vec: str,
                  da: Optional[DataArray] = None
                  ) -> ndarray:
    """
    Create a boolean mask from a vector file. The mask will have the same shape and
    transform as the provided DataArray. If no DataArray is given, the `sanlc` product 
    will be loaded with the bounding box of the vector file and used as the template.
    
    Parameters
    ----------
    vec : str
        Path to a vector file readable by geopandas (e.g. shapefile, GeoJSON, etc.).
    da : DataArray, optional
        DataArray to use as a template for the mask, which will be created with the same
        shape and transform as the DataArray. If None (default), the `sanlc` product
        will be loaded with the bounding box of the vector file and used as the
        template.
    
    Returns
    -------
    mask : ndarray
        The output mask as a boolean NumPy array.
    
    Examples
    --------
    >>> import sdc.utils as utils
    >>> from sdc.load import load_product
    
    >>> vec = 'path/to/vector/file.geojson'
    >>> ds = load_product(product='s2_l2a', vec=vec)
    >>> mask = utils.mask_from_vec(vec=vec, da=ds.B04)
    >>> ds_masked = ds.where(mask)
    """
    import geopandas as gpd
    from rasterio.features import rasterize
    
    if da is None:
        da = load_product(product="sanlc", vec=vec)
    if 'time' in da.dims:
        da = da.isel(time=0)
    
    vec_data = gpd.read_file(vec)
    mask = rasterize([(geom, 1) for geom in vec_data.geometry],
                     out_shape=da.shape,
                     transform=da.odc.transform)
    return mask.astype('bool')


def separate_asc_desc(ds: Dataset) -> tuple[Dataset, Dataset]:
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
    
    if not any([x in ds_copy.data_vars for x in ['vv', 'vh']]):
        raise ValueError("Dataset doesn't contain Sentinel-1 data.")
    
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
