from pystac import Catalog
from odc.stac import load as odc_stac_load
import xarray as xr
import numpy as np

from typing import Optional, Any, Iterable
from xarray import Dataset, DataArray
from pystac import Item

from sdc.products import _ancillary as anc
from sdc.products import _query as query


def load_s1_rtc(bounds: tuple[float, float, float, float],
                time_range: Optional[tuple[str, str]] = None,
                time_pattern: Optional[str] = None,
                override_defaults: Optional[dict] = None,
                bands: Optional[list[str]] = None
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
    override_defaults : dict, optional
        Dictionary of loading parameters to override the default parameters with. 
        Partial overriding is possible, i.e. only override a specific parameter while 
        keeping the others at their default values. For an overview of allowed 
        parameters, see documentation of `odc.stac.load`:
        https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html#odc-stac-load
        If `None` (default), the default parameters will be used: 
        - crs: 'EPSG:4326'
        - resolution: 0.0002
        - resampling: 'bilinear'
        - chunks: {'time': -1, 'latitude': 'auto', 'longitude': 'auto'}
    bands : list of str, optional
        A list of band names to load. Defaults to None, which will load all bands.
    
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
    if bands is None:
        bands = ['vv', 'vh', 'area', 'angle']
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(anc.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='float32',
                       **params)
    if 'angle' in bands:
        ds['angle'] = _angle(items=items, bounds=bounds, params=params)
    return ds


def _angle(items: Iterable[Item],
           bounds: tuple[float, float, float, float],
           params: dict[str, Any]
           ) -> DataArray:
    """Loads the angle band from Sentinel-1 RTC product"""
    params['resampling'] = 'nearest'
    ds = odc_stac_load(items=items, bands='angle', bbox=bounds, dtype='uint8',
                       **params)
    return ds.angle


def load_s1_surfmi(bounds: tuple[float, float, float, float],
                   time_range: Optional[tuple[str, str]] = None,
                   time_pattern: Optional[str] = None,
                   override_defaults: Optional[dict] = None
                   ) -> DataArray:
    """
    Loads the Sentinel-1 Surface Moisture Index (SurfMI) product for an area of interest.
    
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
    override_defaults : dict, optional
        Dictionary of loading parameters to override the default parameters with. 
        Partial overriding is possible, i.e. only override a specific parameter while 
        keeping the others at their default values. For an overview of allowed 
        parameters, see documentation of `odc.stac.load`:
        https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html#odc-stac-load
        If `None` (default), the default parameters will be used: 
        - crs: 'EPSG:4326'
        - resolution: 0.0002
        - resampling: 'bilinear'
        - chunks: {'time': -1, 'latitude': 'auto', 'longitude': 'auto'}
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the Sentinel-1 SurfMI data.
    
    Notes
    -----
    See https://doi.org/10.3390/rs10091482 for more information on the SurfMI.
    """
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    chunks = params.pop('chunks')
    rechunk = chunks.copy()
    
    # make sure we use default chunks with time=1
    chunks = anc.common_params()['chunks']
    chunks['time'] = 1
    
    # Load dry and wet reference as well as s1_rtc data chunked per time step
    catalog = Catalog.from_file(anc.get_catalog_path(product='s1_smi_2'))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds)
    ds_ref = odc_stac_load(items=items, bbox=bounds, dtype='float32',
                           chunks=chunks, **params)
    meta_dry = items[0].assets['vv_q05'].href.removeprefix('./')
    meta_wet = items[0].assets['vv_q95'].href.removeprefix('./')
    
    catalog = Catalog.from_file(anc.get_catalog_path(product='s1_rtc'))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    ds_s1 = odc_stac_load(items=items, bands=['vv'], bbox=bounds, dtype='float32',
                          chunks=chunks, **params)
    
    # Squeeze time dimension from reference data and persist in cluster memory
    ds_ref = ds_ref.squeeze()
    dry_ref = ds_ref.vv_q05.persist()
    wet_ref = ds_ref.vv_q95.persist()
    
    # Calculate SurfMI and rechunk to wanted chunk size
    smi = ((ds_s1.vv - dry_ref)/(wet_ref - dry_ref))*100
    smi = smi.chunk(rechunk)
    smi = xr.where(smi < 0, 0, smi)
    smi = xr.where(smi > 100, 100, smi)
    
    smi = smi.assign_attrs(dry_reference=meta_dry, wet_reference=meta_wet)
    return smi


def load_s1_coherence(bounds: tuple[float, float, float, float],
                      time_range: Optional[tuple[str, str]] = None,
                      time_pattern: Optional[str] = None,
                      override_defaults: Optional[dict] = None
                      ) -> DataArray:
    """
    Loads the Sentinel-1 Coherence data product for an area of interest.
    
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
    override_defaults : dict, optional
        Dictionary of loading parameters to override the default parameters with. 
        Partial overriding is possible, i.e. only override a specific parameter while 
        keeping the others at their default values. For an overview of allowed 
        parameters, see documentation of `odc.stac.load`:
        https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html#odc-stac-load
        If `None` (default), the default parameters will be used: 
        - crs: 'EPSG:4326'
        - resolution: 0.0002
        - resampling: 'bilinear'
        - chunks: {'time': -1, 'latitude': 'auto', 'longitude': 'auto'}
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the Sentinel-1 Coherence data.
    
    Notes
    -----
    The Sentinel-1 Coherence data has been sourced from the Alaska Satellite Facility 
    (ASF). For more product details, see: 
    https://hyp3-docs.asf.alaska.edu/guides/insar_product_guide
    """
    product = 's1_coh_2'
    bands = ['coh_vv']
    
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(anc.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='float32',
                       **params)
    ds = xr.where(ds > 0, ds, np.nan)
    
    return ds.coh_vv
