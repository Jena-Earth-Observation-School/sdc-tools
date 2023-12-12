from pystac import Catalog
from odc.stac import load as odc_stac_load
import xarray as xr

from typing import Optional
from xarray import Dataset, DataArray

from sdc.products import _ancillary as anc
from sdc.products import _query as query


def load_s1_rtc(bounds: tuple[float, float, float, float],
                time_range: Optional[tuple[str, str]] = None,
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
    catalog = Catalog.from_file(anc.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='float32',
                       **anc.common_params())
    
    return ds


def load_s1_surfmi(bounds: tuple[float, float, float, float],
                   time_range: Optional[tuple[str, str]] = None,
                   time_pattern: Optional[str] = None
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
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the Sentinel-1 SurfMI data.
    
    Notes
    -----
    See https://doi.org/10.3390/rs10091482 for more information on the SurfMI.
    """
    # Load dry and wet reference as well as s1_rtc data chunked per time step
    common_params = anc.common_params()
    common_params['chunks']['time'] = 1
    
    catalog = Catalog.from_file(anc.get_catalog_path(product='s1_smi_2'))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds)
    ds_ref = odc_stac_load(items=items, bbox=bounds, dtype='float32', 
                           **common_params)
    meta_dry = items[0].assets['vv_q05'].href.removeprefix('./')
    meta_wet = items[0].assets['vv_q95'].href.removeprefix('./')
    
    catalog = Catalog.from_file(anc.get_catalog_path(product='s1_rtc'))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    ds_s1 = odc_stac_load(items=items, bands=['vv'], bbox=bounds, dtype='float32',
                          **common_params)
    
    # Squeeze time dimension from reference data and persist in cluster memory
    ds_ref = ds_ref.squeeze()
    dry_ref = ds_ref.vv_q05.persist()
    wet_ref = ds_ref.vv_q95.persist()
    
    # Calculate SurfMI
    smi = ((ds_s1.vv - dry_ref)/(wet_ref - dry_ref))*100
    smi = smi.chunk({'time': -1, 'latitude': 'auto', 'longitude': 'auto'})
    smi = xr.where(smi < 0, 0, smi)
    smi = xr.where(smi > 100, 100, smi)
    
    smi = smi.assign_attrs(dry_reference=meta_dry, wet_reference=meta_wet)
    return smi


def load_s1_coherence(bounds: tuple[float, float, float, float],
                      time_range: Optional[tuple[str, str]] = None,
                      time_pattern: Optional[str] = None
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
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(anc.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='float32',
                       **anc.common_params())
    
    return ds.coh_vv
