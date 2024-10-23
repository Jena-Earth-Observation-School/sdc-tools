from pystac import Catalog
from odc.stac import load as odc_stac_load

from typing import Optional
from xarray import DataArray

from sdc.products import _ancillary as anc
from sdc.products import _query as query


def load_sanlc(bounds: tuple[float],
               year: Optional[int] = None,
               override_defaults: Optional[dict] = None
               ) -> DataArray:
    """
    Loads the South African National Land Cover (SANLC) data product for an area of 
    interest.
    
    Parameters
    ----------
    bounds: tuple of float
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
        Will be used to filter the STAC Catalog for intersecting STAC Collections.
    year: int, optional
        The year of the SANLC product to load. Default is None, which loads all
        available years, which currently are:
        - 2018
        - 2020
    override_defaults : dict, optional
        Dictionary of loading parameters to override the default parameters with. 
        Partial overriding is possible, i.e. only override a specific parameter while 
        keeping the others at their default values. For an overview of allowed 
        parameters, see documentation of `odc.stac.load`:
        https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html#odc-stac-load
        If `None` (default), the default parameters will be used: 
        - crs: 'EPSG:4326'
        - resolution: 0.0002
        - resampling: 'nearest' (*)
        - chunks: {'time': -1, 'latitude': 'auto', 'longitude': 'auto'}
        (*) This parameter is fixed for this specific product and cannot be
        overridden.
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the SANLC data.
    """
    product = 'sanlc'
    bands = ['nlc']
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(anc.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds)
    
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    params['resampling'] = 'nearest'
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='uint8',
                       **params)
    
    if year is not None:
        if year not in [2018, 2020]:
            raise ValueError('The SANLC product is only available for the years 2018 '
                             'and 2020.')
        ds = ds.sel(time=f'{year}-01-01', method='nearest')
    
    return ds.nlc
