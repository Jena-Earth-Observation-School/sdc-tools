from pystac import Catalog
from odc.stac import load as odc_stac_load

from typing import Optional
from xarray import DataArray

from sdc.products import _ancillary as anc
from sdc.products import _query as query


def load_sanlc(bounds: tuple[float, float, float, float],
               year: Optional[int] = None
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
    
    common_params = anc.common_params()
    common_params['resampling'] = 'nearest'
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='uint8',
                       **common_params)
    
    if year is not None:
        if year not in [2018, 2020]:
            raise ValueError('The SANLC product is only available for the years 2018 '
                             'and 2020.')
        ds = ds.sel(time=f'{year}-01-01', method='nearest')
    
    return ds.nlc
