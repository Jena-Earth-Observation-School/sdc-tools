from pystac import Catalog
from odc.stac import load as odc_stac_load

from typing import Tuple
from xarray import DataArray

import sdc.utils as utils
import sdc.query as query


def load_sanlc(bounds: Tuple[float, float, float, float]
               ) -> DataArray:
    """
    Loads the South African National Land Cover (SANLC) data product for an area of 
    interest.
    
    Parameters
    ----------
    bounds: tuple of float
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
        Will be used to filter the STAC Catalog for intersecting STAC Collections.
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the SANLC data.
    """
    product = 'sanlc'
    bands = ['nlc']
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(utils.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds)
    
    common_params = utils.common_params()
    common_params['resampling'] = 'nearest'
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='uint8',
                       **common_params)
    
    return ds.nlc.squeeze()
