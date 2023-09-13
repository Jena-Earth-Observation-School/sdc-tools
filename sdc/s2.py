import fiona
from pystac import Catalog
import stackstac

import sdc.utils as utils
import sdc.query as query

from typing import Optional, Tuple
from xarray import Dataset


def load_s2_l2a(vec: str,
                time_range: Optional[Tuple[str, str]] = None,
                time_pattern: Optional[str] = '%Y-%m-%d') -> Dataset:
    """
    Loads the Sentinel-2 L2A data for an area of interest.

    Parameters
    ----------
    vec : str
        Path to a vector file. Must be readable by fiona. The bounding box of the vector file will be used to filter
        the STAC Catalog for matching STAC Collections.
    time_range : tuple of str, optional
        The time range in the format (start_time, end_time) to filter STAC Items by. Defaults to None, which will
        load all STAC Items in the STAC Collections.
    time_pattern : str, optional
        The pattern used to parse the time strings of `time_range`. Defaults to '%Y-%m-%d'.

    Returns
    -------
    Dataset
        An xarray Dataset containing the Sentinel-2 L2A data.
    """
    bbox = fiona.open(vec, 'r').bounds
    params = utils.common_params()
    
    catalog = Catalog.from_file(utils.get_catalog_path("s2_l2a"))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bbox, time_range=time_range, time_pattern=time_pattern)
    items = utils.convert_asset_hrefs(list_stac_obj=items, href_type='absolute')
    
    da = stackstac.stack(items=items, bounds=bbox, **params)
    ds = utils.dataarray_to_dataset(da=da)
    
    return ds
