import fiona
from pystac import Catalog
import numpy as np

from typing import Optional, Tuple, Any
from xarray import Dataset, DataArray
from pystac import Item

import sdc.utils as utils
import sdc.query as query


def load_s2_l2a(vec: str,
                time_range: Optional[Tuple[str, str]] = None,
                time_pattern: Optional[str] = '%Y-%m-%d',
                apply_mask: bool = True) -> Dataset:
    """
    Loads the Sentinel-2 L2A data for an area of interest.
    
    Parameters
    ----------
    vec : str
        Path to a vector file. Must be readable by fiona. The bounding box of the vector
        file will be used to filter the STAC Catalog for intersecting STAC Collections.
    time_range : tuple of str, optional
        The time range in the format (start_time, end_time) to filter STAC Items by.
        Defaults to None, which will load all STAC Items in the filtered STAC
        Collections.
    time_pattern : str, optional
        The pattern used to parse the time strings of `time_range`. Defaults to
        '%Y-%m-%d'.
    apply_mask : bool, optional
        Whether to apply a valid-data mask to the data. Defaults to True.
        The mask is created from the Scene Classification (SCL) band.
    
    Returns
    -------
    Dataset
        An xarray Dataset containing the Sentinel-2 L2A data.
    
    Notes
    -----
    The Sentinel-2 L2A data is sourced from Digital Earth Africa. For more product details,
    see: https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-2_Level-2A_specs.html
    """
    measurements = ('B02', 'B03', 'B04',  # Blue, Green, Red (10 m)
                    'B05', 'B06', 'B07',  # Red Edge 1, 2, 3 (20 m)
                    'B08',                # NIR (10 m)
                    'B8A',                # NIR 2 (20 m)
                    'B09',                # Water Vapour (60 m)
                    'B11', 'B12')         # SWIR 1, SWIR 2 (20 m)
    
    bbox = fiona.open(vec, 'r').bounds
    params = utils.common_params()
    
    catalog = Catalog.from_file(utils.get_catalog_path("s2_l2a"))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bbox,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    items = utils.convert_asset_hrefs(list_stac_obj=items, href_type='absolute')
    
    stackstac_params = {'items': items, 'assets': list(measurements), 'bounds': bbox, 'dtype': np.dtype("uint16"),
                        'fill_value': 0}
    stackstac_params.update(params)
    da = utils.stackstac_wrapper(params=stackstac_params)
    ds = utils.dataarray_to_dataset(da=da)
    
    # Apply cloud mask
    if apply_mask:
        params['bounds'] = bbox
        mask = _scl_mask(items=items, params=params)
        ds = ds.where(mask, other=0, drop=True)
    
    # Normalize the values to range [0, 1] and convert to float32
    ds = ds / 10000
    ds = ds.where((ds > 0) & (ds <= 1)).astype("float32")
    
    return ds


def _scl_mask(items: list[Item],
              params: dict[str, Any]) -> DataArray:
    """
    Creates a valid-data mask from the Scene Classification (SCL) band of Sentinel-2 L2A data.
    
    Parameters
    ----------
    items : list[Item]
        A list of STAC Items from Sentinel-2 L2A data.
    params : dict[str, Any]
        Parameters to pass to stackstac.stack.
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the valid-data mask.
    
    Notes
    -----
    An overview table of the SCL classes can be found in Table 3:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-2_Level-2A_specs.html#Specifications
    """
    stackstac_params = {'items': items, 'assets': ['SCL'], 'dtype': np.dtype("uint8"), 'fill_value': 0}
    stackstac_params.update(params)
    da = utils.stackstac_wrapper(params=stackstac_params)
    
    scl = da.sel(band='SCL')
    mask = (
            (scl == 4) |  # Vegetation
            (scl == 5) |  # Bare soils
            (scl == 6) |  # Water
            (scl == 7)    # Unclassified
    )
    return mask.compute()
