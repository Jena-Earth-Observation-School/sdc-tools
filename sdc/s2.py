from pystac import Catalog
import numpy as np

from typing import Optional, Tuple, Any
from xarray import Dataset, DataArray
from pystac import Item

import sdc.utils as utils
import sdc.query as query


def load_s2_l2a(bounds: Tuple[float, float, float, float],
                time_range: Optional[Tuple[str, str]] = None,
                time_pattern: Optional[str] = '%Y-%m-%d',
                apply_mask: bool = True) -> Dataset:
    """
    Loads the Sentinel-2 L2A data product for an area of interest.
    
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
        The mask is created from the `SCL` (Scene Classification Layer) band of the
        product.
    
    Returns
    -------
    Dataset
        An xarray Dataset containing the Sentinel-2 L2A data.
    
    Notes
    -----
    The Sentinel-2 L2A data is sourced from Digital Earth Africa.
    For more product details, see:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-2_Level-2A_specs.html
    """
    product = 's2_l2a'
    measurements = ('B02', 'B03', 'B04',  # Blue, Green, Red (10 m)
                    'B05', 'B06', 'B07',  # Red Edge 1, 2, 3 (20 m)
                    'B08',                # NIR (10 m)
                    'B8A',                # NIR 2 (20 m)
                    'B09',                # Water Vapour (60 m)
                    'B11', 'B12')         # SWIR 1, SWIR 2 (20 m)
    fill_value = 0
    dtype = np.dtype("uint16")
    out_dtype = "float32"
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
                        'dtype': dtype, 'fill_value': fill_value}
    stackstac_params.update(params)
    da = utils.stackstac_wrapper(params=stackstac_params)
    ds = utils.dataarray_to_dataset(da=da)
    
    # Apply cloud mask
    if apply_mask:
        params['bounds'] = bounds
        valid = _mask(items=items, params=params)
        ds = ds.where(valid, other=0, drop=True)
    
    # Normalize the values to range [0, 1] and convert to `out_dtype`
    ds = ds / 10000
    ds = ds.where((ds > 0) & (ds <= 1)).astype(out_dtype)
    
    return ds


def _mask(items: list[Item],
          params: dict[str, Any]) -> DataArray:
    """
    Creates a valid-data mask from the `SCL` (Scene Classification Layer) band of
    Sentinel-2 L2A data.
    
    Parameters
    ----------
    items : list[Item]
        A list of STAC Items from Sentinel-2 L2A data.
    params : dict[str, Any]
        Parameters to pass to `stackstac.stack`
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the valid-data mask.
    
    Notes
    -----
    An overview table of the SCL classes can be found in Table 3:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-2_Level-2A_specs.html#Specifications
    """
    stackstac_params = {'items': items, 'assets': ['SCL'], 'dtype': np.dtype("uint8"),
                        'fill_value': 0}
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
