import xarray as xr
from pystac import Catalog
from odc.stac import load as odc_stac_load

from typing import Optional, Tuple, List
from xarray import Dataset, DataArray
from pystac import Item

import sdc.utils as utils
import sdc.query as query


def load_s2_l2a(bounds: Tuple[float, float, float, float],
                time_range: Optional[Tuple[str, str]] = None,
                time_pattern: str = '%Y-%m-%d',
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
    bands = ['B02', 'B03', 'B04',  # Blue, Green, Red (10 m)
             'B05', 'B06', 'B07',  # Red Edge 1, 2, 3 (20 m)
             'B08',                # NIR (10 m)
             'B8A',                # NIR 2 (20 m)
             'B09',                # Water Vapour (60 m)
             'B11', 'B12']         # SWIR 1, SWIR 2 (20 m)
    common_params = utils.common_params()
    
    # Load and filter STAC Items
    catalog = Catalog.from_file(utils.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=list(bounds), dtype='uint16',
                       **common_params)
    
    if apply_mask:
        valid = _mask(items=items, bounds=bounds)
        ds = xr.where(valid, ds, 0)
    
    # Normalize the values to range [0, 1] and convert to float32
    ds = ds / 10000
    ds = ds.where((ds > 0) & (ds <= 1)).astype("float32")
    
    return ds


def _mask(items: List[Item],
          bounds: Tuple[float, float, float, float]) -> DataArray:
    """
    Creates a valid-data mask from the `SCL` (Scene Classification Layer) band of
    Sentinel-2 L2A data.
    
    Notes
    -----
    An overview table of the SCL classes can be found in Table 3:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-2_Level-2A_specs.html#Specifications
    """
    ds = odc_stac_load(items=items, bands='SCL', bbox=bounds, dtype='uint8',
                       **utils.common_params())
    mask = (
            (ds.SCL == 4) |  # Vegetation
            (ds.SCL == 5) |  # Bare soils
            (ds.SCL == 6) |  # Water
            (ds.SCL == 7)    # Unclassified
    )
    return mask
