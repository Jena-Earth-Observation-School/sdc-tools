import numpy as np
import xarray as xr
from pystac import Catalog
from odc.stac import load as odc_stac_load

from typing import Optional, Any, Iterable
from xarray import Dataset, DataArray
from pystac import Item

from sdc.utils import groupby_acq_slices
from sdc.products import _ancillary as anc
from sdc.products import _query as query


def load_s2_l2a(bounds: tuple[float, float, float, float] = None,
                collection_ids: Optional[list[str]] = None,
                time_range: Optional[tuple[str, str]] = None,
                time_pattern: Optional[str] = None,
                apply_mask: bool = True,
                group_acq_slices: bool = False,
                override_defaults: Optional[dict] = None,
                bands: Optional[list[str]] = None
                ) -> Dataset:
    """
    Loads the Sentinel-2 L2A data product for an area of interest.
    
    Parameters
    ----------
    bounds: tuple of float
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
        Will be used to filter the STAC Catalog for intersecting STAC Collections.
    collection_ids : list of str, optional
        A list of collection IDs to filter. If not None, this will override the `bbox`
        option.
    time_range : tuple of str, optional
        The time range in the format (start_time, end_time) to filter STAC Items by.
        Defaults to None, which will load all STAC Items in the filtered STAC
        Collections.
    time_pattern : str, optional
        Time pattern to parse the time range. Only needed if it deviates from the
        default: '%Y-%m-%d'.
    apply_mask : bool, optional
        Whether to apply a valid-data mask to the data. Defaults to True.
        The mask is created from the `SCL` (Scene Classification Layer) band of the
        product.
    group_acq_slices : bool, optional
        Whether to group the data by acquisition slices. Defaults to False.
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
    bands: list of str, optional
        A list of band names to load. Defaults to None, which will load all bands.
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
    if bands is None:
        bands = ['B02', 'B03', 'B04',  # Blue, Green, Red (10 m)
                 'B05', 'B06', 'B07',  # Red Edge 1, 2, 3 (20 m)
                 'B08',                # NIR (10 m)
                 'B8A',                # NIR 2 (20 m)
                 'B09',                # Water Vapour (60 m)
                 'B11', 'B12']         # SWIR 1, SWIR 2 (20 m)
    
    if bounds is None and collection_ids is None:
        raise ValueError("Either `bounds` or `collection_ids` must be provided.")

    # Load and filter STAC Items
    catalog = Catalog.from_file(anc.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, 
                                         bbox=bounds,
                                         collection_ids=collection_ids,
                                         time_range=time_range,
                                         time_pattern=time_pattern)
    
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    chunks = params.pop('chunks')
    rechunk = None
    if apply_mask:
        rechunk = chunks.copy()
        # make sure we use default chunks with time=1
        chunks = anc.common_params()['chunks']
        chunks['time'] = 1
    
    # Turn into dask-based xarray.Dataset
    ds = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='uint16',
                       chunks=chunks, **params)
    
    if apply_mask:
        valid = _mask(items=items, bounds=bounds, chunks=chunks, params=params)
        ds = xr.where(valid, ds, 0)
    
    # Normalize the values to range [0, 1] and convert to float32
    ds = ds / 10000
    cond = (ds > 0) & (ds <= 1)
    ds = xr.where(cond, ds, np.nan).astype("float32")
    
    # Optional processing steps
    if group_acq_slices:
        ds = groupby_acq_slices(ds)
    if apply_mask:
        ds = ds.chunk(rechunk)
    return ds


def _mask(items: Iterable[Item],
          bounds: tuple[float, float, float, float],
          chunks: dict[str, Any],
          params: dict[str, Any]
          ) -> DataArray:
    """
    Creates a valid-data mask from the `SCL` (Scene Classification Layer) band of
    Sentinel-2 L2A data.
    
    Notes
    -----
    An overview table of the SCL classes can be found in Table 3:
    https://docs.digitalearthafrica.org/en/latest/data_specs/Sentinel-2_Level-2A_specs.html#Specifications
    
    The selection of which classes to consider as valid data is based on
    Baetens et al. (2019): https://doi.org/10.3390/rs11040433 (Table 4).
    """
    ds = odc_stac_load(items=items, bands='SCL', bbox=bounds, dtype='uint8',
                       chunks=chunks, **params)
    mask = ((ds.SCL == 2) |  # dark area pixels
            (ds.SCL > 3) &   # vegetation, bare soils, water, unclassified
            (ds.SCL <= 7) |
            (ds.SCL == 11)   # snow/ice
            )
    return mask
