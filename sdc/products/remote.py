import pystac_client
import planetary_computer
from odc.stac import configure_rio, stac_load
import numpy as np

from sdc import dask_client
from sdc.products import _ancillary as anc

from typing import Optional
from xarray import Dataset


def load_from_stac(stac_endpoint: str,
                   collection: str,
                   bounds: tuple[float, float, float, float],
                   time_range: tuple[str, str],
                   dtype: str,
                   nodata: Optional[int | float] = None,
                   stac_filter: Optional[dict] = None,
                   bands: Optional[list[str]] = None,
                   override_defaults: Optional[dict] = None,
                   verbose: Optional[bool] = False
                   ) -> Dataset:
    """
    Load data from the DEA STAC Catalog.
    
    Parameters
    ----------
    stac_endpoint : str
        The URL of the STAC endpoint to load data from. Two special cases are supported:
        - 'deafrica': loads data from the Digital Earth Africa STAC endpoint.
        - 'pc': loads data from the Planetary Computer STAC endpoint.
    collection : str
        The name of the STAC Collection to load data from.
    bounds : tuple of float
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
        Will be used to filter the STAC Catalog for intersecting STAC Collections.
    time_range : tuple of str
        The time range in the format (start_time, end_time) to filter STAC Items by.
    dtype : str, optional
        The data type to cast the loaded data to. Defaults to None.
    nodata : int or float, optional
        The nodata value to use for the loaded data. If `dtype` is a floating point 
        type, the default nodata value is `np.nan` else it will default to None.
    stac_filter : dict, optional
        A dictionary of additional filters to apply to the STAC Items. See the STAC API
        filter extension for more information.
    bands : list of str, optional
        A list of band names to load. Defaults to None, which will load all bands.
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
    verbose : bool, optional
        Whether to print information about the loading process. Defaults to False.
    
    Returns
    -------
    Dataset
        An xarray Dataset containing the loaded data.
    """
    if nodata is None:
        if isinstance(np.random.rand(1).astype(dtype)[0], np.floating):
            nodata = np.nan
    
    modifier = None
    if stac_endpoint == 'deafrica':
        configure_rio(
            cloud_defaults=True,
            aws={"aws_unsigned": True},
            AWS_S3_ENDPOINT="s3.af-south-1.amazonaws.com",
            client=dask_client
        )
        stac_endpoint = "https://explorer.digitalearth.africa/stac"
    elif stac_endpoint == 'pc':
        modifier = planetary_computer.sign_inplace
        stac_endpoint = "https://planetarycomputer.microsoft.com/api/stac/v1"
    else:
        stac_endpoint = stac_endpoint
    
    catalog = pystac_client.Client.open(stac_endpoint,
                                        modifier=modifier)
    query = catalog.search(
        collections=[collection],
        bbox=bounds,
        datetime=f"{time_range[0]}/{time_range[1]}",
        filter=stac_filter
    )
    items = list(query.items())
    
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    if verbose:
        print(f"Loading {len(items)} STAC Items with the following parameters: "
              f"{params}")
    
    ds = stac_load(items=items, bands=bands, bbox=bounds, 
                   nodata=nodata, dtype=dtype, **params)
    return ds
