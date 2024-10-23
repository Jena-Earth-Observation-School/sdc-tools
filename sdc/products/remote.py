from pystac_client import Client
from odc.stac import configure_rio, stac_load

from sdc import dask_client
from sdc.products import _ancillary as anc

from typing import Optional
from xarray import Dataset


def load_from_dea_stac(bounds: tuple[float, float, float, float],
                       collection: str,
                       time_range: tuple[str, str],
                       stac_filter: Optional[dict] = None,
                       bands: Optional[list[str]] = None,
                       override_defaults: Optional[dict] = None,
                       verbose: Optional[bool] = False
                       ) -> Dataset:
    """
    Load data from the DEA STAC Catalog.
    
    Parameters
    ----------
    bounds: tuple of float
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
        Will be used to filter the STAC Catalog for intersecting STAC Collections.
    collection : str
        The name of the STAC Collection to load data from.
    time_range : tuple of str
        The time range in the format (start_time, end_time) to filter STAC Items by.
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
    configure_rio(
        cloud_defaults=True,
        aws={"aws_unsigned": True},
        AWS_S3_ENDPOINT="s3.af-south-1.amazonaws.com",
        client=dask_client
    )
    
    catalog = Client.open("https://explorer.digitalearth.africa/stac")
    query = catalog.search(
        bbox=bounds,
        collections=[collection],
        filter=stac_filter,
        datetime=f"{time_range[0]}/{time_range[1]}"
    )
    items = list(query.items())
    
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    if verbose:
        print(f"Loading {len(items)} STAC Items with the following parameters: "
              f"{params}")
    
    ds = stac_load(items=items, bands=bands, bbox=bounds, **params)
    return ds
