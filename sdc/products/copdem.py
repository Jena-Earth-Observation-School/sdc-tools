from pystac import Catalog
from odc.stac import load as odc_stac_load
import rioxarray
from rasterio.enums import Resampling
from xrspatial import slope, aspect

from typing import Optional
from xarray import DataArray

from sdc.products import _ancillary as anc
from sdc.products import _query as query


def load_copdem(bounds: tuple[float, float, float, float],
                override_defaults: Optional[dict] = None
                ) -> DataArray:
    """
    Loads the Copernicus 30m GLO DEM (COP-DEM) data product for an area of interest.
    
    Parameters
    ----------
    bounds: tuple of float
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
        Will be used to filter the STAC Catalog for intersecting STAC Collections.
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
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the COP-DEM data.
    """
    product = 'cop_dem'
    bands = ['height']
    
    catalog = Catalog.from_file(anc.get_catalog_path(product=product))
    _, items = query.filter_stac_catalog(catalog=catalog, bbox=bounds)
    
    params = anc.common_params()
    if override_defaults is not None:
        params = anc.override_common_params(params=params, **override_defaults)
    da = odc_stac_load(items=items, bands=bands, bbox=bounds, dtype='float32',
                       **params)
    da = da.height.squeeze()
    
    # Calculate slope and aspect
    da_slope, da_aspect = _calc_slope_aspect(da=da)
    
    # Create Dataset
    ds = da.to_dataset(name=bands[0])
    ds = ds.rename_vars({"height": "elevation"})
    ds['slope'] = da_slope
    ds['aspect'] = da_aspect
    
    return ds


def _calc_slope_aspect(da: DataArray
                       ) -> tuple[DataArray, DataArray]:
    """Calculate slope and aspect from a Copernicus DEM DataArray."""
    
    # Reproject DataArray to UTM
    utm_epsg = f'EPSG:{da.rio.estimate_utm_crs().to_epsg()}'
    da_utm = da.rio.reproject(dst_crs=utm_epsg, 
                              resampling=Resampling.bilinear)
    
    # Calculate slope and aspect
    da_slope_utm = slope(da_utm)
    da_aspect_utm = aspect(da_utm)
    
    # Reproject slope and aspect back to original CRS
    da_slope = da_slope_utm.rio.reproject_match(match_data_array=da, 
                                                resampling=Resampling.bilinear)
    da_aspect = da_aspect_utm.rio.reproject_match(match_data_array=da,
                                                  resampling=Resampling.bilinear)
    da_slope = da_slope.rename({'x': 'longitude', 'y': 'latitude'})
    da_aspect = da_aspect.rename({'x': 'longitude', 'y': 'latitude'})
    
    return da_slope, da_aspect
