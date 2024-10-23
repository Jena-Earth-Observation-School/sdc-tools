from pathlib import Path
import geopandas as gpd

from typing import Optional
from xarray import Dataset, DataArray


def load_product(product: str,
                 vec: str | Path | list[float, float, float, float],
                 time_range: Optional[tuple[str, str]] = None,
                 time_pattern: Optional[str] = None,
                 s2_apply_mask: bool = True,
                 sanlc_year: Optional[int] = None,
                 override_defaults: Optional[dict] = None
                 ) -> Dataset | DataArray:
    """
    Load data products available in the SALDi Data Cube (SDC).
    
    Parameters
    ----------
    product : str
        Product to load. Currently supported products are:
        - s1_rtc
        - s1_surfmi
        - s1_coh
        - s2_l2a
        - sanlc
        - mswep
        - cop_dem
    vec : str or Path or list of float
        Several options to define the spatial extent of the data to load:
        - Path to a vector file readable by GeoPandas (e.g. GeoJSON, Geopackage etc.). 
        In this case, the bounding box of the vector file will be used to load the data.
        - A list of float values defining a bounding box in the format: [minx, miny, 
        maxx, maxy].
        - A SALDi site name in the format 'siteXX', where XX is the site number.
    time_range : tuple of str, optional
        Time range to load as a tuple of strings in the form of: (start_time, stop_time)
        , where start_time and stop_time are strings in the format specified by
        `time_pattern`. Default is None, which loads all available data.
    time_pattern : str, optional
        Time pattern to parse the time range. Only needed if it deviates from the
        default: '%Y-%m-%d'.
    s2_apply_mask : bool, optional
        Whether to apply a valid-data mask to the Sentinel-2 L2A product based on its
        `SCL` (Scene Classification Layer) band. Default is True. This parameter will
        be ignored if `product` is not `s2_l2a`.
    sanlc_year : int, optional
        The year of the SANLC product to load. Default is None, which loads all
        available years. This parameter will be ignored if `product` is not `sanlc`.
        Currently supported years are:
        - 2018
        - 2020
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
    ds : Dataset or DataArray
        Xarray Dataset or DataArray containing the loaded data.
    """
    from sdc.vec import get_site_bounds
    import sdc.products as prod
    
    
    if override_defaults is not None:
        print("[WARNING] Overriding default loading parameters is only recommended for "
              "advanced users. Start with the default parameters and only override "
              "them if you know what you are doing.")
        if product == 'mswep':
            print("[INFO] Overriding default loading parameters is currently not "
                  "supported for the MSWEP product. Default parameters will be used "
                  "instead.")
    
    # `bbox`-parameter of `odc.stac.load` needs to be in lat/lon!
    crs = 4326
    if isinstance(vec, list):
        bounds = tuple(vec)
    elif isinstance(vec, (Path, str)):
        vec = str(vec)
        if vec.lower() in ['site01', 'site02', 'site03', 'site04', 'site05', 'site06']:
            if product in ['s1_rtc', 's1_surfmi', 's2_l2a']:
                print("[WARNING] Loading data for an entire SALDi site will likely result "
                    "in performance issues as it will load data from multiple tiles. "
                    "Only do so if you know what you are doing and have optimized your "
                    "workflow! It is recommended to start with a small subset to test "
                    "your workflow before scaling up.")
            bounds = get_site_bounds(site=vec.lower(), crs=crs)
        else:
            vec_gdf = gpd.read_file(vec)
            vec_gdf = vec_gdf.to_crs(crs)
            bounds = tuple(vec_gdf.total_bounds)
    else:
        raise ValueError(f'Vector input {vec} not supported')
    
    kwargs = {'bounds': bounds,
              'time_range': time_range,
              'time_pattern': time_pattern}
    
    if product == 's1_rtc':
        ds = prod.load_s1_rtc(override_defaults=override_defaults, **kwargs)
    elif product == 's1_surfmi':
        ds = prod.load_s1_surfmi(override_defaults=override_defaults, **kwargs)
    elif product == 's1_coh':
        ds = prod.load_s1_coherence(override_defaults=override_defaults, **kwargs)
    elif product == 's2_l2a':
        ds = prod.load_s2_l2a(apply_mask=s2_apply_mask, 
                              override_defaults=override_defaults, **kwargs)
    elif product == 'sanlc':
        ds = prod.load_sanlc(bounds=bounds, 
                             year=sanlc_year, 
                             override_defaults=override_defaults)
    elif product == 'mswep':
        ds = prod.load_mswep(**kwargs)
    elif product == 'cop_dem':
        ds = prod.load_copdem(bounds=bounds, 
                              override_defaults=override_defaults)
    else:
        raise ValueError(f'Product {product} not supported')
    
    return ds
