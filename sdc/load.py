import fiona

from typing import Optional, Tuple
from xarray import Dataset, DataArray

from sdc.vec import get_site_bounds
import sdc.products as prod


def load_product(product: str,
                 vec: str,
                 time_range: Optional[Tuple[str, str]] = None,
                 time_pattern: Optional[str] = None,
                 s2_apply_mask: bool = True,
                 sanlc_year: Optional[int] = None
                 ) -> Dataset | DataArray:
    """
    Load data products available in the SALDi Data Cube (SDC).
    
    Parameters
    ----------
    product : str
        Product to load. Currently supported products are:
        - s1_rtc
        - s2_l2a
        - sanlc
        - mswep
    vec : str
        Path to a vector file readable by fiona (e.g. shapefile, GeoJSON, etc.) or
        SALDi site name in the format 'siteXX', where XX is the site number. If a
        vector file is provided, its bounding box will be used to load the data.
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
    
    Returns
    -------
    ds : Dataset or DataArray
        Xarray Dataset or DataArray containing the loaded data.
    """
    if vec.lower() in ['site01', 'site02', 'site03', 'site04', 'site05', 'site06']:
        if product in ['s1_rtc', 's2_l2a']:
            print("WARNING: Loading data for an entire SALDi site will likely result "
                  "in performance issues as it will load data from multiple tiles. "
                  "Only do so if you know what you are doing and have optimized your "
                  "workflow! It is recommended to start with a small subset to test "
                  "your workflow before scaling up.")
        bounds = get_site_bounds(site=vec.lower())
    else:
        bounds = fiona.open(vec, 'r').bounds
    
    kwargs = {'bounds': bounds,
              'time_range': time_range,
              'time_pattern': time_pattern}
    
    if product == 's1_rtc':
        ds = prod.load_s1_rtc(**kwargs)
    elif product == 's2_l2a':
        ds = prod.load_s2_l2a(apply_mask=s2_apply_mask, **kwargs)
    elif product == 'sanlc':
        ds = prod.load_sanlc(bounds=bounds, year=sanlc_year)
    elif product == 'mswep':
        ds = prod.load_mswep(**kwargs)
    else:
        raise ValueError(f'Product {product} not supported')
    
    return ds
