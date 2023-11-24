import fiona

from typing import Optional, Tuple
from xarray import Dataset, DataArray

from sdc.vec import get_site_bounds
from sdc.s1 import load_s1_rtc
from sdc.s2 import load_s2_l2a
from sdc.sanlc import load_sanlc
from sdc.mswep import load_mswep


def load_product(product: str,
                 vec: str,
                 time_range: Optional[Tuple[str, str]] = None,
                 time_pattern: Optional[str] = None,
                 s2_apply_mask: bool = True
                 ) -> Dataset | DataArray:
    """
    Load data products available in the SALDi Data Cube (SDC).
    
    Parameters
    ----------
    product : str
        Product to load. Currently supported products are:
        - 's1_rtc'
        - 's2_l2a'
        - 'sanlc'
        - 'mswep'
    vec : str
        Vector file path or SALDi site name in the format 'siteXX', where XX is the site
        number.
    time_range : tuple of str, optional
        Time range to load as a tuple of (start_time, stop_time), where start_time and
        stop_time are strings in the format specified by `time_pattern`. Default is
        None, which loads all available data.
    time_pattern : str, optional
        Time pattern to parse the time range. Only needed if it deviates from the
        default: '%Y-%m-%d'.
    s2_apply_mask : bool, optional
        Whether to apply a valid-data mask to the Sentinel-2 data. Default is True.
        The mask is created from the `SCL` (Scene Classification Layer) band of the
        product.
    
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
        ds = load_s1_rtc(**kwargs)
    elif product == 's2_l2a':
        ds = load_s2_l2a(apply_mask=s2_apply_mask, **kwargs)
    elif product == 'sanlc':
        ds = load_sanlc(bounds=bounds)
    elif product == 'mswep':
        ds = load_mswep(**kwargs)
    else:
        raise ValueError(f'Product {product} not supported')
    
    return ds
