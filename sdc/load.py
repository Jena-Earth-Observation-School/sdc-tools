import fiona

from typing import Optional, Tuple
from xarray import Dataset

from sdc.vec import get_site_bounds
from sdc.utils import groupby_acq_slices
from sdc.s1 import load_s1_rtc
from sdc.s2 import load_s2_l2a


def load_product(product: str, 
                 vec: str, 
                 time_range: Optional[Tuple[str, str]] = None,
                 time_pattern: Optional[str] = '%Y-%m-%d',
                 apply_mask: bool = True,
                 group_acq_slices: bool = True) -> Dataset:
    """
    Load data products available in the SALDi Data Cube (SDC).
    
    Parameters
    ----------
    product : str
        Product to load. Currently supported products are:
        - 's1_rtc'
        - 's2_l2a'
    vec : str
        Vector file path or SALDi site name in the format 'siteXX', where XX is the site
        number.
    time_range : tuple of str, optional
        Time range to load as a tuple of (start_time, stop_time), where start_time and
        stop_time are strings in the format specified by `time_pattern`. Default is
        None, which loads all available data.
    time_pattern : str, optional
        Time pattern to parse the time range. Default is '%Y-%m-%d'.
    apply_mask : bool, optional
        Whether to apply a quality mask to the data. Default is True. The specifics of
        the mask depend on the product. See the documentation of the specific product
        for details.
    group_acq_slices : bool, optional
        Whether to group acquisition slices. Default is True. For more details, see the
        documentation of `utils.group_acq_slices`.
    """
    if vec.lower() in ['site01', 'site02', 'site03', 'site04', 'site05',  'site06']:
        bounds = get_site_bounds(site=vec.lower())
    else:
        bounds = fiona.open(vec, 'r').bounds
    
    kwargs = {'bounds': bounds, 
              'time_range': time_range, 
              'time_pattern': time_pattern,
              'apply_mask': apply_mask}
    
    if product == 's1_rtc':
        ds = load_s1_rtc(**kwargs)
    elif product == 's2_l2a':
        ds = load_s2_l2a(**kwargs)
    else:
        raise ValueError(f'Product {product} not supported')
    
    if group_acq_slices:
        ds = groupby_acq_slices(ds)
    
    return ds
