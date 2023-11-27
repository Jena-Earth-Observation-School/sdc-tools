import numpy as np

from typing import Tuple, Optional
from xarray import DataArray, Dataset
from numpy import ndarray

from sdc.load import load_product


def groupby_acq_slices(ds: Dataset) -> Dataset:
    """
    Groups acquisition slices of all data variables in a Dataset by calculating the mean
    for each rounded 1-hour time interval.
    
    Parameters
    ----------
    ds : Dataset
        The Dataset to be grouped.
    
    Returns
    -------
    ds_copy : Dataset
        The grouped Dataset.
    
    Examples
    --------
    >>> import sdc.utils as utils
    >>> from sdc.load import load_product
    
    >>> ds = load_product(product='s2_l2a', vec='path/to/vector/file.geojson')
    >>> ds_grouped = utils.groupby_acq_slices(ds)
    """
    ds_copy = ds.copy(deep=True)
    ds_copy.coords['time'] = ds_copy.time.dt.round('1H')
    ds_copy = ds_copy.groupby('time').mean(skipna=True)
    if len(np.unique(ds.time.dt.date)) < len(ds_copy.time):
        print("Warning: Might have missed to group some acquisition slices!")
    return ds_copy


def ds_nanquantiles(ds: Dataset,
                    dim: Optional[str | Tuple[str]] = None,
                    variables: Optional[str | Tuple[str]] = None,
                    quantiles: Optional[float | Tuple[float]] = None,
                    compute: bool = False
                    ) -> Dataset:
    """
    Aggregate the time dimension of a Dataset by calculating quantiles for each data
    variable. Returns a new Dataset with the quantiles as new variables.
    
    Parameters
    ----------
    ds : Dataset
        The Dataset to calculate quantiles for.
    dim : str or tuple of str, optional
        Dimension(s) to reduce. If None (default), the 'time' dimension will be reduced.
    variables : str or tuple of str, optional
        The data variables to calculate quantiles for. If None (default), quantiles will
        be calculated for all data variables.
    quantiles : float or tuple of float, optional
        The quantiles to calculate. If None (default), the quantiles (0.05, 0.95) will
        be calculated.
    compute : bool, optional
        Whether to compute the new variables into memory. Default is False, which means
        that the new variables will be lazily evaluated.
    
    Returns
    -------
    ds_copy : Dataset
        The new Dataset with the quantiles as new variables.
    
    Examples
    --------
    >>> import sdc.utils as utils
    >>> from sdc.load import load_product
    
    >>> ds = load_product(product='s1_rtc', vec='path/to/vector/file.geojson')
    >>> q = (0.05, 0.95)
    >>> v = ('vv', 'vh')
    >>> ds_quantiles = utils.ds_nanquantiles(ds=ds, variables=v, quantiles=q)
    """
    ds_copy = ds.copy(deep=True)
    if dim is None:
        dim = 'time'
    if isinstance(dim, str):
        dim = (dim,)
    else:
        dim = tuple(dim)
    if variables is None:
        variables = list(ds_copy.data_vars)
        other_variables = []
    else:
        if isinstance(variables, str):
            variables = [variables]
        else:
            variables = list(variables)
        other_variables = [v for v in list(ds_copy.data_vars) if v not in variables]
    if quantiles is None:
        quantiles = (0.05, 0.95)
    else:
        if isinstance(quantiles, float):
            quantiles = (quantiles,)
        else:
            quantiles = tuple(quantiles)
    q = quantiles
    quantiles = np.atleast_1d(np.asarray(quantiles, dtype=np.float64))
    
    # Calculate quantiles for each variable (DataArray) using da_nanquantiles
    for v in variables:
        ds_copy[f'{v}_quantiles'] = da_nanquantiles(da=ds_copy[v], 
                                                    quantiles=q,
                                                    dim=dim)
        for x in quantiles:
            q_str = str(int(x * 100))
            ds_copy[f'{v}_q{q_str}'] = ds_copy[f'{v}_quantiles'].sel(quantile=x)
    
    # Cleanup; we only want the new quantile variables
    ds_copy = ds_copy.drop_vars(variables + other_variables +
                                [f'{v}_quantiles' for v in variables])
    ds_copy = ds_copy.drop_dims(['quantile'] + list(dim))
    
    # Cast back to float32
    ds_copy = ds_copy.astype('float32')
    
    if compute:
        ds_copy = ds_copy.compute()
    return ds_copy


def da_nanquantiles(da: DataArray,
                    dim: Optional[str | Tuple[str]] = None,
                    quantiles: float | Tuple[float] = None
                    ) -> DataArray:
    """
    Calculate quantiles along a given dimension of a DataArray, ignoring NaN values.
    If multiple quantiles are given, the returned DataArray will have a new dimension
    'quantile' with the quantile values as coordinates. If only a single quantile is
    given, the quantile dimension will be dropped.
    
    Parameters
    ----------
    da: DataArray
        The DataArray to calculate quantiles for.
    dim : str or tuple of str, optional
        Dimension(s) to reduce. If None (default), the 'time' dimension will be reduced.
    quantiles : float or tuple of float, optional
        The quantiles to calculate. If None (default), the quantiles (0.05, 0.95) will
        be returned.
    
    Returns
    -------
    result: DataArray
        If `q` is a single quantile, then the result is a scalar. If multiple
        quantiles are given, first axis of the result corresponds to the quantile and a
        quantile dimension is added to the return array. The other dimensions are the
        dimensions that remain after the reduction of the array.
    
    Examples
    --------
    >>> import sdc.utils as utils
    >>> from sdc.load import load_product
    
    >>> ds = load_product(product='s1_rtc', vec='path/to/vector/file.geojson')
    >>> da = ds.vv
    >>> da_quantiles = utils.da_nanquantiles(ds=ds, quantiles=(0.05, 0.95))
    >>> da_quantiles.sel(quantile=0.05)
    
    Notes
    -----
    This is a simple workaround for https://github.com/pydata/xarray/issues/7377.
    The structure of this function is based on:
    https://github.com/pydata/xarray/blob/6c5840e1198707cdcf7dc459f27ea9510eb76388/xarray/core/variable.py#L2128-L2271
    Instead of `numpy.nanquantile`, the following `nanquantile` method of the numbagg
    package is implemented:
    https://github.com/numbagg/numbagg/blob/50357a2545c831a911c2fa92fc7de485a2f610aa/numbagg/funcs.py#L184
    """
    from xarray.core.utils import is_scalar
    from xarray.core.computation import apply_ufunc
    from numbagg import nanquantile
    
    if dim is None:
        dim = 'time'
    if isinstance(dim, str):
        dim = [dim]
    else:
        dim = list(dim)
    
    scalar = is_scalar(quantiles)
    if quantiles is None:
        quantiles = (0.05, 0.95)
    else:
        if isinstance(quantiles, float):
            quantiles = (quantiles,)
        else:
            quantiles = tuple(quantiles)
    quantiles = np.atleast_1d(np.asarray(quantiles, dtype=np.float64))
    
    def _wrapper(x, **kwargs):
        # move quantile axis to end. required for apply_ufunc
        return np.moveaxis(nanquantile(x.copy(), **kwargs), source=0, destination=-1)
    
    result = apply_ufunc(
        _wrapper,
        da,
        input_core_dims=[dim],
        exclude_dims=set(dim),
        output_core_dims=[["quantile"]],
        output_dtypes=[np.float64],
        dask_gufunc_kwargs=dict(output_sizes={"quantile": len(quantiles)}),
        dask="parallelized",
        kwargs={'quantiles': quantiles, 'axis': [-1]},
    )
    result = result.assign_coords(quantile=DataArray(quantiles, dims=("quantile",)))
    result = result.transpose("quantile", ...)
    
    if scalar:
        result = result.squeeze("quantile")
    
    result.attrs = da.attrs.copy()
    return result


def mask_from_multipolygon(vec: str,
                           da: Optional[DataArray] = None
                           ) -> ndarray:
    """
    Create a boolean mask from a vector file containing a single MultiPolygon feature.
    
    Parameters
    ----------
    vec : str
        Path to a vector file readable by geopandas (e.g. shapefile, GeoJSON, etc.).
    da : DataArray, optional
        DataArray to use as a template for the mask, which will be created with the same
        shape and transform as the DataArray. If None (default), the `sanlc` product
        will be loaded with the bounding box of the vector file and used as the
        template.
    
    Returns
    -------
    mask : ndarray
        The output mask as a boolean NumPy array.
    
    Examples
    --------
    >>> import sdc.utils as utils
    >>> from sdc.load import load_product
    >>> import xarray as xr
    >>> import numpy as np
    
    >>> vec = 'path/to/vector/file.geojson'
    >>> ds = load_product(product='s2_l2a', vec=vec)
    >>> mask = utils.mask_from_multipolygon(vec=vec)
    >>> ds_masked = xr.where(mask, ds, np.nan)
    """
    import geopandas as gpd
    from rasterio.features import rasterize
    
    if da is None:
        da = load_product(product="sanlc", vec=vec)
    if 'time' in da.dims:
        da = da.isel(time=0)
    
    vec_data = gpd.read_file(vec)
    mask = rasterize([(geom, 1) for geom in vec_data.geometry],
                     out_shape=da.shape,
                     transform=da.odc.transform)
    return mask.astype('bool')
