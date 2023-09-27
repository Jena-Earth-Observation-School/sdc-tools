import warnings
from copy import deepcopy
from pathlib import Path
import numpy as np

from typing import List, Any
from pystac import Catalog, Collection, Item
from xarray import DataArray, Dataset


def get_catalog_path(product: str) -> str:
    """
    Gets the path to the STAC Catalog file for a given product.
    
    Parameters
    ----------
    product : str
        Name of the data product.
    
    Returns
    -------
    str
        Path to the STAC Catalog file.
    """
    base_path = Path("/geonfs/02_vol3/SaldiDataCube/original_data")
    _dir = base_path.joinpath(product.upper())
    _file = _dir.joinpath("catalog.json")
    if not _dir.exists():
        raise FileNotFoundError(f"Product '{product}': Could not find product directory {_dir}")
    if not _file.exists():
        raise FileNotFoundError(f"Product '{product}': Could not find STAC Catalog file {_file}")
    return str(_file)


def common_params() -> dict[str, Any]:
    """
    Returns parameters common to all products.
    
    Returns
    -------
    dict[str, Any]
         Dictionary of parameters that are common to all products.
    """
    from rasterio.enums import Resampling
    return {"epsg": 4326,
            "resolution": 0.0002,  # actually pixel spacing, not resolution!
            "resampling": Resampling['bilinear'],
            "xy_coords": 'center',
            "chunksize": (-1, 1, 'auto', 'auto')}


def convert_asset_hrefs(list_stac_obj: List[Catalog | Collection | Item],
                        href_type: str
                        ) -> List[Catalog | Collection | Item] | List[None]:
    """
    Converts the asset hrefs of a list of STAC Objects (Catalogs, Collections or Items)
    to either absolute or relative.
    
    Parameters
    ----------
    list_stac_obj : List[Catalog | Collection | Item]
        List of STAC Objects.
    href_type : str
        Type of href to convert to. Can be either 'absolute' or 'relative'.
    
    Returns
    -------
    List[Catalog | Collection | Item] | List[None]
        A list of STAC Objects with converted asset hrefs.
    """
    list_stac_obj_copy = deepcopy(list_stac_obj)
    if len(list_stac_obj_copy) == 0:
        return list_stac_obj_copy
    for stac_obj in list_stac_obj_copy:
        if href_type == 'absolute':
            stac_obj.make_asset_hrefs_absolute()
        elif href_type == 'relative':
            stac_obj.make_asset_hrefs_relative()
    return list_stac_obj_copy


def dataarray_to_dataset(da: DataArray) -> Dataset:
    """
    Converts a DataArray loaded using the stackstac library to a Dataset.
    
    Parameters
    ----------
    da : DataArray
        The DataArray to convert.
    
    Returns
    -------
    Dataset
        The converted Dataset with band dimension dropped and band coordinates saved as attrs in variables.
    
    Notes
    -----
    Source: https://github.com/gjoseph92/stackstac/discussions/198#discussion-4760525
    """
    stack = da.copy()
    ds = stack.to_dataset("band")
    for coord, da in ds.band.coords.items():
        if "band" in da.dims:
            for i, band in enumerate(stack.band.values):
                ds[band].attrs[coord] = da.values[i]
    
    ds = ds.drop_dims("band")
    return ds


def stackstac_wrapper(params: dict[str, Any]) -> DataArray:
    """
    Wrapper function for stackstac to avoid a pandas UserWarning if the version of stackstac is <= 0.5.0.
    
    Parameters
    ----------
    params : dict[str, Any]
        Parameters to pass to stackstac.stack.
    
    Returns
    -------
    DataArray
        An xarray DataArray containing the stacked data.
    """
    import stackstac
    if stackstac.__version__ <= "0.5.0":
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            da = stackstac.stack(**params)
    else:
        da = stackstac.stack(**params)
    
    return da


def groupby_solarday(ds: Dataset) -> Dataset:
    """
    Groups the observations of all data variables in a Dataset by calculating the mean for each solar day.
    
    Parameters
    ----------
    ds : Dataset
        The Dataset to group by solar day.
    
    Returns
    -------
    ds_copy : Dataset
        The grouped Dataset.
    
    Notes
    -----
    - This will result in coordinates that include the time-dimension to be dropped. Filter-operations using these
    coordinates should be done before calling this function.
    - The time coordinate will be rounded down to the nearest day.
    """
    ds_copy = ds.copy(deep=True)
    ds_copy.coords['time'] = ds_copy.time.dt.floor('1D')
    ds_copy = ds_copy.groupby('time').mean()
    return ds_copy


def ds_nanquantiles(ds: Dataset,
                    dim: str | tuple[str] = 'time',
                    variables: str | tuple[str] | None = None,
                    quantiles: float | tuple[float] = (0.05, 0.95),
                    compute: bool = False) -> Dataset:
    """
    Aggregate the time dimension of a Dataset by calculating quantiles for each data variable. Returns a new Dataset
    with the quantiles as new variables.
    
    Parameters
    ----------
    ds : Dataset
        The Dataset to calculate quantiles for.
    dim : str or tuple of str, optional
        Dimension(s) to reduce. Default is 'time'.
    variables : str or tuple of str or None, optional
        The data variables to calculate quantiles for. If None (default), all data variables will be used.
    quantiles : float or tuple of float, optional
        The quantiles to calculate. Default is (0.05, 0.95).
    compute : bool, optional
        Whether to compute the new variables into memory. Default is False, which means that the new variables will
        be lazily evaluated.
    
    Returns
    -------
    ds_copy : Dataset
        The new Dataset with the quantiles as new variables.
    """
    ds_copy = ds.copy(deep=True)
    if isinstance(dim, str):
        dim = (dim,)
    if variables is None:
        variables = list(ds_copy.data_vars)
        other_variables = []
    else:
        if isinstance(variables, str):
            variables = [variables]
        else:
            variables = list(variables)
        other_variables = [v for v in list(ds_copy.data_vars) if v not in variables]
    q = quantiles
    quantiles = np.atleast_1d(np.asarray(quantiles, dtype=np.float64))
    
    # Calculate quantiles
    for v in variables:
        ds_copy[f'{v}_quantiles'] = xclim_nanquantile(da=ds_copy[v], q=q, dim=dim)
        for x in quantiles:
            q_str = str(int(x * 100))
            ds_copy[f'{v}_q{q_str}'] = ds_copy[f'{v}_quantiles'].sel(quantile=x)
    
    # Cleanup; we only want the new quantile variables
    ds_copy = ds_copy.drop_vars(variables + other_variables + [f'{v}_quantiles' for v in variables])
    ds_copy = ds_copy.drop_dims(['quantile'] + list(dim))
    
    if compute:
        ds_copy = ds_copy.compute()
    return ds_copy


def xclim_nanquantile(da: DataArray,
                      q: float | tuple[float],
                      dim: str | tuple[str] = 'time') -> DataArray:
    """
    Simple workaround for https://github.com/pydata/xarray/issues/7377
    See notes for more information.
    
    Parameters
    ----------
    da: DataArray
        The DataArray to calculate quantiles for.
    q: float or sequence of float
        Quantiles to compute, which must be between 0 and 1 (inclusive). E.g., [0.1, 0.9]
    dim: str or tuple of str, optional
        Dimension(s) over which to apply this function. Default is 'time'.
    
    Returns
    -------
    result: DataArray
        If `q` is a single quantile, then the result
        is a scalar. If multiple percentiles are given, first axis of
        the result corresponds to the quantile and a quantile dimension
        is added to the return array. The other dimensions are the
        dimensions that remain after the reduction of the array.
    
    Notes
    -----
    The structure of this function is based on:
    https://github.com/pydata/xarray/blob/6c5840e1198707cdcf7dc459f27ea9510eb76388/xarray/core/variable.py#L2128-L2271
    Instead of `numpy.nanquantile`, the following `_nan_quantile` method of the xclim package is implemented:
    https://github.com/Ouranosinc/xclim/blob/0a48bbc137a11d1c295b0f803183df5996f2dd6a/xclim/core/utils.py#L410-L474
    """
    from xarray.core.utils import is_scalar
    from xarray.core.computation import apply_ufunc
    from xclim.core.utils import _nan_quantile
    
    scalar = is_scalar(q)
    q = np.atleast_1d(np.asarray(q, dtype=np.float64))
    
    if is_scalar(dim):
        dim = [dim]
    else:
        dim = list(dim)
    
    def _wrapper(x, **kwargs):
        return np.moveaxis(_nan_quantile(x.copy(), **kwargs), source=0, destination=-1)
    
    result = apply_ufunc(
        _wrapper,
        da,
        input_core_dims=[dim],
        exclude_dims=set(dim),
        output_core_dims=[["quantile"]],
        output_dtypes=[np.float64],
        dask_gufunc_kwargs=dict(output_sizes={"quantile": len(q)}),
        dask="parallelized",
        kwargs={'quantiles': q, 'axis': -1},
    )
    result = result.assign_coords(quantile=DataArray(q, dims=("quantile",)))
    result = result.transpose("quantile", ...)
    
    if scalar:
        result = result.squeeze("quantile")
    
    result.attrs = da.attrs.copy()
    return result
