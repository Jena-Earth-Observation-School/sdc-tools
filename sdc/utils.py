import warnings
from copy import deepcopy
from pathlib import Path

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
    Converts an xarray.Dataarray loaded using the stackstac library to an xarray.Dataset
    
    Parameters
    ----------
    da : Dataarray
        Dataarray loaded using the stackstac library.
    
    Returns
    -------
    Dataset
        Dataset with band dimension dropped and band coordinates saved as attrs in
        variables.
    
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
