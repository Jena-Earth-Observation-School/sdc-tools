from copy import deepcopy
from pathlib import Path

from typing import List
from pystac import Catalog, Collection, Item


def get_catalog_path(product):
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
    return base_path.joinpath(product.upper(), "catalog.json")


def common_params():
    """
    Returns parameters common to all products.
    
    Returns
    -------
    dict
         Dictionary of parameters that are common to all products.
    """
    from rasterio.enums import Resampling
    return {"epsg": 4326,
            "resolution": 0.0002,
            "dtype": "float32",
            "resampling": Resampling['bilinear'],
            "xy_coords": 'center'}


def convert_asset_hrefs(list_stac_obj: List[Catalog or Collection or Item],
                        href_type: str) -> List[Catalog or Collection or Item]:
    """
    Converts the asset hrefs of a list of STAC Objects to either absolute or relative.
    
    Parameters
    ----------
    list_stac_obj : List[Catalog or Collection or Item]
        List of Catalogs, Collections or Items.
    href_type : str
        Type of href to convert to. Can be either 'absolute' or 'relative'.
    
    Returns
    -------
    List[Catalog or Collection or Item]
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


def overwrite_default_dask_chunk_size():
    """
    Overwrites the default dask chunk size to 256 MiB.
    
    Returns
    -------
    None
    """
    import dask
    dask.config.set({'array.chunk-size': '256MiB'})
