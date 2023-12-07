from copy import deepcopy
from pathlib import Path

from typing import Any
from pystac import Catalog, Collection, Item


def get_catalog_path(product: str) -> str | Path:
    """
    Gets the path to the STAC Catalog file for a given product.

    Parameters
    ----------
    product : str
        Name of the data product.

    Returns
    -------
    str or Path
        The path to the STAC Catalog file of a given product or the path to the product
         directory if product is 'MSWEP'.
    """
    base_path = Path("/geonfs/02_vol3/SaldiDataCube/original_data")
    _dir = base_path.joinpath(product.upper())
    _file = _dir.joinpath("catalog.json")
    if not _dir.exists():
        raise FileNotFoundError(f"Product '{product}': "
                                f"Could not find product directory {_dir}")
    if product == 'mswep':
        return _dir
    if not _file.exists():
        raise FileNotFoundError(f"Product '{product}': "
                                f"Could not find STAC Catalog file {_file}")
    else:
        return str(_file)


def common_params() -> dict[str, Any]:
    """
    Returns parameters common to all products.

    Returns
    -------
    dict
         Dictionary of parameters that are common to all products.
    """
    return {"crs": 'EPSG:4326',
            "resolution": 0.0002,
            "resampling": 'bilinear',
            "chunks": {'time': -1, 'latitude': 'auto', 'longitude': 'auto'}}


def convert_asset_hrefs(list_stac_obj: list[Catalog | Collection | Item],
                        href_type: str
                        ) -> list[Catalog | Collection | Item] | list[None]:
    """
    Converts the asset hrefs of a list of STAC Objects (Catalogs, Collections or Items)
    to either absolute or relative.

    Parameters
    ----------
    list_stac_obj : list of Catalog or Collection or Item
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