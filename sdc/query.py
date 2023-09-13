from datetime import datetime
import pytz
from shapely.geometry import box

from typing import List, Optional, Tuple
from pystac import Catalog, Collection, Item


def filter_stac_catalog(catalog: Catalog, 
                        bbox: Optional[Tuple[float, float, float, float]] = None, 
                        time_range: Optional[Tuple[str, str]] = None, 
                        time_pattern: str = '%Y-%m-%d') -> Tuple[List[Collection], List[Item]]:
    """
    The STAC Catalog is first filtered based on a provided bounding box, returning a list of STAC Collections. These 
    Collections are then filtered based on a provided time range, returning a list of STAC Items.
    
    Parameters
    ----------
    catalog : Catalog
        The STAC Catalog to filter.
    bbox : tuple of float, optional
        The bounding box in the format (minx, miny, maxx, maxy).
    time_range : tuple of str, optional
        The time range in the format (start_time, end_time).
    time_pattern : str, optional
        The pattern used to parse the time strings.
    
    Returns
    -------
    Tuple[List[Collection], List[Item]]
        A tuple containing the filtered STAC Collections and Items.
    """
    filtered_collections = filter_collections(catalog, bbox)
    filtered_items = filter_items(filtered_collections, time_range, time_pattern)
    return filtered_collections, filtered_items


def filter_collections(catalog: Catalog, 
                       bbox: Optional[Tuple[float, float, float, float]] = None) -> List[Collection]:
    """
    Filters the collections in a STAC Catalog based on a bounding box.
    
    Parameters
    ----------
    catalog : Catalog
        The STAC Catalog to filter.
    bbox : tuple of float, optional
        The bounding box in the format (minx, miny, maxx, maxy).
    
    Returns
    -------
    List[Collection]
        A list of filtered collections.
    """
    if bbox is None:
        return [collection for collection in catalog.get_children() if isinstance(collection, Collection)]
    return [
        collection for collection in catalog.get_children()
        if isinstance(collection, Collection) and
        collection.extent.spatial.bboxes is not None and
        any(_bbox_intersection(list(bbox), b) is not None for b in collection.extent.spatial.bboxes)
    ]


def _bbox_intersection(bbox1: List[float], 
                       bbox2: List[float]) -> Optional[List[float]]:
    """
    Computes the intersection of two bounding boxes.

    Parameters
    ----------
    bbox1 : list of float
        The first bounding box in the format [west, south, east, north].
    bbox2 : list of float
        The second bounding box in the format [west, south, east, north].

    Returns
    -------
    intersection : list or None
        The intersection of the two bounding boxes as a list of float.
        Returns None if there is no intersection.
    """
    box1 = box(*bbox1)
    box2 = box(*bbox2)
    intersection = box1.intersection(box2)
    if intersection.is_empty:
        return None
    else:
        return list(intersection.bounds)


def filter_items(collections: List[Collection], 
                 time_range: Optional[Tuple[str, str]] = None, 
                 time_pattern: str = '%Y-%m-%d') -> List[Item]:
    """
    Filters the items in a list of collections based on a time range.
    
    Parameters
    ----------
    collections : List[Collection]
        The list of collections to filter.
    time_range : tuple of str, optional
        The time range in the format (start_time, end_time).
    time_pattern : str, optional
        The pattern used to parse the time strings.
    
    Returns
    -------
    List[Item]
        A list of filtered items.
    """
    items = []
    for collection in collections:
        for item in collection.get_items():
            if time_range is not None:
                start_date = _timestring_to_utc_datetime(time=time_range[0], pattern=time_pattern)
                end_date = _timestring_to_utc_datetime(time=time_range[1], pattern=time_pattern)
                if not (start_date <= item.datetime <= end_date):
                    continue
            items.append(item)
    return items


def _timestring_to_utc_datetime(time: str, 
                                pattern: str) -> datetime:
    """
    Convert time string to UTC datetime object.

    Parameters
    ----------
    time : str
        The time string to convert.
    pattern : str
        The format of the time string.

    Returns
    -------
    datetime : datetime.datetime
        The converted datetime object.
    """
    return datetime.strptime(time, pattern).replace(tzinfo=pytz.UTC)
