import glob
from datetime import datetime
import pytz
from shapely.geometry import box

from pathlib import Path
from typing import Optional, Iterable
from pystac import Catalog, Collection, Item


def filter_stac_catalog(catalog: Catalog,
                        bbox: Optional[tuple[float, float, float, float]] = None,
                        collection_ids: Optional[list[str]] = None,
                        time_range: Optional[tuple[str, str]] = None,
                        time_pattern: Optional[str] = None
                        ) -> tuple[list[Collection], Iterable[Item]]:
    """
    The STAC Catalog is first filtered based on a provided bounding box, returning a
    list of STAC Collections. These Collections are then filtered based on a provided
    time range, returning a list of STAC Items.
    
    Parameters
    ----------
    catalog : Catalog
        The STAC Catalog to filter.
    bbox : tuple of float, optional
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
    collection_ids : list of str, optional
        A list of collection IDs to filter. If not None, this will override the `bbox`
        option.
    time_range : tuple of str, optional
        Time range to load as a tuple of (start_time, stop_time), where start_time and
        stop_time are strings in the format specified by `time_pattern`. Default is
        None, which loads all available data.
    time_pattern : str, optional
        Time pattern to parse the time range. Only needed if it deviates from the
        default: '%Y-%m-%d'.
    
    Returns
    -------
    filtered_collections : list of Collection
        A list of filtered collections.
    filtered_items : list of Item
        A list of filtered items.
    """
    filtered_collections = filter_collections(catalog, bbox, collection_ids)
    filtered_items = filter_items(filtered_collections, time_range, time_pattern)
    return filtered_collections, filtered_items


def filter_collections(catalog: Catalog,
                       bbox: Optional[tuple[float, float, float, float]] = None,
                       collection_ids: Optional[list[str]] = None
                       ) -> list[Collection]:
    """
    Filters the collections in a STAC Catalog based on a bounding box.
    
    Parameters
    ----------
    catalog : Catalog
        The STAC Catalog to filter.
    bbox : tuple of float, optional
        The bounding box of the area of interest in the format (minx, miny, maxx, maxy).
    collection_ids : list of str, optional
        A list of collection IDs to filter. If not None, this will override the `bbox`
        option.
    
    Returns
    -------
    list of Collection
        A list of filtered collections.
    """
    if collection_ids is not None:
        return [collection for collection in catalog.get_children()
                if isinstance(collection, Collection) and
                collection.id in collection_ids]
    elif bbox is not None:
        return [collection for collection in catalog.get_children() if
                isinstance(collection, Collection) and
                collection.extent.spatial.bboxes is not None and
                any(_bbox_intersection(list(bbox), b) is not None
                    for b in collection.extent.spatial.bboxes)]
    else:
        return [collection for collection in catalog.get_children()
                if isinstance(collection, Collection)]


def _bbox_intersection(bbox1: list[float, float, float, float],
                       bbox2: list[float, float, float, float]
                       ) -> list[float, float, float, float] | None:
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
    intersection : list of float or None
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


def filter_items(collections: list[Collection],
                 time_range: Optional[tuple[str, str]] = None,
                 time_pattern: Optional[str] = None
                 ) -> list[Item]:
    """
    Filters the items in a list of collections based on a time range.
    
    Parameters
    ----------
    collections : list of Collection
        The list of collections to filter.
    time_range : tuple of str, optional
        Time range to load as a tuple of (start_time, stop_time), where start_time and
        stop_time are strings in the format specified by `time_pattern`. Default is
        None, which loads all available data.
    time_pattern : str, optional
        Time pattern to parse the time range. Only needed if it deviates from the
        default: '%Y-%m-%d'.
    
    Returns
    -------
    items : list of Item
        A list of filtered items.
    """
    items = []
    for collection in collections:
        for item in collection.get_items():
            if time_range is not None:
                start_date = _timestring_to_utc_datetime(time=time_range[0],
                                                         pattern=time_pattern)
                end_date = _timestring_to_utc_datetime(time=time_range[1],
                                                       pattern=time_pattern)
                if not (start_date <= item.datetime <= end_date):
                    continue
            items.append(item)
    return items


def filter_mswep_nc(directory: Path,
                    time_range: Optional[tuple[str, str]] = None,
                    time_pattern: Optional[str] = None
                    ) -> list[str]:
    """
    Find and filter MSWEP NetCDF files based on a time range.
    
    Parameters
    ----------
    directory : Path
        The directory to search for MSWEP NetCDF files.
    time_range : tuple of str, optional
        Time range to load as a tuple of (start_time, stop_time), where start_time and
        stop_time are strings in the format specified by `time_pattern`. Default is
        None, which loads all available data.
    time_pattern : str, optional
        Time pattern to parse the time range. Only needed if it deviates from the
        default: '%Y-%m-%d'.
    
    Returns
    -------
    files : list of str
        A list of paths to MSWEP NetCDF files.
    """
    files = glob.glob(str(directory.joinpath('*.nc')))
    if time_range is not None:
        start_time = _timestring_to_utc_datetime(time_range[0], time_pattern)
        end_time = _timestring_to_utc_datetime(time_range[1], time_pattern)
        years = [start_time.year + i for i in range(end_time.year -
                                                    start_time.year + 1)]
        files = [f for f in files if any(str(y) in f for y in years)]
    return files


def _timestring_to_utc_datetime(time: str,
                                pattern: Optional[str] = None
                                ) -> datetime:
    """
    Convert time string to UTC datetime object.
    
    Parameters
    ----------
    time : str
        The time string to convert.
    pattern : str
        The format of the time string. Default is '%Y-%m-%d'.
    
    Returns
    -------
    datetime : datetime
        The converted datetime object.
    """
    if pattern is None:
        pattern = '%Y-%m-%d'
    return datetime.strptime(time, pattern).replace(tzinfo=pytz.UTC)
