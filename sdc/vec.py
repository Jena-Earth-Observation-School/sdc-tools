import geopandas as gpd

SITE01 = '''{
  "type": "FeatureCollection",
  "name": "saldi_01",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                19.45,
                -33.75
              ],
              [
                20.9,
                -33.75
              ],
              [
                20.9,
                -34.85
              ],
              [
                19.45,
                -34.85
              ],
              [
                19.45,
                -33.75
              ]
            ]
          ]
        ]
      }
    }
  ]
}'''

SITE02 = '''{
  "type": "FeatureCollection",
  "name": "saldi_02",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                19.5,
                -28.25
              ],
              [
                20.750000000000114,
                -28.25
              ],
              [
                20.750000000000114,
                -29.25
              ],
              [
                19.5,
                -29.25
              ],
              [
                19.5,
                -28.25
              ]
            ]
          ]
        ]
      }
    }
  ]
}'''

SITE03 = '''{
  "type": "FeatureCollection",
  "name": "saldi_03",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                23.991120751603482,
                -28.5
              ],
              [
                24.991120751603482,
                -28.5
              ],
              [
                24.991120751603482,
                -29.5
              ],
              [
                23.991120751603482,
                -29.5
              ],
              [
                23.991120751603482,
                -28.5
              ]
            ]
          ]
        ]
      }
    }
  ]
}'''

SITE04 = '''{
  "type": "FeatureCollection",
  "name": "saldi_04",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                26.45,
                -29
              ],
              [
                27.5,
                -29
              ],
              [
                27.5,
                -30
              ],
              [
                26.45,
                -30
              ],
              [
                26.45,
                -29
              ]
            ]
          ]
        ]
      }
    }
  ]
}'''

SITE05 = '''{
  "type": "FeatureCollection",
  "name": "saldi_05",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                25.75,
                -25
              ],
              [
                27.000000000000114,
                -25
              ],
              [
                27.000000000000114,
                -26
              ],
              [
                25.75,
                -26
              ],
              [
                25.75,
                -25
              ]
            ]
          ]
        ]
      }
    }
  ]
}'''

SITE06 = '''{
  "type": "FeatureCollection",
  "name": "saldi_06",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
    }
  },
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [
          [
            [
              [
                30.750000000000114,
                -24.9
              ],
              [
                32.05,
                -24.9
              ],
              [
                32.05,
                -26
              ],
              [
                30.750000000000114,
                -26
              ],
              [
                30.750000000000114,
                -24.9
              ]
            ]
          ]
        ]
      }
    }
  ]
}'''


def get_site_bounds(site: str) -> tuple[float, float, float, float]:
    """
    Get the bounds as a tuple of (min_x, min_y, max_x, max_y) of a SALDi site.
    
    Parameters
    ----------
    site : str
        The SALDi site name in the format 'siteXX', where XX is the site number.
    
    Returns
    -------
    bounds : tuple of float
        The bounds as a tuple of (min_x, min_y, max_x, max_y).    
    """
    driver = "GeoJSON"
    if site.lower() == 'site01':
        gdf = gpd.read_file(SITE01, driver=driver)
    elif site.lower() == 'site02':
        gdf = gpd.read_file(SITE02, driver=driver)
    elif site.lower() == 'site03':
        gdf = gpd.read_file(SITE03, driver=driver)
    elif site.lower() == 'site04':
        gdf = gpd.read_file(SITE04, driver=driver)
    elif site.lower() == 'site05':
        gdf = gpd.read_file(SITE05, driver=driver)
    elif site.lower() == 'site06':
        gdf = gpd.read_file(SITE06, driver=driver)
    else:
        raise ValueError(f'Site {site} not supported')
    
    bounds = (gdf.bounds.minx.values[0],
              gdf.bounds.miny.values[0],
              gdf.bounds.maxx.values[0],
              gdf.bounds.maxy.values[0])
    
    return bounds
