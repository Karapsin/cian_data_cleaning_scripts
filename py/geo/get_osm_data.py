# pip install -U osmnx geopandas shapely pyproj requests

import pandas as pd
import geopandas as gpd
import osmnx as ox
import requests

from shapely.geometry import LineString
from shapely.ops import unary_union, polygonize

PLACE = "Moscow, Russia"

# MKAD in OSM (Moscow Ring Road)
MKAD_RELATION_ID = 2094222

# Be nice to public servers
ox.settings.log_console = True
ox.settings.use_cache = True
ox.settings.overpass_rate_limit = True
ox.settings.requests_timeout = 180
# Optional (recommended) - identify your app
# ox.settings.http_user_agent = "my-osm-moscow-features/1.0 (contact: you@example.com)"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"  # you can swap if needed


def _fetch(boundary, tags) -> gpd.GeoDataFrame:
    """
    OSMnx v2+: ox.features.features_from_polygon
    Fallback for older OSMnx: ox.geometries.geometries_from_polygon
    """
    if hasattr(ox, "features") and hasattr(ox.features, "features_from_polygon"):
        return ox.features.features_from_polygon(boundary, tags=tags)
    return ox.geometries.geometries_from_polygon(boundary, tags=tags)


def _mk(boundary, label: str, tags: dict, post_filter=None) -> gpd.GeoDataFrame:
    """
    Fetch features, optionally post-filter, return 2-col GeoDataFrame:
    label + coords (geometry).
    """
    gdf = _fetch(boundary, tags)
    if gdf.empty:
        return gpd.GeoDataFrame({"label": [], "coords": []}, geometry="coords", crs="EPSG:4326")

    gdf = gdf[gdf.geometry.notna()].copy()

    if post_filter is not None:
        gdf = post_filter(gdf)
        if gdf.empty:
            return gpd.GeoDataFrame({"label": [], "coords": []}, geometry="coords", crs="EPSG:4326")

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    out = gpd.GeoDataFrame(
        {"label": label, "coords": gdf.geometry},
        geometry="coords",
        crs="EPSG:4326",
    )

    # Deduplicate within this label by OSM element identity if present
    if isinstance(gdf.index, pd.MultiIndex) and set(["osmid"]).issubset(gdf.reset_index().columns):
        tmp = gdf.reset_index()[["element_type", "osmid"]].copy()
        out = out.reset_index(drop=True)
        out["_element_type"] = tmp["element_type"].values
        out["_osmid"] = tmp["osmid"].values
        out = out.drop_duplicates(subset=["label", "_element_type", "_osmid"]).drop(
            columns=["_element_type", "_osmid"]
        )
    else:
        out = out.drop_duplicates(subset=["label", "coords"])

    return out


def get_mkad_polygon_wgs84(
    relation_id: int = MKAD_RELATION_ID,
    overpass_url: str = OVERPASS_URL,
    timeout_s: int = 180,
    metric_epsg: int = 32637,  # UTM 37N fits Moscow well
):
    """
    Download MKAD relation member ways with geometry from Overpass, polygonize to get interior polygon.
    Returns a shapely Polygon/MultiPolygon in EPSG:4326.
    """
    query = f"""
    [out:json][timeout:{timeout_s}];
    relation({relation_id});
    way(r);
    out geom;
    """

    r = requests.post(overpass_url, data={"data": query}, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()

    ways = [el for el in data.get("elements", []) if el.get("type") == "way" and "geometry" in el]
    if not ways:
        raise RuntimeError("MKAD ways not found via Overpass (no 'way' elements with geometry).")

    lines = []
    for w in ways:
        coords = w.get("geometry", [])
        if len(coords) < 2:
            continue
        line = LineString([(p["lon"], p["lat"]) for p in coords])
        if not line.is_empty:
            lines.append(line)

    if not lines:
        raise RuntimeError("MKAD linework is empty after parsing ways.")

    merged = unary_union(lines)
    polys = list(polygonize(merged))
    if not polys:
        raise RuntimeError("Failed to polygonize MKAD linework into a polygon. (Linework may not be a closed ring.)")

    # Choose the largest polygon by area in a metric CRS
    polys_gs = gpd.GeoSeries(polys, crs="EPSG:4326")
    areas = polys_gs.to_crs(epsg=metric_epsg).area.values
    mkad_poly = polys[int(areas.argmax())]
    return mkad_poly


def filter_within_mkad(df: gpd.GeoDataFrame, mkad_poly_wgs84) -> gpd.GeoDataFrame:
    """
    Keep only features strictly within the MKAD polygon.
    Note: geometries that cross MKAD will be dropped; only fully-inside geometries remain.

    If you want to also keep geometries exactly on the boundary line, use:
        mask = within | touches
    """
    df = df.set_geometry("coords")
    df_wgs = df.to_crs("EPSG:4326") if df.crs else df.set_crs("EPSG:4326")

    mkad = gpd.GeoSeries([mkad_poly_wgs84], crs="EPSG:4326").iloc[0]

    mask = df_wgs.geometry.within(mkad)
    # mask = df_wgs.geometry.within(mkad) | df_wgs.geometry.touches(mkad)  # optional boundary-inclusive

    return df_wgs[mask].copy()


def build_moscow_labeled_df(place: str = PLACE) -> gpd.GeoDataFrame:
    boundary_gdf = ox.geocode_to_gdf(place).to_crs("EPSG:4326")
    boundary = boundary_gdf.geometry.iloc[0]

    # --- WATER
    tags_water_union = {
        "waterway": ["river", "stream", "canal"],
        "natural": "water",
        "water": "river",
    }

    def water_filter(gdf):
        w = gdf.get("waterway", pd.Series(pd.NA, index=gdf.index))
        n = gdf.get("natural", pd.Series(pd.NA, index=gdf.index))
        wa = gdf.get("water", pd.Series(pd.NA, index=gdf.index))
        return gdf[w.isin(["river", "stream", "canal"]) | ((n == "water") & (wa == "river"))].copy()

    water = _mk(boundary, "water", tags_water_union, post_filter=water_filter)

    # --- INDUSTRIAL
    tags_industrial = {
        "landuse": "industrial",
        "building": "industrial",
        "man_made": "works",
        "industrial": True,
    }
    industrial = _mk(boundary, "industrial_area", tags_industrial)

    # --- ENERGY
    tags_energy = {"power": ["plant", "generator"]}
    energy = _mk(boundary, "energy", tags_energy)

    # --- WASTE
    tags_waste = {
        "man_made": ["wastewater_plant", "composting_plant"],
        "amenity": ["waste_transfer_station", "recycling"],
        "landuse": "landfill",
    }
    waste = _mk(boundary, "waste", tags_waste)

    # --- GREEN
    tags_green = {
        "leisure": ["park", "garden", "nature_reserve"],
        "landuse": ["forest", "grass", "recreation_ground"],
        "natural": ["wood", "grassland"],
    }
    green = _mk(boundary, "green", tags_green)

    combined = pd.concat([water, industrial, energy, waste, green], ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="coords", crs="EPSG:4326")

    # Optional de-dupe by priority (geom-only)
    priority = {"energy": 0, "waste": 1, "industrial_area": 2, "water": 3, "green": 4}
    combined["_prio"] = combined["label"].map(priority).fillna(999).astype(int)
    combined = (
        combined.sort_values("_prio")
        .drop_duplicates(subset=["coords"], keep="first")
        .drop(columns=["_prio"])
        .reset_index(drop=True)
    )

    # --- FILTER: ONLY within MKAD
    mkad_poly = get_mkad_polygon_wgs84()
    combined = filter_within_mkad(combined, mkad_poly)

    return combined


# ---- RUN + MAP ----
df = build_moscow_labeled_df()

m = df.explore(
    column="label",
    categorical=True,
    legend=True,
    tooltip=["label"],
)

m.save("moscow_features_within_mkad.html")
print("Saved: moscow_features_within_mkad.html")

df.to_file("moscow_features_within_mkad.gpkg", layer="features", driver="GPKG")
print("Saved: df to moscow_features_within_mkad.gpkg")