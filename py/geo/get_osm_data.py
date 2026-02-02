import numpy as np
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

# ---- GREEN FILTER THRESHOLDS ----
GREEN_METRIC_EPSG = 32637              # UTM 37N (meters) for Moscow vicinity
GREEN_MIN_WIDTH_M = 100.0
GREEN_MIN_LENGTH_M = 400.0
GREEN_MIN_AREA_M2 = 0.5 * 1_000_000.0  # 0.5 km^2 = 500,000 m^2


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
    """
    df = df.set_geometry("coords")
    df_wgs = df.to_crs("EPSG:4326") if df.crs else df.set_crs("EPSG:4326")

    mkad = gpd.GeoSeries([mkad_poly_wgs84], crs="EPSG:4326").iloc[0]
    mask = df_wgs.geometry.within(mkad)
    return df_wgs[mask].copy()


# -------------------- NEW: GREEN post-filter --------------------

def _mrr_length_width_m(geom) -> tuple[float, float]:
    """
    Return (length, width) in meters based on the geometry's minimum rotated rectangle.
    Expects geom in a metric CRS (meters).
    """
    try:
        if geom is None or geom.is_empty:
            return 0.0, 0.0
        # Try to fix minor invalidities (cheap)
        if hasattr(geom, "is_valid") and not geom.is_valid:
            geom = geom.buffer(0)
        mrr = geom.minimum_rotated_rectangle  # oriented envelope :contentReference[oaicite:2]{index=2}
        if mrr.geom_type != "Polygon":
            return 0.0, 0.0

        coords = list(mrr.exterior.coords)
        if len(coords) < 5:
            return 0.0, 0.0

        # Rectangle exterior has 5 points (last == first). Use first 4 edges.
        seglens = []
        for i in range(4):
            x1, y1 = coords[i]
            x2, y2 = coords[i + 1]
            seglens.append(float(np.hypot(x2 - x1, y2 - y1)))

        seglens = [l for l in seglens if l > 0]
        if not seglens:
            return 0.0, 0.0

        length = max(seglens)
        width = min(seglens)
        return length, width
    except Exception:
        return 0.0, 0.0


def green_filter(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Keep only green areas with:
      - (Multi)Polygon geometry
      - area >= 0.5 km^2
      - min-rot-rect length >= 400m and width >= 100m
    Computed in a projected CRS (meters). :contentReference[oaicite:3]{index=3}
    """
    if gdf.empty:
        return gdf

    g = gdf[gdf.geometry.notna()].copy()

    # Ensure CRS before projecting
    g = g.set_crs("EPSG:4326") if g.crs is None else g.to_crs("EPSG:4326")
    g_m = g.to_crs(epsg=GREEN_METRIC_EPSG)

    # Polygons only (parks/woods should mostly be polygons; this avoids lines/points)
    geom_type = g_m.geometry.geom_type
    mask_poly = geom_type.isin(["Polygon", "MultiPolygon"])

    # Area filter first (cheap), then MRR dims (more expensive)
    area_m2 = g_m.geometry.area
    mask_area = area_m2 >= GREEN_MIN_AREA_M2

    candidates = g_m[mask_poly & mask_area]
    if candidates.empty:
        return g.iloc[0:0].copy()

    keep_idx = []
    for idx, geom in candidates.geometry.items():
        length_m, width_m = _mrr_length_width_m(geom)
        if (length_m >= GREEN_MIN_LENGTH_M) and (width_m >= GREEN_MIN_WIDTH_M):
            keep_idx.append(idx)

    return g.loc[keep_idx].copy()


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

    # --- GREEN (FILTERED by area + MRR dims)
    tags_green = {
        "leisure": ["park", "garden", "nature_reserve"],
        "landuse": ["forest", "grass", "recreation_ground"],
        "natural": ["wood", "grassland"],
    }
    green = _mk(boundary, "green", tags_green, post_filter=green_filter)

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
