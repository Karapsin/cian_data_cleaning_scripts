import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import BallTree

from pyproj import Transformer
from shapely.ops import nearest_points

EARTH_R = 6_371_000.0

OSM_GPKG_PATH = "moscow_features_within_mkad.gpkg"
OSM_GPKG_LAYER = "features"   # change if your layer name differs
OSM_LABELS = ['energy', 'waste', 'industrial_area', 'water', 'green']

# Metric CRS for Moscow to compute distances in meters
OSM_METRIC_EPSG = 32637  # UTM 37N


def fix_lat_lng(df, lat_col="lat", lng_col="lng"):
    swap = df[lat_col] <= df[lng_col]
    df.loc[swap, [lat_col, lng_col]] = df.loc[swap, [lng_col, lat_col]].to_numpy()


def get_radians(df, lat_col, lng_col):
    return np.deg2rad(df[[lat_col, lng_col]].to_numpy(dtype=np.float64))


def add_distance_to_center(properties_coords_df):
    properties_radian = get_radians(properties_coords_df, 'lat', 'lng')
    lat0, lon0 = np.deg2rad([55.75578, 37.61786])  # Moscow "0 km"

    dlat = properties_radian[:, 0] - lat0
    dlon = properties_radian[:, 1] - lon0
    a = np.sin(dlat/2)**2 + np.cos(lat0) * np.cos(properties_radian[:, 0]) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    properties_coords_df['distance_to_center_meters'] = (EARTH_R * c).astype(np.float64)


def get_objects_count_within_thresholds(properties_coords_df, properties_radian, objects_ball_tree, suffix):
    thresholds_dict = {0: "0m", 100: "100m", 500: "500m", 1000: "1km", 5000: "5km"}

    thresholds_m = np.array(list(thresholds_dict.keys()), dtype=np.float64)
    thresholds_rad = thresholds_m / EARTH_R

    _, dists = objects_ball_tree.query_radius(
        properties_radian,
        r=thresholds_rad[-1],
        return_distance=True,
        sort_results=True
    )

    counts = np.zeros((len(properties_radian), len(thresholds_dict)), dtype=int)
    for i, di in enumerate(dists):
        counts[i] = np.searchsorted(di, thresholds_rad, side='right')

    for i, key_value in enumerate(thresholds_dict.items()):
        properties_coords_df[f'{suffix}_within{key_value[1]}'] = counts[:, i]


def get_closest_station_objects(properties_coords_df, objects_coords_df, suffix):
    properties_radian = get_radians(properties_coords_df, 'lat', 'lng')
    objects_radian = get_radians(objects_coords_df, 'lat', 'lon')

    ball_tree = BallTree(objects_radian, metric='haversine')

    dist_rad, ind = ball_tree.query(properties_radian, k=1, return_distance=True)

    def extract_neighbor(col_name):
        return objects_coords_df[col_name].to_numpy()[ind[:, 0]]

    properties_coords_df[f'nearest_{suffix}'] = extract_neighbor('station_name')
    properties_coords_df[f'nearest_{suffix}_line'] = extract_neighbor('line')
    properties_coords_df[f'nearest_{suffix}_lat'] = extract_neighbor('lat')
    properties_coords_df[f'nearest_{suffix}_lng'] = extract_neighbor('lon')
    properties_coords_df[f'nearest_{suffix}_distance_meters'] = (dist_rad[:, 0] * EARTH_R).astype(np.float64)

    get_objects_count_within_thresholds(properties_coords_df, properties_radian, ball_tree, suffix)


def get_closest_ads_count(properties_coords_df, ads_coords_df):
    properties_radian = get_radians(properties_coords_df, 'lat', 'lng')

    deal_types = ads_coords_df['ad_deal_type'].unique().tolist()
    for single_deal_type in deal_types:
        filtered_ads_coords_df = ads_coords_df.query("ad_deal_type == @single_deal_type").copy()
        objects_radian = get_radians(filtered_ads_coords_df, 'lat', 'lng')
        ball_tree = BallTree(objects_radian, metric='haversine')

        get_objects_count_within_thresholds(properties_coords_df, properties_radian, ball_tree, single_deal_type)


# -------------------- FIXED: OSM closest features (distance to EDGE) --------------------

def load_osm_features_edges_gdf(
    gpkg_path=OSM_GPKG_PATH,
    layer=OSM_GPKG_LAYER,
    metric_epsg: int = OSM_METRIC_EPSG,
) -> gpd.GeoDataFrame:
    """
    Read OSM features and build an "edge geometry" GeoDataFrame in METERS:
      - Polygons/MultiPolygons -> boundary (edge)
      - Lines/MultiLines -> unchanged (edge is the line itself)
      - Points -> unchanged
    Returns GeoDataFrame with columns: ['label', 'edge'] in EPSG:metric_epsg
    """
    gdf = gpd.read_file(gpkg_path, layer=layer)

    if "label" not in gdf.columns:
        raise ValueError("OSM gpkg must contain a 'label' column.")
    if "coords" in gdf.columns and gdf.geometry.name != "coords":
        gdf = gdf.set_geometry("coords")

    # work in WGS84 then project to meters
    gdf = gdf.to_crs("EPSG:4326") if gdf.crs else gdf.set_crs("EPSG:4326")
    gdf_m = gdf.to_crs(epsg=metric_epsg)

    geom = gdf_m.geometry
    is_poly = geom.geom_type.isin(["Polygon", "MultiPolygon"])

    edge = geom.copy()
    # Important: boundary of polygon = its edges (LineString/MultiLineString).
    # boundary of LineString = endpoints only -> DO NOT apply to lines.
    edge[is_poly] = geom[is_poly].boundary

    out = gpd.GeoDataFrame(
        {"label": gdf_m["label"].astype(str).to_numpy()},
        geometry=edge,
        crs=f"EPSG:{metric_epsg}",
    ).rename_geometry("edge")

    out = out[out.geometry.notna() & ~out.geometry.is_empty].reset_index(drop=True)
    return out


def _extract_endpoints_wgs_from_shortest_lines(lines_m: gpd.GeoSeries, metric_epsg: int) -> tuple[np.ndarray, np.ndarray]:
    """
    Given shortest lines (point->edge) in meters, return endpoint (on edge) as lat/lng arrays in WGS84.
    """
    to_wgs = Transformer.from_crs(f"EPSG:{metric_epsg}", "EPSG:4326", always_xy=True)

    lat = np.full(len(lines_m), np.nan, dtype=np.float64)
    lng = np.full(len(lines_m), np.nan, dtype=np.float64)

    for i, ln in enumerate(lines_m):
        if ln is None or ln.is_empty:
            continue
        coords = list(ln.coords)
        if len(coords) < 2:
            continue
        x, y = coords[-1]  # line ends at nearest point on "other" geometry
        lon, la = to_wgs.transform(x, y)
        lat[i] = la
        lng[i] = lon

    return lat, lng


def add_closest_osm_features(
    properties_coords_df: pd.DataFrame,
    osm_edges_gdf: gpd.GeoDataFrame,
    labels=OSM_LABELS,
    metric_epsg: int = OSM_METRIC_EPSG,
    max_distance_m: float | None = None,  # set e.g. 50000 to limit search radius
):
    """
    Adds ONLY nearest-edge features for each OSM label:
      - closest_<label>_distance_meters  (distance to closest POINT ON EDGE)
      - closest_<label>_lat
      - closest_<label>_lng

    No within{...} counters for OSM features.
    """
    # property points in meters (same order as properties_coords_df)
    props_geom_wgs = gpd.GeoSeries(
        gpd.points_from_xy(properties_coords_df["lng"], properties_coords_df["lat"], crs="EPSG:4326")
    )
    props_geom_m = props_geom_wgs.to_crs(epsg=metric_epsg)

    n = len(properties_coords_df)

    for lab in labels:
        suffix = f"closest_{lab}"

        edges_sub = osm_edges_gdf[osm_edges_gdf["label"] == lab]
        if edges_sub.empty:
            properties_coords_df[f"{suffix}_distance_meters"] = np.nan
            properties_coords_df[f"{suffix}_lat"] = np.nan
            properties_coords_df[f"{suffix}_lng"] = np.nan
            continue

        # nearest geometry in the tree for each input point
        idx, dist = edges_sub.sindex.nearest(
            props_geom_m,
            return_all=False,
            return_distance=True,
            max_distance=max_distance_m,
        )

        out_dist = np.full(n, np.nan, dtype=np.float64)
        out_lat = np.full(n, np.nan, dtype=np.float64)
        out_lng = np.full(n, np.nan, dtype=np.float64)

        left_ix = np.asarray(idx[0], dtype=int)
        right_pos = np.asarray(idx[1], dtype=int)

        # distances are in meters (projected CRS)
        out_dist[left_ix] = np.asarray(dist, dtype=np.float64)

        # get nearest point on edge for matched pairs
        pts_sel = gpd.GeoSeries(props_geom_m.iloc[left_ix].to_numpy(), crs=f"EPSG:{metric_epsg}")
        edges_sel = gpd.GeoSeries(edges_sub.geometry.iloc[right_pos].to_numpy(), crs=f"EPSG:{metric_epsg}")

        if hasattr(gpd.GeoSeries, "shortest_line"):
            # Fast path: shortest line between point and edge; endpoint is nearest point on edge
            lines = pts_sel.shortest_line(edges_sel)
            lat_sel, lng_sel = _extract_endpoints_wgs_from_shortest_lines(lines, metric_epsg)
            out_lat[left_ix] = lat_sel
            out_lng[left_ix] = lng_sel
        else:
            # Fallback: per-row nearest_points (slower, but works everywhere)
            to_wgs = Transformer.from_crs(f"EPSG:{metric_epsg}", "EPSG:4326", always_xy=True)
            for k in range(len(left_ix)):
                li = left_ix[k]
                p = pts_sel.iloc[k]
                e = edges_sel.iloc[k]
                if p is None or p.is_empty or e is None or e.is_empty:
                    continue
                _, p_edge = nearest_points(p, e)
                lon, la = to_wgs.transform(p_edge.x, p_edge.y)
                out_lat[li] = la
                out_lng[li] = lon

        properties_coords_df[f"{suffix}_distance_meters"] = out_dist
        properties_coords_df[f"{suffix}_lat"] = out_lat
        properties_coords_df[f"{suffix}_lng"] = out_lng


# -------------------- MAIN --------------------
def get_geo_features_df():
    properties_coords_df = pd.read_csv("csv/prepared_data/offers_parsed/all_deal_types_cleaned.csv")[['lng', 'lat']].drop_duplicates()
    ads_coords_df = pd.read_csv("csv/prepared_data/offers_parsed/all_deal_types_cleaned.csv")[['ad_deal_type', 'property_id', 'lng', 'lat']].drop_duplicates()
    stations_df = pd.read_excel("xlsx/geo/processed/stations.xlsx")

    fix_lat_lng(properties_coords_df, "lat", "lng")
    fix_lat_lng(ads_coords_df, "lat", "lng")
    fix_lat_lng(stations_df, "lat", "lon")

    add_distance_to_center(properties_coords_df)
    get_closest_station_objects(properties_coords_df, stations_df.query("station_type == 'subway'"), suffix='subway')
    get_closest_station_objects(properties_coords_df, stations_df.query("station_type == 'mcd'"), suffix='mcd')
    get_closest_ads_count(properties_coords_df, ads_coords_df)

    # FIXED: closest OSM features (distance to nearest edge, not to center)
    osm_edges_gdf = load_osm_features_edges_gdf(OSM_GPKG_PATH, layer=OSM_GPKG_LAYER, metric_epsg=OSM_METRIC_EPSG)
    add_closest_osm_features(properties_coords_df, osm_edges_gdf, labels=OSM_LABELS, metric_epsg=OSM_METRIC_EPSG)

    properties_coords_df = properties_coords_df.drop_duplicates().reset_index()

    uniq_coords = (properties_coords_df['lng'].astype(str) + '_' + properties_coords_df['lat'].astype(str)).unique().shape[0]
    if properties_coords_df.shape[0] != uniq_coords:
        raise ValueError("something is wrong (coords are not unique)")

    properties_coords_df.to_csv("csv/final_datasets/geo_features.csv", index=False)
