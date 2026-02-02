import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import BallTree

EARTH_R = 6_371_000.0

OSM_GPKG_PATH = "moscow_features_within_mkad.gpkg"
OSM_GPKG_LAYER = "features"   # change if your layer name differs
OSM_LABELS = ['energy', 'waste', 'industrial_area', 'water', 'green']


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


# -------------------- NEW: OSM closest features --------------------

def load_osm_features_points(gpkg_path=OSM_GPKG_PATH, layer=OSM_GPKG_LAYER) -> pd.DataFrame:
    """
    Read the within-MKAD OSM features GeoPackage and convert each geometry into a point:
    representative_point() is guaranteed to lie on/in the geometry (good for polygons/lines).
    """
    gdf = gpd.read_file(gpkg_path, layer=layer)

    # ensure we have the expected columns
    if "label" not in gdf.columns:
        raise ValueError("OSM gpkg must contain a 'label' column.")
    # geometry column might be called 'coords' in your file; ensure it's set as active geometry
    if "coords" in gdf.columns and gdf.geometry.name != "coords":
        gdf = gdf.set_geometry("coords")

    # work in WGS84 lon/lat
    gdf = gdf.to_crs("EPSG:4326") if gdf.crs else gdf.set_crs("EPSG:4326")

    pts = gdf.geometry.representative_point()

    out = pd.DataFrame({
        "label": gdf["label"].astype(str).to_numpy(),
        "lat": pts.y.to_numpy(dtype=np.float64),
        "lon": pts.x.to_numpy(dtype=np.float64),
    })

    # defensive: drop bad rows
    out = out.replace([np.inf, -np.inf], np.nan).dropna(subset=["lat", "lon", "label"]).reset_index(drop=True)
    return out


def add_closest_osm_features(properties_coords_df, osm_points_df, labels=OSM_LABELS):
    """
    Adds ONLY nearest-distance features for each OSM label:
      - closest_<label>_distance_meters
      - closest_<label>_lat
      - closest_<label>_lng

    No within{0m,100m,500m,1km,5km} counters for OSM features.
    """
    properties_radian = get_radians(properties_coords_df, 'lat', 'lng')

    for lab in labels:
        suffix = f"closest_{lab}"

        sub = osm_points_df[osm_points_df["label"] == lab].copy()
        if sub.empty:
            properties_coords_df[f"{suffix}_distance_meters"] = np.nan
            properties_coords_df[f"{suffix}_lat"] = np.nan
            properties_coords_df[f"{suffix}_lng"] = np.nan
            continue

        objects_radian = get_radians(sub, 'lat', 'lon')
        ball_tree = BallTree(objects_radian, metric='haversine')

        dist_rad, ind = ball_tree.query(properties_radian, k=1, return_distance=True)

        properties_coords_df[f"{suffix}_distance_meters"] = (dist_rad[:, 0] * EARTH_R).astype(np.float64)
        properties_coords_df[f"{suffix}_lat"] = sub["lat"].to_numpy()[ind[:, 0]].astype(np.float64)
        properties_coords_df[f"{suffix}_lng"] = sub["lon"].to_numpy()[ind[:, 0]].astype(np.float64)


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

    # NEW: closest OSM features from within-MKAD gpkg
    osm_points_df = load_osm_features_points(OSM_GPKG_PATH, layer=OSM_GPKG_LAYER)
    add_closest_osm_features(properties_coords_df, osm_points_df, labels=OSM_LABELS)

    properties_coords_df = properties_coords_df.drop_duplicates().reset_index()

    uniq_coords = (properties_coords_df['lng'].astype(str) + '_' + properties_coords_df['lat'].astype(str)).unique().shape[0]
    if properties_coords_df.shape[0] != uniq_coords:
        raise ValueError("something is wrong (coords are not unique)")

    properties_coords_df.to_csv("csv/final_datasets/geo_features.csv", index=False)
