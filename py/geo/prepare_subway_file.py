import pandas as pd
import re


def parse_params(s: str):
    parts = re.split(r'(?:\\n|\n)', s)
    kv = dict(p.split(':', 1) for p in parts if ':' in p)
    return pd.Series({
        'station_name': kv.get('StationName', '').strip(),
        'diameter_name': kv.get('DiameterName', '').strip(),
    })

# subway stations
subway = pd.read_excel("xlsx/geo/raw/subway_coords_raw.xlsx")[['station_name', 'line', 'lon', 'lat']]
subway['station_type'] = 'subway'

# mcd stations
mcd = pd.read_excel("xlsx/geo/raw/mcd_coords_raw.xlsx")[['station_params', 'lon', 'lat']]
mcd[['station_name', 'line']] = mcd['station_params'].apply(parse_params)
mcd = mcd[['station_name', 'line', 'lon', 'lat']]
mcd['station_type'] = 'mcd'

# concat and save
pd.concat([subway, mcd]).to_excel("xlsx/geo/processed/stations.xlsx")
