import pandas as pd
import numpy as np
from tqdm import tqdm
from functools import reduce 
from hashlib import sha256
import re

from py.utils.data_cleaning.cols_order import cols_order
from py.utils.data_cleaning.clean_price_history import clean_price_history
from py.utils.db_related.db_utils import query_table
from py.utils.db_related.cmd_utils import start_db, stop_db
from py.utils.general.dttm import time_print
from py.utils.geo.coords_features_gen import fix_lat_lng

KEY_COLUMNS = ['lat', 'lng', 'floorNumber', 'roomsCount', 'ad_deal_type']

def concat_series(series1, series2):
    return series1.astype(str) + '_' + series2.astype(str)

def hash_str(input_str):
    return sha256(input_str.encode()).hexdigest()

def hash_cols(df, cols_list):
    
    series_list = [df[col] for col in cols_list]
    glued_cols = reduce(concat_series, series_list)    

    return glued_cols.apply(hash_str)

def get_property_id(df):

    df['lat'] = df['lat'].astype(float)
    df['lng'] = df['lng'].astype(float)
    df['floorNumber'] = df['floorNumber'].fillna(-1).astype(int)
    df['roomsCount'] = df['roomsCount'].fillna(-1).astype(int)
    df['ad_deal_type'] = df['ad_deal_type'].astype(str)

    df['property_id'] = hash_cols(df, KEY_COLUMNS)

    return df

def parse_cian_range(input_str):

    if not(isinstance(input_str, str)):
        return 

    match = re.match(r"(\d+),(\d+)—(\d+),(\d+)\xa0(\w+)\xa0(.+)", input_str)
    if match:
        cian_left_bound = float(f"{match.group(1)}.{match.group(2)}")
        cian_right_bound = float(f"{match.group(3)}.{match.group(4)}")
        multiplier_str = match.group(5)
        currency = match.group(6)

    if multiplier_str == 'млн':
        multiplier = 1_000_000
    else:
        raise ValueError(f"Unknown multipler {multiplier_str}")

    if currency != '₽':
        raise ValueError(f"Unknown currency {currency}")

    return (cian_left_bound * multiplier, cian_right_bound * multiplier)

def parse_rent_time(input_str):
    if not(isinstance(input_str, str)):
        return
    
    if input_str == 'от года':
        return '1 year and more'
    elif input_str == 'несколько месяцев':
        return 'less than 1 year'
    else:
        raise ValueError(f"Unknown rent time {input_str}") 

def parse_kids_and_animals(input_str):
    if not(isinstance(input_str, str)):
        return
    
    if input_str == 'можно с детьми':
        return 'kids'
    elif input_str == 'можно с животными':
        return 'animals'
    elif input_str == 'можно с детьми и животными':
        return 'kids and animals'
    else:
        raise ValueError(f"Unknown rent time {input_str}") 

def determine_apartment_status(row):
    if row.str.lower().str.contains('апартамент', na=False).any():
        return True
    elif row.str.lower().str.contains('квартир', na=False).any():
        return False
    else:
        return np.nan


def sum_nums_in_string(s):
    if pd.isnull(s):
        return 0
    numbers = s.replace(',', ' ').split()
    return sum(int(word) for word in numbers if word.isdigit())


def correct_prices(df, batch_size=20000):
    
    price_last = np.empty(len(df), dtype=np.float64)
    price_first = np.empty(len(df), dtype=np.float64)
    for start in tqdm(range(0, len(df), batch_size), desc="updating priceTotal", unit="rows"):
        end = min(start + batch_size, len(df))
        batch = df.iloc[start:end]

        results_max = (
            batch['price_history']
            .apply(eval) 
            .apply(lambda tuples: max(tuples, 
                                      key=lambda x: pd.to_datetime(x[0], utc=True)
                                  )[1]
             )
        )

        results_min = (
            batch['price_history']
            .apply(eval) 
            .apply(lambda tuples: min(tuples, 
                                      key=lambda x: pd.to_datetime(x[0], utc=True)
                                  )[1]
             )
        )

        price_last[start:end] = results_max.to_numpy(dtype=np.float64)
        price_first[start:end] = results_min.to_numpy(dtype=np.float64)

    df['price_last'] = price_last
    df['price_first'] = price_first
    
    return df

############################################################################################3
# main function

def clean_dataset(deal_type):

    start_db()
    time_print("reading offers_parsed df from mongodb")
    df = query_table("offers_parsed", query_dict={"ad_deal_type": deal_type})
    extracted_deal_types = df["ad_deal_type"].unique().tolist()
    time_print(f"loaded deal typed: {extracted_deal_types}")

    time_print("reading search_clean df from mongodb")
    search_clean = query_table("search_clean", query_dict={"ad_deal_type": deal_type})
    stop_db()

    search_clean['last_seen_dttm'] = search_clean['last_seen_dttm'].apply(lambda x: x[1] if isinstance(x, list) else x)

    time_print("refreshing some columns in search_clean df")
    temp_df = df.copy()
    temp_df['filter_col'] = pd.to_datetime(temp_df['offer_page_load_dttm'])
    temp_df['filter_value'] = temp_df.groupby('url')['filter_col'].transform('max')
    temp_df = temp_df[temp_df['filter_col'] == temp_df['filter_value']].copy()

    search_clean = (
        search_clean
        .drop(KEY_COLUMNS, axis = 1)
        .merge(temp_df[['url'] + KEY_COLUMNS], how="left", on='url')
    )

    del temp_df

    time_print("saving refreshed search_clean")
    search_clean = get_property_id(search_clean)
    search_clean.to_csv(f"csv/prepared_data/search_clean/{deal_type}.csv", index = False)

    del search_clean

    time_print("turning creationDate to dttm")
    df['creationDate'] = pd.to_datetime(df['creationDate'], format = 'ISO8601')

    time_print("property_id gen")
    df = get_property_id(df)

    time_print("parsing lifts cols")
    df['passengerLiftsCount'] = abs(df['passengerLiftsCount'])
    df['cargoLiftsCount'] = abs(df['cargoLiftsCount'])

    time_print("offers_id aggregation")
    df["offer_id"] = df["url"].str.split("/").apply(lambda x: x[-2]).astype(int)
    unique_offer_ids = df.groupby('property_id')['offer_id'].apply(lambda x: set(x.unique())).reset_index()
    df = df.drop(['offer_id'], axis = 1).merge(unique_offer_ids, on='property_id', suffixes=('', '_set'))

    if not(bool(df['cian_price_range'].isna().all())):
        time_print("parsing cian price range")
        df[['cian_range_left_bound', 'cian_range_right_bound']] = df['cian_price_range'].apply(parse_cian_range).apply(pd.Series)
    else:
        df[['cian_range_left_bound', 'cian_range_right_bound']] = None


    time_print("turning some cols to bool + filling NAs")
    df['is_individual_project'] = df['seriesName'].apply(lambda x: x == 'Индивидуальный проект')
    df['has_videos'] = df['videos'].fillna("[]").apply(lambda x: True if x != "[]" else False).value_counts()
    df['isPenthouse'] = df['isPenthouse'].fillna(False).astype("bool")
    df["ad_is_closed"] = df["ad_is_closed"].fillna(False).astype("bool")

    time_print("getting last parsing dates")
    df['offer_page_load_dttm'] = pd.to_datetime(df['offer_page_load_dttm'])
    df['max_dt'] = df.groupby('property_id')['offer_page_load_dttm'].transform('max')

    time_print("getting first creation dates")
    df['creationDate'] = pd.to_datetime(df['creationDate'])
    df['first_creation_date'] = df.groupby('property_id')['creationDate'].transform('min')
    df['last_creation_date'] = df.groupby('property_id')['creationDate'].transform('max')

    time_print("counting duplicates")
    distinct_url_count = df.groupby('property_id')['url'].nunique()
    df['distinct_url_count'] = df['property_id'].map(distinct_url_count)

    time_print("counting entries")
    df['entries_count'] = df.groupby('property_id')['url'].transform('size')

    time_print("getting photos_num")
    df['photos_num'] = df['photo_url_list'].apply(lambda x: len(eval(x)))

    time_print("getting parking col")
    df['parking'] = df['parking'].fillna("{}").apply(lambda x: eval(x).get('type'))

    time_print("parsing sidebar info")
    df['sidebar_info'] = df['sidebar_info'].fillna("[]").apply(lambda sidebar_list: {x['title']: x['value'] for x in eval(sidebar_list)})


    vars_dict = {
        'sale_terms_sidebar': 'Условия сделки',
        'mortgage_sidebar': 'Ипотека',
        'bargaining_sidebar': 'Торг',
        'rent_time': 'Срок аренды',
        'kids_and_animals': 'Условия проживания'
    }

    for key, value in vars_dict.items():
        df[key] = df['sidebar_info'].apply(lambda x: x.get(value))

    df['rent_time'] = df['rent_time'].apply(parse_rent_time)
    df['kids_and_animals'] = df['kids_and_animals'].apply(parse_kids_and_animals)
    df['mortgage_sidebar'] = df['mortgage_sidebar'].apply(lambda x: True if x == 'возможна' else x)
    df['bargaining_sidebar'] = df['bargaining_sidebar'].apply(lambda x: True if x == 'возможен' else x)

    # if sidebar claims that mortgage is allowed - it is allowed
    # if values in 'mortgageAllowed' is missing, but present in 'mortgage_sidebar',
    # 'mortgage_sidebar' is used

    df['mortgageAllowed'] = (
        df['mortgageAllowed']
        .combine(df['mortgage_sidebar'], 
                lambda x, y: True if y else x
        )
    )

    # same for bargainAllowed
    df['bargainAllowed'] = (
        df['bargainAllowed']
        .combine(df['bargaining_sidebar'], 
                lambda x, y: True if y else x
        )
    )

    # price_history fix
    time_print("starting price_history cleaning (may take some time)...")
    df = clean_price_history(df)
    time_print("finished")

    time_print("final afjustments")
    # fill NAs in isApartments:
    # if 'апартамент' is at least in one desc col, then it is apartment
    columns_to_check = ['seo_media_title_short', 'seo_main_title', 'seo_descr', 'title', 'description']
    nan_mask = df['isApartments'].isna()
    df.loc[nan_mask, 'isApartments'] = df.loc[nan_mask, columns_to_check].apply(determine_apartment_status, axis=1)

    urls_to_exclude = set(pd.read_csv('urls_to_exclude.csv')['url'])
    property_id_to_exclude = set(df.query("url in @urls_to_exclude")['property_id'])


    # final adjustments before filtering
    df['currency'] = df['currency'].fillna('rur')
    df['isEmergency'] = df['isEmergency'].fillna(False).astype(bool)
    df['isIllegalConstruction'] = df['isIllegalConstruction'].fillna(False).astype(bool)
    df['bathrooms_num'] = df['wc_type'].apply(sum_nums_in_string)

    query_str = """ 
        1 == 1 \
        and currency == 'rur' \
        and (sale_terms == 'free' or sale_terms_sidebar == 'свободная продажа' or ad_deal_type in ('short_rent', 'long_rent')) \
        and isEmergency == False \
        and isIllegalConstruction == False \
        and offer_page_load_dttm == max_dt \
        and property_id not in @property_id_to_exclude """

    clean_df = df.query(query_str).reset_index()

    # extra cleaning to ensure property_id uniqueness
    property_id_counts = clean_df['property_id'].value_counts()
    unique_keys = property_id_counts[property_id_counts == 1].index
    clean_df = clean_df[clean_df['property_id'].isin(unique_keys)].reset_index()

    # 100% final adjustmentsq
    clean_df = correct_prices(clean_df)
    fix_lat_lng(clean_df, "lat", "lng")


    clean_df[cols_order].to_csv(f"csv/prepared_data/offers_parsed/{deal_type}_cleaned.csv", index = False)

def cleaning_routine():
    deal_types = ['sale_secondary', 'short_rent', 'long_rent', 'sale_primary']
    for single_deal_type in deal_types:
        time_print(f"processing {single_deal_type}")
        clean_dataset(single_deal_type)

    file_names = [f"csv/prepared_data/offers_parsed/{single_deal_type}_cleaned.csv" for single_deal_type in deal_types]
    pd.concat([pd.read_csv(file) for file in file_names]).to_csv("csv/prepared_data/all_deal_types_cleaned.csv", index = False)
