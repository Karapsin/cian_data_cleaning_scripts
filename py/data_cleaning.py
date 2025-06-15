import pandas as pd
from functools import reduce 
from hashlib import sha256
import re

from py.cols_order import cols_order
from py.clean_price_history import clean_price_history


def concat_series(series1, series2):
    return series1.astype(str) + '_' + series2.astype(str)

def hash_str(input_str):
    return sha256(input_str.encode()).hexdigest()

def hash_cols(df, cols_list):
    
    series_list = [df[col] for col in cols_list]
    glued_cols = reduce(concat_series, series_list)    

    return glued_cols.apply(hash_str)

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

############################################################################################3
# main function

def clean_dataset(df):

    df['creationDate'] = pd.to_datetime(df['creationDate'], format = 'ISO8601')

    key_columns = ['lat', 'lng', 'floorNumber', 'roomsCount', 'ad_deal_type', 'totalArea']
    df['property_id'] =  hash_cols(df, key_columns)

    # lifts
    df['passengerLiftsCount'] = abs(df['passengerLiftsCount'])
    df['cargoLiftsCount'] = abs(df['cargoLiftsCount'])

    # cian price range
    df[['cian_range_left_bound', 'cian_range_right_bound']] = df['cian_price_range'].apply(parse_cian_range).apply(pd.Series)

    # turning some cols to bool + filling NAs
    df['is_individual_project'] = df['seriesName'].apply(lambda x: x == 'Индивидуальный проект')
    df['has_videos'] = df['videos'].fillna("[]").apply(lambda x: True if x != "[]" else False).value_counts()
    df['isPenthouse'] = df['isPenthouse'].fillna(False)
    df["ad_is_closed"] = df["ad_is_closed"].fillna(False)

    # get last parsing dates
    df['offer_page_load_dttm'] = pd.to_datetime(df['offer_page_load_dttm'])
    df['max_dt'] = df.groupby('property_id')['offer_page_load_dttm'].transform('max')

    # get first creation dates
    df['creationDate'] = pd.to_datetime(df['creationDate'])
    df['first_creation_date'] = df.groupby('property_id')['creationDate'].transform('max')
    df['last_creation_date'] = df.groupby('property_id')['creationDate'].transform('min')

    # count duplicates'
    distinct_url_count = df.groupby('property_id')['url'].nunique()
    df['distinct_url_count'] = df['property_id'].map(distinct_url_count)

    # count entries 
    df['entries_count'] = df.groupby('property_id')['url'].transform('size')

    # photos_num
    df['photos_num'] = df['photo_url_list'].apply(lambda x: len(eval(x)))

    # parking
    df['parking'] = df['parking'].fillna("{}").apply(lambda x: eval(x).get('type'))

    # sidebar
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
    df = clean_price_history(df)

    # fill NAs in isApartments:
    # if 'апартамент' is at least in one desc col, then it is apartment
    columns_to_check = ['seo_media_title_short', 'seo_main_title', 'seo_descr', 'title', 'description']
    nan_mask = df['isApartments'].isna()
    df.loc[nan_mask, 'isApartments'] = df.loc[nan_mask, columns_to_check].apply(determine_apartment_status, axis=1)

    urls_to_exclude = set(pd.read_csv('urls_to_exclude.csv')['url'])
    property_id_to_exclude = set(df.query("url in @urls_to_exclude")['property_id'])


    # final adjustments before filtering
    df['currency'] = df['currency'].fillna('rur')
    df['isEmergency'] = df['isEmergency'].fillna(False)
    df['isIllegalConstruction'] = df['isIllegalConstruction'].fillna(False)


    query_str = """ 
        1 == 1 \
        and currency == 'rur' \
        and (sale_terms == 'free' or sale_terms_sidebar == 'свободная продажа' or ad_deal_type in ('short_rent', 'long_rent')) \
        and isEmergency == False \
        and isIllegalConstruction == False \
        and offer_page_load_dttm == max_dt \
        and property_id not in @property_id_to_exclude """

    clean_df = df.query(query_str).reset_index()

    return clean_df[cols_order]


clean_df = clean_dataset(pd.read_csv('offers_parsed.csv'))
clean_df.to_csv("cleaned_data.csv", index = False)
