import os 
import numpy as np
import pandas as pd

from py.utils.yadisk.yadisk_utils import get_dir_names
from py.final_datasets.cols_order import (
    LONG_RENT_COLS,
    SALE_SECONDARY_COLS
)

def prepare_final_dataset(deal_type, start_dt, end_dt, days_to_follow, cols_order):

    # data load
    df = pd.read_csv(f"csv/prepared_data/offers_parsed/{deal_type}_cleaned.csv")

    # start_dt filter
    start_dt_mask = pd.to_datetime(df['first_creation_date']) >= pd.to_datetime(start_dt)
    df = df[start_dt_mask].reset_index()

    # end_dt filter
    end_dt_mask = pd.to_datetime(df['first_creation_date']) <= pd.to_datetime(end_dt)
    df = df[end_dt_mask].reset_index()


    # max(last_seen_dttm) for each by property ids
    last_seen_df = (
        pd.read_csv(f"csv/prepared_data/search_clean/{deal_type}.csv")
            .query(f"ad_deal_type == '{deal_type}'")
            .dropna(subset=['last_seen_dttm'])
            [['property_id', 'last_seen_dttm']]
            .groupby(['property_id'])
            .agg(last_seen_dttm=('last_seen_dttm', 'max'))
            .reset_index()
    )

    # join to get last_seen_dttm
    df = df.merge(last_seen_df, how = 'inner', on='property_id')

    # is_censored
    df['first_creation_date'] = pd.to_datetime(df['first_creation_date'])
    df['last_seen_dttm'] = pd.to_datetime(df['last_seen_dttm'])
    df['is_censored'] = df['last_seen_dttm'] > (df['first_creation_date'] + pd.Timedelta(days=days_to_follow))

    # duration calc
    df['duration'] = (df['last_seen_dttm'] - df['first_creation_date']) / pd.Timedelta(hours=24)

    # sanity check which saved me twice already
    if df.query("duration < 0").shape[0] != 0:
        raise ValueError("negative duration (something is wrong)")

    df.loc[df['ad_is_closed'] == False, 'duration'] = None

    # get ao and district cols
    df = df.merge(pd.read_excel("xlsx/geo/processed/districts.xlsx").rename(columns = {"district_code": "search_alias"}), how = 'inner', on='search_alias')


    df[cols_order].to_csv(f"csv/final_datasets/{deal_type}.csv", index = False)


def get_dirs_csv():

    deal_types = ['long_rent', 'sale_secondary']

    df = pd.concat([pd.read_csv(f"csv/final_datasets/{single_deal_type}.csv")[['ad_deal_type', 'offer_id', 'property_id']] 
                    for single_deal_type in deal_types
                    ]
        )

    df['offer_id'] = df['offer_id'].apply(lambda x: eval(x))
    df = df.explode('offer_id', ignore_index=True)

    all_offer_ids = set(df['offer_id'].to_list())
    dirs_df = get_dir_names(all_offer_ids, "first")[["dir", "offer_id"]]

    df = df.merge(dirs_df, how = 'inner', on='offer_id')
    df["dir"] = df["dir"].apply(lambda x: f"/cian_project_photos/{x}/photos")

    df.to_csv("csv/final_datasets/photo_dirs.csv", index = False)


start_dt = '2025-07-01'
end_dt = '2025-08-15'

prepare_final_dataset('long_rent', start_dt, end_dt, 45, LONG_RENT_COLS)
prepare_final_dataset('sale_secondary', start_dt, end_dt, 90, SALE_SECONDARY_COLS)
get_dirs_csv()
