import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from py.utils.yadisk.yadisk_utils import load_file
from py.utils.yadisk.json_state_utils import parse_offer_json

df = pd.read_csv("cleaned_data.csv").query("ad_deal_type == 'long_rent' and ad_is_closed")
mask = (pd.to_datetime(df["last_seen_dttm"]) > (datetime.now() - timedelta(days = 9)))
mask.sum()

df = df[mask]

df["offer_id"] = df["url"].str.split("/").apply(lambda x: x[-2])

offers_to_load = set(df["offer_id"].astype(int))

load_file(offers_to_load, dt_type = "first")
id_phone_pair = list()
for offer_id in offers_to_load:
    html = Path(f"html_load/{offer_id}").read_text(encoding="utf-8")

    try:
        phone = parse_offer_json(html)["tracking"]["page"]["offerPhone"]
    except Exception as e:
        print(offer_id)
        continue

    params_dict = dict()
    params_dict["offer_id"] = offer_id
    params_dict["phone"] = phone

    id_phone_pair.append(params_dict)

phone_df = pd.DataFrame([x for x in id_phone_pair if x['phone'] != ""])
phone_df["offer_id"] = phone_df["offer_id"].astype(str)

df.merge(phone_df, how = "inner").to_csv("phones.csv", index = False)