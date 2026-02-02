import pandas as pd
from py.utils.yadisk.yadisk_utils import get_public_links_for_photos

cols = [
'property_id',
'ad_deal_type',
'url',
'priceTotal',
'floorNumber',
'roomsCount',
'ceilingHeight',
'wc_type',
'repairType',
'windowsViewType',
'totalArea',
'livingArea',
'kitchenArea',
'title',
'description',
'photo_links'
]

df = pd.read_csv("cleaned_data.csv").query("ad_deal_type == 'long_rent' and ad_is_closed").sample(n=200)
df["offer_id"] = df["url"].str.split("/").apply(lambda x: x[-2]).astype(int)



links = get_public_links_for_photos(df["offer_id"].to_list())

links_df = pd.DataFrame({
    'offer_id': [key for key in links.keys()],
    'photo_links': [str(value) for value in links.values()]
})

df = df.merge(links_df, how = 'left')
df['links_num'] = df['photo_links'].apply(lambda x: len(eval(x)))

df[cols].to_csv("df_with_links.csv", index = False)



