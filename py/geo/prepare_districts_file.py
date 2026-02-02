import pandas as pd

# read raw dataset
districts_df = pd.read_excel("xlsx/geo/raw/districts.xlsx")[['name', 'code', 'type', 'district_code']]

# process ao entries
ao_df = districts_df.query("type==2")[['name', 'code']]
ao_df['code'] = ao_df['code'].astype(str).apply(lambda x: x[:1]).astype(int)
ao_df.rename(columns={"name": "ao"}, inplace=True)

# process district entries
districts_df = districts_df.dropna(subset="district_code")[['name', 'code', 'district_code']]
districts_df['code'] = districts_df['code'].astype(str).apply(lambda x: x[:1]).astype(int)
districts_df.rename(columns={"name": "district"}, inplace=True)

# final df
districts_df = districts_df.merge(ao_df)[['ao', 'district', 'district_code']]
districts_df.to_excel("xlsx/geo/processed/districts.xlsx")
