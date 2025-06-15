import pandas as pd
import numpy as np
import ast
import time 
import pathlib

# ------------------------------------------------------------------------------
# CONFIG -----------------------------------------------------------------------
# ------------------------------------------------------------------------------
CSV_PATH      = "cleaned_data.csv"     # <— source file you uploaded
OUT_PATH      = "daily_prices.csv"     # <— where final file goes
TARGET_START  = pd.Timestamp("2025-04-12", tz="UTC")  # ignore anything earlier

# ------------------------------------------------------------------------------
# 1) READ & BASIC CLEAN --------------------------------------------------------
# ------------------------------------------------------------------------------
df = pd.read_csv(CSV_PATH, low_memory=False)

# ensure date columns are true datetimes
for col in ["creationDate", "editDate"]:
    df[col] = pd.to_datetime(df[col], utc=True, format='ISO8601').dt.floor("D")

# ------------------------------------------------------------------------------
# 2) price_history  → tidy long table ------------------------------------------
# ------------------------------------------------------------------------------
def _parse_list(val):
    """String → real list; anything broken → empty list."""
    if isinstance(val, str) and val.startswith("["):
        try:
            return ast.literal_eval(val)
        except Exception:
            return None
    return None

df["price_history"] = df["price_history"].apply(_parse_list)

prices = (
    df[["property_id", "price_history"]]
      .explode("price_history", ignore_index=True)
)

check1 = prices[~prices["price_history"].apply(lambda x: isinstance(x, tuple))].shape[0] != 0
if check1 != 0:
    raise ValueError("failed to parse some entries in 'price_history' series")


prices[["ts", "price"]] = pd.DataFrame(
    prices.pop("price_history").tolist(),
    index=prices.index
)

prices["ts"]    = pd.to_datetime(prices["ts"], utc=True, format='mixed') 
prices["price"] = prices["price"].astype(float)
prices["date"]  = prices["ts"].dt.floor("D")

# keep **last** change on each calendar day
# i.e. if we had multiple changes of price within 1 day we are keeping the last one
prices = (
    prices.sort_values(["property_id", "date", "ts"])
          .groupby(["property_id", "date"], as_index=False)
          .tail(1)
          .sort_values(["date", "property_id"])
          .reset_index(drop=True)
)

# ------------------------------------------------------------------------------
# 3) build open / close window per property ------------------------------------
# ------------------------------------------------------------------------------
meta = df[["property_id", "creationDate", "editDate", "ad_is_closed"]].copy()

meta["end_date"] = np.where(
    meta["ad_is_closed"] & meta["editDate"].notna(),
    meta["editDate"] - pd.Timedelta(days=1),    # closed ads: day **before** editDate
    pd.NaT
)

last_price_date = prices.groupby("property_id")["date"].max().rename("last_price_date")
meta = meta.merge(last_price_date, on="property_id", how="left")
meta["end_date"] = meta["end_date"].fillna(meta["last_price_date"])
meta.drop(columns="last_price_date", inplace=True)

# ------------------------------------------------------------------------------
# 4) DAILY skeleton ------------------------------------------------------------
# ------------------------------------------------------------------------------
t0 = time.time()
prop_ids, dates = [], []

for pid, start, end in zip(meta["property_id"], meta["creationDate"], meta["end_date"]):
    if pd.isna(start) or pd.isna(end):
        continue
    start = max(start, TARGET_START)
    if end < start:
        continue
    rng = pd.date_range(start, end, freq="D")
    prop_ids.extend(np.repeat(pid, len(rng)))
    dates.extend(rng)

daily = pd.DataFrame({"property_id": prop_ids, "date": dates})
daily = daily.sort_values(["date", "property_id"]).reset_index(drop=True)
print(f"  ▶ daily skeleton built in {time.time()-t0:0.1f}s "
      f"({len(daily):,} rows)")

# ------------------------------------------------------------------------------
# 5) attach the price that was **in force** each day ---------------------------
# ------------------------------------------------------------------------------
prices = prices.sort_values(["date", "property_id"]).reset_index(drop=True)

result = (
    pd.merge_asof(daily, prices,
                  on="date",
                  by="property_id",
                  direction="backward")
      .dropna(subset=["price"])      # drop days *before* first price
      .reset_index(drop=True)
      [["property_id", "date", "price"]]  # neat ordering
)

# ------------------------------------------------------------------------------
# 6) SAVE & SHOW ---------------------------------------------------------------
# ------------------------------------------------------------------------------
pathlib.Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
result.to_csv(OUT_PATH, index=False)

print("\n✅ finished.")
print(f"   rows in final table : {len(result):,}")
print(f"   distinct properties : {result['property_id'].nunique():,}")
display(result.head())
print(f"\nFile saved to: {OUT_PATH}")
