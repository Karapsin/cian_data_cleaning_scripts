
import pandas as pd
from dateutil.parser import parse
import re
import ast
from tqdm import tqdm

PSEUDO_NONE_STR = 'placeholder'
_dt_pattern = re.compile(r"[\-/:T]") 

# functions to standartize price_history
def is_datetime_like(x) -> bool:
    """True if x can reasonably be read as a date."""
    # Fast pre-check: if the string has no date punctuation, skip full parsing
    s = str(x)
    if not _dt_pattern.search(s):
        return False
    try:
        parse(s)                           # heavy-weight validation
        return True
    except Exception:
        return False


def is_number_like(x) -> bool:
    """True if x can be converted to float (int, float, numeric str)."""
    try:
        float(x)
        return True
    except Exception:
        return False


def fix_tuple(t: tuple):

    if len(t) != 2:           # not a 2-tuple → give up gracefully
        return t

    a, b = t

    # situation matrix --------------------------------------------------------
    a_dt, a_num = is_datetime_like(a), is_number_like(a)
    b_dt, b_num = is_datetime_like(b), is_number_like(b)

    # case 1: already (datetime, number) --------------------------------------
    if a_dt and b_num:
        return (a, b)

    # case 2: reversed (number, datetime) -------------------------------------
    if a_num and b_dt:
        return (b, a)

    raise ValueError("failed to fix tuples")


def tidy_price_history(cell):
    """
    Clean one cell from df['price_history'].
    Works whether the cell is still a string or already a list of tuples.
    """
    data = ast.literal_eval(cell) if isinstance(cell, str) else cell
    return str([fix_tuple(t) for t in data])


def collapse_price_history(df: pd.DataFrame) -> pd.Series:

    df = df.copy()

    seen_urls = set()
    seen_tuples = set()    
    merged_history = list()

    df.loc[:, 'ph_list'] = df['price_history'].apply(ast.literal_eval)

    for ph, url in zip(df["ph_list"], df["url"]):
        if url in seen_urls:           
            continue
        seen_urls.add(url)

        for t in ph:                      
            if t not in seen_tuples:       
                merged_history.append(t)
                seen_tuples.add(t)


    row = df.iloc[0].copy()                 
    row["price_history"] = str(merged_history)  
    
    return row.drop(labels=["ph_list"])          



def clean_price_history(df: pd.DataFrame, batch_pids = 2000) -> pd.DataFrame:

    pids = df['property_id'].unique().tolist()
    rows = []
    for i in tqdm(range(0, len(pids), batch_pids), desc="Collapsing (batched)"):
        chunk = pids[i:i+batch_pids]
        sub = df[df['property_id'].isin(chunk)][
            ['property_id','url','price_history','priceTotal','creationDate']
        ].copy()

        # same transforms as before, but applied once to the sub-DF
        sub['price_history'] = sub['price_history'].fillna(PSEUDO_NONE_STR)
        sub['price_history'] = sub['price_history'].apply(lambda x: PSEUDO_NONE_STR if x == '[]' else x)
        sub["price_history"] = sub.apply(
            lambda row: row['price_history'] if row['price_history'] != PSEUDO_NONE_STR
            else str((row['priceTotal'], str(row['creationDate']).replace(' ', 'T'))),
            axis=1
        )
        mask = sub["price_history"].astype(str).str.startswith('(')
        sub.loc[mask, "price_history"] = '[' + sub.loc[mask, "price_history"] + ']'
        sub['price_history'] = sub['price_history'].apply(tidy_price_history)

        for pid in chunk:
            rows.append(collapse_price_history(sub[sub['property_id'] == pid]))

    df_collapsed = pd.DataFrame(rows).reset_index(drop=True)[['property_id', 'price_history']]

    # merge back the collapsed price_history
    out = df.drop(columns=['price_history']).merge(
        df_collapsed,
        on='property_id',
        how='left'
    )
    return out