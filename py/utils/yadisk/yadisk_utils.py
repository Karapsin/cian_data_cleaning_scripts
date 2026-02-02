import os
import yadisk
import pathlib 
import pandas as pd

def download_dir(client, 
                 remote_dir, 
                 local_dir, 
                 batch=500
    ):

    local_dir = pathlib.Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    offset = 0
    while True:
        chunk = list(client.listdir(remote_dir, limit=batch, offset=offset))
        for res in chunk:
            print(f"downloading {res.name}")
            dst = local_dir / res.name          # just the leaf name
            if res.type == "dir":
                download_dir(client, res.path, dst, batch)
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                client.download(res.path, dst.as_posix())
        if len(chunk) < batch:                  
            break
        offset += batch                        

def delete_folder(client: yadisk.Client,
                  path: str,
                  *,
                  permanently: bool = False,
                  wait: bool = True,
                  poll_interval: float = 1.0,
                  poll_timeout: float | None = None
    ) -> None:

    client.remove(
        path,
        permanently=permanently,
        wait=wait,
        poll_interval=poll_interval,
        poll_timeout=poll_timeout,
    )
    print(f'deleted {path!r}{" permanently" if permanently else ""}')


def get_dir_names(all_offer_ids, dt_type):

    if dt_type not in {"last", "first"}:
        raise ValueError(f"unknown dt_type = '{dt_type}', only 'last' and 'first' are supported")

    fun = 'max' if dt_type == 'last' else 'min'


    df = pd.read_csv("yadisk_dirs.csv")
    df["date"] = pd.to_datetime(df["dir"].str.extract(r"_(\d{4}-\d{2}-\d{2})$")[0],   
                                format="%Y-%m-%d",
                                errors="coerce"                                      
                 )

    df["query_dt"] = (
        df
        .groupby("offer_id")["date"]      
        .transform(fun)               
    )


    df.query("offer_id in @all_offer_ids and date == query_dt")
    filtered_df = df.query("offer_id in @all_offer_ids and date == query_dt")

    return filtered_df

def load_file(all_offer_ids, filename, dt_type = "last", output_dir = "html_load"):
    
    filtered_df = get_dir_names(all_offer_ids, dt_type = dt_type)

    for dir_name, offer_id in zip(filtered_df["dir"], filtered_df["offer_id"]):
        with yadisk.Client(token=os.environ["YANDEX_DISK_TOKEN"]) as client:
            client.download(f"/cian_project_photos/{dir_name}/{filename}", f'{output_dir}/{offer_id}')
