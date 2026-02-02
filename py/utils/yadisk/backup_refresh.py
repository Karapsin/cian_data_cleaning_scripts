import os
import yadisk

from py.utils.db_related.cmd_utils import stop_db, run_sh
from py.utils.db_related.db_utils import query_table
from py.utils.yadisk.yadisk_utils import download_dir, delete_folder
from py.utils.general.dttm import shift_dt, parse_date

def refresh_local_backup():

    client = yadisk.Client(token=os.environ["YANDEX_DISK_TOKEN"])

    all_folders = [item.name for item in client.listdir("/database", type="dir")] 
    dates = [parse_date(x.replace('db_backup_', '')) for x in all_folders]

    last_dt = max(dates)
    folder_to_load = [folder for i, folder in enumerate(all_folders) if dates[i] == last_dt][0]




    print(f"last date is {last_dt}, starting download...")
    download_dir(client, f"/database/{folder_to_load}", "loaded_backup", 500)
    print("download is finished, starting new db")

    run_sh("restore_db.sh")
    has_data = query_table('parsing_finish_dttms').shape[0] > 0
    stop_db()
    
    if not(has_data):
        raise ValueError("backup is broken")

    print("new db started")

    cutoff_dt = shift_dt(last_dt, -30)
    folders_to_delete = [folder for i, folder in enumerate(all_folders) if dates[i] < cutoff_dt]
    if len(folders_to_delete) > 0:
        print("cleaning unneeded backups")
        [delete_folder(client, f"database/{folder}") for folder in folders_to_delete]
    print("finished")
