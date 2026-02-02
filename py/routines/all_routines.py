from py.utils.yadisk.backup_refresh import refresh_local_backup
from py.utils.yadisk.refresh_local_yadisk_dirs import refresh_yadisk_dirs
from py.utils.data_cleaning.data_cleaning import cleaning_routine
from py.utils.general.dttm import time_print
from py.utils.geo.coords_features_gen import get_geo_features_df

def resfresh_local_db():
    time_print("refreshing local mongodb")
    refresh_local_backup()

def refresh_yadisk_dirs_table():
    time_print("refreshing yadisk dirs data")
    refresh_yadisk_dirs()

def do_cleaning_routine():
    time_print("cleaning parsed offers table")
    cleaning_routine()

def compute_geo_features():
    time_print("computing geo features")
    get_geo_features_df()
