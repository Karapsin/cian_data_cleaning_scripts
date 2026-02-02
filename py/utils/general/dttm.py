import time
from datetime import datetime, timedelta

def get_current_date(output='text'):
    return str(datetime.now())[0:10] if output=='text' else datetime.now()

def get_current_datetime(output='text'):
    now = datetime.now()
    return now.strftime('%Y-%m-%d %H:%M') if output == 'text' else now

def get_current_time(output='text'):
    return datetime.now().strftime("%H:%M") if output=='text' else datetime.now()

def dttm_to_seconds(dttm):
    if isinstance(dttm, str):
        dttm = datetime.strptime(dttm, '%Y-%m-%d %H:%M')
    return time.mktime(dttm.timetuple())

def parse_date(str_date):
    return datetime.strptime(str_date, '%Y_%m_%d').date()

def shift_dt(dt, days_num):
    return dt + timedelta(days = days_num)

def time_print(string):
    return print((get_current_time()+' '+string))