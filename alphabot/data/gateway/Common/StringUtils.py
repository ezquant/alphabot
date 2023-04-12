import datetime
import bson
import re
import time


DATE_FORMAT = '%Y-%m-%d'

def is_date(date_str):
    try:
        time.strptime(date_str, DATE_FORMAT)
        return True
    except:
        return False

def get_date(date_str):
    if is_date(date_str):
        struct_time = time.strptime(date_str, DATE_FORMAT)
        return datetime.datetime(*struct_time[:6]).date()
    raise Exception("not correct date format")

def date_to_object_id(date_time: str):
    if len(date_time) != 10:
        raise Exception("date length doesn't equal to 8")
    time = datetime.datetime.strptime(date_time, DATE_FORMAT)
    return bson.ObjectId.from_datetime(time)