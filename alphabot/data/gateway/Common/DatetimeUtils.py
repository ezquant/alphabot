from Common.StringUtils import *
from datetime import date, timedelta

def get_interval_dates(start_date: str, end_date: str):
    if not is_date(start_date) or not is_date(end_date):
        raise Exception("start date or end date format mismatched")
    s_date = get_date(start_date)
    e_date = get_date(end_date)

    if s_date > e_date:
        raise Exception("start date later than end date")

    delta = e_date - s_date

    days = []
    for i in range(delta.days):
        day = s_date + timedelta(days = i)
        days.append(str(day))

    return days

def get_days_between_dates(start_date: str, end_date: str):
    if not is_date(start_date) or not is_date(end_date):
        raise Exception("start date or end date format mismatched")
    s_date = get_date(start_date)
    e_date = get_date(end_date)
    return (e_date - s_date).days

# "2019-10-01" -> 20191001
def convert_date_str_to_int(date_str: str):
    if not is_date(date_str):
        raise Exception("not date str for converting")
    return int(date_str.replace("-", ""))


def get_today_date():
    return date.today()

def get_n_days_before_date(n: int):
    dt = datetime.datetime.now() - timedelta(days=n)
    return dt.date()

def date_to_str(d: date):
    return d.strftime(DATE_FORMAT)