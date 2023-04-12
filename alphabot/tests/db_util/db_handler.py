#!/usr/bin/env python2.7
import pymongo
from pymongo import MongoClient
import datetime as dt


class CycType(object):
    CYC_MINUTE = 1
    CYC_DAY = 2
    CYC_WEEK = 3
    CYC_MONTH = 4
    CYC_SEASON = 5
    CYC_HAFLYEAR = 6
    CYC_YEAR = 7


DB_IP = '127.0.0.1'
DB_PORT = 27017
DB_NAME = 'emquant'

start = dt.datetime(2017, 3, 16, 0, 0, 0, 0)
end = dt.datetime.now()
cyc_type = CycType.CYC_MINUTE


def col_print(col, cyc_type, start, end):
    for doc in col.find({'cycType': cyc_type, 'date': {"$gte": start, "$lte": end}}).sort("date", pymongo.ASCENDING):
        print(doc)

def col_remove(col, cyc_type, start, end):
    col.remove({'cycType': cyc_type, 'date': {"$gte": start, "$lte": end}})

def col_create_index(col, index_list):
    indexes_str = '_1_'.join(index_list) + '_1'
    indexes = [(x, pymongo.ASCENDING) for x in index_list]
    for key, value in col.index_information().items():
        if indexes_str in key:
            return
    print('Create index for {}'.format(col.full_name))
    print(indexes)
    col.create_index(indexes)


if __name__ == '__main__':
    emq_db = MongoClient(DB_IP, DB_PORT)[DB_NAME]
    col_list = sorted(emq_db.collection_names())
    code_list = [c for c in col_list if 'SH_' in c or 'SZ_' in c]
    for code in code_list:
        print('--- {} ---'.format(code))
        col = emq_db[code]
        # col_print(col, cyc_type, start, end)
        # col_remove(col, cyc_type, start, end)
        col_create_index(col, ['date'])

