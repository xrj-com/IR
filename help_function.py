import sqlite3
import pandas.io.sql as sql
import pandas as pd
from my_parameter import *


class Data:

    def __init__(self, data_path):
        self.data_path = data_path
        self.conn = sqlite3.connect(self.data_path)

    def __del__(self):
        self.conn.close()

    def query(self, str_sql, index=None):
        if index is None:
            data = sql.read_sql(str_sql, self.conn)
        else:
            data = sql.read_sql(str_sql, self.conn, index_col=index)
        return data

    def close(self):
        self.conn.close()


class GetProcessedData:

    def __init__(self, param=Const.TRAIN, other=Const.OTHER):
        self.param = param
        self.other = other
        self.Data = Data(other.PROCESSED_DATA_PATH)

    def get_X(self):
        X = self.Data.query(select_table_sql(self.param.X_TABLE))
        return X

    def get_label(self):
        label = self.Data.query(select_table_sql(self.param.LABEL_TABLE))
        return label

    def get_feature(self):
        feature = self.Data.query(select_table_sql(self.param.X_FEATURE_TABLE))
        return feature

    def get_behavior_count(self):
        count = self.Data.query(select_table_sql(self.param.X_STATI_BEHAVIOR_TABLE))
        return count

    def get_fil_label(self):
        fil_label = self.Data.query(select_table_sql(self.param.LABEL_FIL_TABLE))
        return fil_label

    def get_play_test_data(self):
        play = self.Data.query(select_table_sql(self.other.DATA_TEST_TABLE))
        return play

    def close(self):
        self.Data.close()


def drop_table_sql(table_name):
    return 'drop table '+table_name


def select_label_sql(param=Const.TRAIN, other=Const.OTHER):

    sql_str = 'select * from ' + other.ORIG_USER_TABLE + ' where time between "'\
           + param.LABEL_BEGIN + '" and "' + param.LABEL_END + '"'

    return sql_str


def select_x_sql(param=Const.TRAIN, other=Const.OTHER):
    sql_str = 'select * from ' + other.ORIG_USER_TABLE + ' where time between "' \
              + param.X_TIME_BEGIN + '" and "' + param.X_TIME_END + '"'

    return sql_str


def select_table_sql(table_name):
    return 'select * from ' + table_name


if __name__ == '__main__':
    pass
    # test = Data(Const.OTHER.PROCESSED_DATA_PATH)
    # test.query(drop_table_sql('train_fil_x'))
    # test.close()
    # test = GetProcessedData()
    # test.get_fil_X()


