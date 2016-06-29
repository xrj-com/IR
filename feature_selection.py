# -*- coding:utf-8 -*-
from processing import *
from behavior_stat import *
from my_parameter import *


def feature_to_db(param=Const.TRAIN, other=Const.OTHER):

    read_data = GetProcessedData(param)

    mainData = read_data.get_fil_X()
    label = read_data.get_label()


















    print X_feature


    # save data
    # temp = Data(other.PROCESSED_DATA_PATH)
    # X_feature.to_sql(param.X_FEATURE_TABLE, temp.conn, if_exists='replace')
    # read_data.close()
    # temp.close()

if __name__ == '__main__':
    feature_to_db(param=Const.TRAIN, other=Const.OTHER)