from processing import *
from behavior_stat import *
from my_parameter import *


def feature_to_db(param=Const.TRAIN, other=Const.OTHER, is_test=True):

    X_data = Data(other.PROCESSED_DATA_PATH)

    X_feature = X_data.query(select_table_sql(param.X_STATI_BEHAVIOR_TABLE), index=['user_id', 'item_id'])

    X_feature.to_sql(param.X_FEATURE_TABLE, X_data.conn, if_exists='replace')


    if is_test:
        test = X_data.query(select_table_sql(param.X_FEATURE_TABLE), index=['user_id', 'item_id'])
        print test

    X_data.close()