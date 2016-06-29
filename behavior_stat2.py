from help_function import *
import pandas as pd
from my_parameter import *


def behavior_stat_to_db2(param=Const.TRAIN, other=Const.OTHER):
    # load data from db
    read = GetProcessedData(param)
    # X is training data ~
    X = read.get_X()

    count_1 = X[X['behavior_type'] == 1].groupby(['user_id', 'item_id']).size()
    count_2 = X[X['behavior_type'] == 2].groupby(['user_id', 'item_id']).size()
    count_3 = X[X['behavior_type'] == 3].groupby(['user_id', 'item_id']).size()
    count_4 = X[X['behavior_type'] == 4].groupby(['user_id', 'item_id']).size()

    behavior_count = pd.DataFrame({1: count_1, 2: count_2,
                                   3: count_3, 4: count_4}, )

    behavior_count = behavior_count.fillna(0)

    # save data to db

    temp = Data(other.PROCESSED_DATA_PATH)
    behavior_count.to_sql(param.X_STATI_BEHAVIOR_TABLE, temp.conn, if_exists='replace')
    show = pd.read_sql_query(select_table_sql(param.X_STATI_BEHAVIOR_TABLE), temp.conn)
    print show

    read.close()
    temp.close()

if __name__ == '__main__':

    behavior_stat_to_db2()


