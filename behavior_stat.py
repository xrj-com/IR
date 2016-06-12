from multiprocessing import Manager, Pool
from help_function import *
import pandas as pd
import time
import os
from my_parameter import *

global left_time


def data_count(user_item, train_x):
    global left_time
    temp = train_x.ix[user_item[0]].ix[user_item[1]]
    try:
        next_one = temp.value_counts().reindex([1, 2, 3, 4], fill_value=0)
    except AttributeError:
        next_one = pd.Series({temp: 1}, name='behavior_type').reindex([1, 2, 3, 4], fill_value=0)

    left_time -= 1
    if not left_time % 100:
        print 'left ' + str(left_time) + '  -----Process: %s' % os.getpid()
    return next_one


def m_ui_pc(begin, end, q):
    global X
    global user_item_pair
    print('Process to count: %s' % os.getpid())
    q.put(user_item_pair.iloc[begin:end, :].apply(lambda x: data_count(x, X), axis=1))
    print('Process %s is DONE !' % os.getpid())


def read_queue(q, q_result, p_num):
    global user_item_pair
    df_list = []
    print('Process to combine: %s' % os.getpid())
    while True:
        df = q.get(True)
        df_list.append(df)
        if len(df_list) == p_num:
            break
    q_result.put(pd.concat([user_item_pair, pd.concat(df_list)], axis=1))


def behavior_stat_to_db(param=Const.TRAIN, other=Const.OTHER, process_num=12):
    global left_time
    global X, user_item_pair
    print 'Counting behavior......... '
    x_data = Data(other.PROCESSED_DATA_PATH)
    X = x_data.query(select_table_sql(param.X_TABLE), index=['user_id', 'item_id'])
    X = X['behavior_type']

    set_index = list(set(X.index))
    user_item_pair = pd.DataFrame(set_index, columns=['user_id', 'item_id'])
    print user_item_pair
    user_item_len = len(user_item_pair)

    test_num = user_item_len
    core_num = process_num

    # queue
    queue = Manager().Queue()
    q_result = Manager().Queue()

    # split data to different processes
    interval = test_num/core_num
    left_time = interval
    task_list = [i*interval for i in range(core_num)]
    task_list.append(test_num)

    ####################################
    start_CPU = time.clock()
    start_time = time.time()

    p = Pool(core_num+1)

    for i in range(core_num):
        p.apply_async(m_ui_pc, args=(task_list[i], task_list[i+1], queue))

    p.apply_async(read_queue, args=(queue, q_result, core_num))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')

    value = q_result.get(True)
    final = value.sort_index()
    print final

    end_CPU = time.clock()
    end_time = time.time()
    print '%f CPU second' % (end_CPU - start_CPU)
    print '%f real second' % (end_time - start_time)
    print

    temp = Data(other.PROCESSED_DATA_PATH)
    final.iloc[0:test_num].to_sql(param.X_STATI_BEHAVIOR_TABLE, temp.conn, if_exists='replace')
    show = pd.read_sql_query(select_table_sql(param.X_STATI_BEHAVIOR_TABLE), temp.conn, index_col='index')
    print show

    x_data.close()
    temp.close()

if __name__ == '__main__':
    behavior_stat_to_db(param=Const.TRAIN, other=Const.OTHER, process_num=20)