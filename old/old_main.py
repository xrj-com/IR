from multiprocessing import Manager, Pool
from help_function import *
import pandas as pd
import time
import os


def m_ui_pc(begin, end, q):
    print('Process to count: %s' % os.getpid())
    q.put(user_item_pair.iloc[begin:end, :].apply(lambda x: data_count(x, train_x), axis=1))
    print('Process %s is DONE !' % os.getpid())


def read_queue(q, q_result, p_num):

    df_list = []
    print('Process to read: %s' % os.getpid())
    while True:
        df = q.get(True)
        df_list.append(df)
        if len(df_list) == p_num:
            break
    q_result.put(pd.concat([user_item_pair, pd.concat(df_list)], axis=1))


if __name__ == '__main__':

    all_data = Data('./data/train_data.db')

    user_item_pair = pd.read_sql_query('select* from user_item_index', all_data.conn, index_col='index')
    train_x = pd.read_sql_query('select* from train_x', all_data.conn, index_col=['user_id', 'item_id'])
    user_item_len = user_item_pair.shape[0]

    # test_num = 10000
    test_num = user_item_len

    core_num = 12

    # queue
    queue = Manager().Queue()
    q_result = Manager().Queue()

    # split data to different processes
    interval = test_num/core_num
    task_list = [i*interval for i in range(core_num)]
    task_list.append(test_num)

    ####################################
    start_CPU = time.clock()
    start_time = time.time()

    p = Pool(core_num+1)
    result = []
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

    temp = Data('./data/train_behavior.db')
    final.iloc[0:test_num].to_sql('user_item_behavior', temp.conn, if_exists='replace')
    show = pd.read_sql_query('select* from user_item_behavior', temp.conn, index_col='index')
    print show

    all_data.close()
    temp.close()
