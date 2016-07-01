# encoding=utf-8
from help_function import *
from my_parameter import *
import numpy as np


def slice_x_to_db(param=Const.TRAIN, other=Const.OTHER, is_test=True):

    print 'Slice x between ' + param.X_TIME_BEGIN + ' and ' + param.X_TIME_END

    orig_data = Data(other.ORIG_DATA_PATH)
    sql_str = select_x_sql(param)
    x = orig_data.query(sql_str)

    # set index to user_id and item_id
    x = x[['user_id', 'item_id', 'behavior_type', 'time']][:]
    x = x.set_index(['user_id', 'item_id'])

    # save data to db
    new_data = Data(other.PROCESSED_DATA_PATH)
    x.to_sql(param.X_TABLE, new_data.conn, if_exists='replace')

    # test result
    if is_test:

        test_frame = pd.read_sql_query('select* from ' + param.X_TABLE, new_data.conn, index_col=['user_id', 'item_id'])
        print test_frame

    orig_data.close()
    new_data.close()


def get_label_to_db(param=Const.TRAIN, other=Const.OTHER, is_test=True):

    print 'Getting label data between ' + param.LABEL_BEGIN + ' and ' + param.LABEL_END

    orig_data = Data(other.ORIG_DATA_PATH)

    sql_str = select_label_sql(param)
    label = orig_data.query(sql_str)
    del label['user_geohash']
    del label['item_category']
    del label['time']
    del label['index']

    result = label.drop_duplicates(['user_id', 'item_id', 'behavior_type'])
    result = result.set_index(['user_id', 'item_id'])

    is_buy_idx = result[result.behavior_type == 4].index
    not_buy_idx = result.index.difference(is_buy_idx)
    is_buy = pd.Series(1.0, index=is_buy_idx, name='is_buy')

    not_buy = pd.Series(0.0, index=not_buy_idx, name='is_buy')
    label = is_buy.append(not_buy)
    new_label = pd.DataFrame(label)

    new_data = Data(other.PROCESSED_DATA_PATH)
    new_label.to_sql(param.LABEL_TABLE, new_data.conn, if_exists='replace')

    if is_test:
        test_frame = pd.read_sql_query('select* from ' + param.LABEL_TABLE, new_data.conn, index_col=['user_id', 'item_id'])
        print test_frame

    new_data.close()
    orig_data.close()


def x_filter(param=Const.TRAIN, other=Const.OTHER):
    '''
    print len(sys.argv)
    print str(sys.argv[0])
    print str(sys.argv[1])
    uibDF = sys.argv[1]
    labelDF = sys.argv[2]
    '''
    X_data = Data(other.PROCESSED_DATA_PATH)
    uibDF = X_data.query(select_table_sql(param.X_STATI_BEHAVIOR_TABLE))
    labelDF = X_data.query(select_table_sql(param.LABEL_TABLE))

    # conn = sqlite3.connect('./data/train_behavior.db')
    # uibDF = pd.read_sql_query('select* from user_item_behavior', conn, index_col='index')
    # labelDF = pd.read_sql_query('select* from train_label', conn)

    uibDF2 = uibDF.drop(['item_id'], axis=1)
    print uibDF2
    grouped = uibDF2.groupby('user_id')
    sum = grouped.sum().reset_index()
    print sum
    sum.insert(5, 'sum123+', sum['1'] + sum['2'] + sum['3'] + 0.5)
    sum.insert(6, '4+', sum['4'] + 0.5)
    sum.insert(7, 'rate', sum['sum123+'] / sum['4+'])
    sum_sorted = sum.sort_values(['rate']).reset_index()

    # 找到位于1/3处的rate值和位于2/3处的rate值
    minRate = sum_sorted.iloc[len(sum_sorted.index)/3, 8]
    maxRate = sum_sorted.iloc[len(sum_sorted.index)*2/3, 8]

    print minRate
    print maxRate

    # 将每个用户对应的rate制成user_rate表
    user_rate = sum_sorted.drop(['index', '1', '2', '3', '4', 'sum123+', '4+'], axis=1)

    # label表中每一个UI对中user对应的rate添加进去并形成新表train_label_rate
    user_label_rate = pd.merge(labelDF, user_rate, on='user_id', how='left')

    # 对train_label_rate进行处理，添加一列filter
    # 去除：rate小于minRate且购买了的 或 rate大于maxRate且未购买的
    # 用0表示过滤，1表示保留

    filterList=[]
    filterNum = 0
    for x in range(len(labelDF.index)):
        print x
        if user_label_rate.ix[x]['rate'] < minRate:
            if user_label_rate.ix[x]['is_buy'] == 1:
                filterList.append(0)
                filterNum = filterNum + 1
            else:
                filterList.append(1)
        elif user_label_rate.ix[x][3] > maxRate:
            if user_label_rate.ix[x][2] == 0:
                filterList.append(0)
                filterNum = filterNum + 1
            else:
                filterList.append(1)
        else:
            filterList.append(1)

    filterSeries=pd.Series(filterList,index=user_label_rate.index)
    user_label_rate['filter'] = filterSeries

    user_label_rate2 = user_label_rate.drop(['rate'], axis=1)

    # 需要的话可以用这一句将生成的表写入数据库
    user_label_rate2.to_sql(param.LABEL_FIL_TABLE, X_data.conn, if_exists='replace')


    print user_label_rate2


def mach_label_with_X(param=Const.TRAIN, other=Const.OTHER):

    index = ['user_id', 'item_id']
    process_data = GetProcessedData(param=param, other=other)
    X = process_data.get_X().set_index(index)
    label = process_data.get_label().set_index(index)
    X_minus_label_index = X.index.difference(label.index)
    label_minus_X_index = label.index.difference(X.index)
    left_label_index = label.index.difference(label_minus_X_index)

    label = label.ix[left_label_index]
    add_label = pd.DataFrame(0, index=X_minus_label_index, columns=['is_buy'])
    label = pd.concat([label, add_label], axis=0)

    new_data = Data(other.PROCESSED_DATA_PATH)
    label.to_sql(param.LABEL_TABLE, new_data.conn, if_exists='replace')
    new_data.close()


def evaluate(clf, param=Const.TEST, other=Const.OTHER):
    print 'Testing model...'

    X_data = Data(other.PROCESSED_DATA_PATH)
    X_feature = X_data.query(select_table_sql(param.X_FEATURE_TABLE), index=['user_id', 'item_id'])

    py = clf.predict(X_feature)

    return py


def score(py):

    ##prediction file
    prediction = []
    X_data = Data(Const.OTHER.PROCESSED_DATA_PATH)
    X_feature = X_data.query(select_table_sql(Const.TEST.X_FEATURE_TABLE), index=['user_id', 'item_id'])
    i=-1
    for a in py:
        i=i+1
        if a == 1.0:
            prediction.append((X_feature.index[i][0],X_feature.index[i][1]))
    prediction = set(prediction)


    ##ture file
    conn = sqlite3.connect('./data/processed_data.db')
    curs = conn.cursor()
    curs.execute("select * from " + Const.TEST.LABEL_TABLE)
    test_label = curs.fetchall()
    answer = []
    for i in range(len(test_label)):
        if test_label[i][2] == 1.0:
            answer.append((test_label[i][0],test_label[i][1]))
    answer = set(answer)


    ##the true sample
    inter = prediction & answer

    # print "the number of correct sample:"+len(inter)

    ##compute F1,P,R
    if len(inter) > 0:
        a = len(answer)
        b = len(prediction)
        c = len(inter)
        R = 1.0 * c / a * 100
        P = 1.0 * c / b * 100
        F1 = 2.0 * R * P / (P + R)
    return F1,P,R



if __name__ == "__main__":
    x_filter(param=Const.TRAIN, other=Const.OTHER)


























