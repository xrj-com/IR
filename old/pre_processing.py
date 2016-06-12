from help_function import *

all_data = Data('./data/train_data.db')
######################## get train_x ########################
# time range:(2014-11-18-00, q2014-12-18 23 zhousi)
str_sql = 'select * from train_user where time between "2014-12-14 00" and "2014-12-16 23"'
train_x = all_data.query(str_sql)

# get unique UI pair and length
user_item_pair = train_x.ix[:, ['user_id', 'item_id']]
user_item_pair = user_item_pair.drop_duplicates(['user_id', 'item_id'])
user_item_pair = user_item_pair.reset_index(drop=True)
user_item_len = user_item_pair.shape[0]

# set index to user_id and item_id
train_x = train_x[['user_id', 'item_id', 'behavior_type', 'time']][:]
train_x = train_x.set_index(['user_id', 'item_id'])

# save data to db
user_item_pair.to_sql('user_item_index', all_data.conn, if_exists='replace')
train_x.to_sql('train_x', all_data.conn, if_exists='replace')

# test result
test = pd.read_sql_query('select* from user_item_index', all_data.conn, index_col='index')
print test
test = pd.read_sql_query('select* from train_x', all_data.conn, index_col=['user_id', 'item_id'])
print test
all_data.close()



############################ get labels  ##################################################
from help_function import *
import numpy as np

all_data = Data('./data/train_data.db')

str_sql = 'select * from train_user where time between "2014-12-17 00" and "2014-12-17 23"'
train_label = all_data.query(str_sql)
del train_label['user_geohash']
del train_label['item_category']
del train_label['time']
del train_label['index']


result = train_label.drop_duplicates(['user_id', 'item_id', 'behavior_type'])
result = result.set_index(['user_id', 'item_id'])

is_buy_idx = result[result.behavior_type == 4].index
not_buy_idx = result.index.difference(is_buy_idx)
is_buy = pd.Series(1.0, index=is_buy_idx, name='is_buy')
not_buy = pd.Series(0.0, index=not_buy_idx, name='is_buy')
train_label = is_buy.append(not_buy)
label_frame = pd.DataFrame(train_label)


some = Data('./data/train_behavior.db')
label_frame.to_sql('train_label', some.conn, if_exists='replace')
some.close()

########################### Train model #####################################

from sklearn import svm
clf = svm.SVC(kernel='linear')
some = Data('./data/train_behavior.db')
train_x = pd.read_sql_query('select* from user_item_behavior', some.conn, index_col=['user_id', 'item_id'])
train_label = pd.read_sql_query('select* from train_label', some.conn, index_col=['user_id', 'item_id'])
del train_x['index']
same_index = train_x.index.intersection(train_label.index)
train_x = train_x.ix[same_index].astype(float).values
train_label = train_label.ix[same_index].astype(float).values
train_label = np.array([one[0] for one in train_label])

clf.fit(train_x, train_label)

