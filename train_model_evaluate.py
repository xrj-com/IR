from sklearn import svm
from behavior_stat import *
from my_parameter import Const
import numpy as np


def train_model(param=Const.TRAIN, other=Const.OTHER):
    print 'Training model...'

    clf = svm.SVC(kernel='linear')
    X_data = Data(other.PROCESSED_DATA_PATH)
    X_feature = X_data.query(select_table_sql(param.X_FEATURE_TABLE), index=['user_id', 'item_id'])
    label = X_data.query(select_table_sql(param.LABEL_TABLE), index=['user_id', 'item_id'])

    same_index = X_feature.index.intersection(label.index)

    train_X = X_feature.ix[same_index].astype(float).values
    train_label = label.ix[same_index].astype(float).values
    train_label = np.array([one[0] for one in train_label])

    clf.fit(train_X, train_label)

    return clf

