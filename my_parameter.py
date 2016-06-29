
# time range:(2014-11-18-00, q2014-12-18 23 zhousi)


class Const:
    # train data time range and table
    class TRAIN:
        X_TIME_BEGIN = "2014-12-11 00"
        X_TIME_END = "2014-12-16 23"
        X_TABLE = 'train_x'
        X_FEATURE_TABLE = 'train_feature'
        X_STATI_BEHAVIOR_TABLE = 'train_behavior'
        X_FIL = 'train_fil_x'

        LABEL_BEGIN = "2014-12-17 00"
        LABEL_END = "2014-12-17 23"
        LABEL_TABLE = 'train_label'

        def __init__(self):
            pass

    # test data time range and table
    class TEST:
        X_TIME_BEGIN = "2014-12-12 00"
        X_TIME_END = "2014-12-17 23"
        X_TABLE = 'test_x'
        X_FEATURE_TABLE = 'test_feature'
        X_STATI_BEHAVIOR_TABLE = 'test_behavior'
        X_FIL = 'test_fil_x'

        LABEL_BEGIN = "2014-12-18 00"
        LABEL_END = "2014-12-18 23"
        LABEL_TABLE = 'test_label'

        def __init__(self):
            pass

    class OTHER:
        # data path
        ORIG_DATA_PATH = './data/train_data.db'
        PROCESSED_DATA_PATH = './data/processed_data.db'
        ORIG_USER_TABLE = 'train_user'
        ORIG_ITEM_TABLE = 'train_item'
        DATA_TEST_TABLE = 'just_for_test'

        def __init__(self):
            pass

    def __init__(self):
        pass



