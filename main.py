from processing import *
from behavior_stat import *
from my_parameter import *
from feature_selection import feature_to_db
from train_model_evaluate import train_model


if __name__ == '__main__':

    # for the train set

    # slice_x_to_db(param=Const.TRAIN, other=Const.OTHER, is_test=True)
    # get_label_to_db(param=Const.TRAIN, other=Const.OTHER, is_test=True)
    # mach_label_with_X(Const.TRAIN)
    # behavior_stat_to_db(param=Const.TRAIN, other=Const.OTHER, process_num=12)
    # feature_to_db(param=Const.TRAIN, other=Const.OTHER, is_test=True)

    ######################################################################
    # for the test set
    # slice_x_to_db(param=Const.TEST, other=Const.OTHER, is_test=True)
    # get_label_to_db(param=Const.TEST, other=Const.OTHER, is_test=True)
    # mach_label_with_X(Const.TEST)
    # behavior_stat_to_db(param=Const.TEST, other=Const.OTHER, process_num=12)
    # feature_to_db(param=Const.TEST, other=Const.OTHER, is_test=True)

    # training model

    # clf = train_model(param=Const.TRAIN, other=Const.OTHER)
    # print clf

    # training model
    # clf = train_model(param=Const.TRAIN, other=Const.OTHER)
    #
    #
    # #save the model
    # joblib.dump(clf,'./model/demo.pkl')
    # ##load the model
    # #clf=joblib.load('./model/demo.pkl')
    #
    # # evaluate
    # py=evaluate(clf)
    # np.save("prediction_label.npy",py)
    #
    # F1,P,R=score(py)
    # print "F1=",F1
    # print "P=",P
    # print "R=",R
    #
    # np.save("evaluate.npy",[F1,P,R])



    test = GetProcessedData(Const.TRAIN)
    print test.get_X()



