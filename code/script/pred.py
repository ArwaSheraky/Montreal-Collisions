''' help message '''

#!/usr/bin/env python

import argparse
import sklearn
import joblib
import datetime
import sklearn
import os
import sys
try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
#    from operator import add
except Exception as e:
    print(e)

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

def parser_assign():
    '''Setting up parser for the input features '''
    parser = argparse.ArgumentParser(usage="Use a list of four elements as positional argument. Elements must be separated by a comma. Format: day,DI,2,neige")
    parser.add_argument("parameters")   # name of the input list specified in Dockerfile as env variable
    args = parser.parse_args()
    i_list = args.parameters
    
    try:
        i_list = i_list.split(",")
        print('\nInput list: ', i_list)
    except Exception as e:
        print(i_list)
    return i_list


def input_check(input_list):
    '''Checking the format of the features'''
    assert len(input_list) == 4, 'Invalid list ' + input_list + ' must include 4 features: one of each from the folowing options: \
        \nlight conditions: day or night \
        \nweek day: DI, LU, MA, ME, JU, VE, SA  \
        \nmonth: 1, 2, 3, ..., 12  \
        \nweather conditions: normal, pluie, naige, verglas '
    assert (input_list[0] == 'day') | (input_list[0] == 'night'), 'Invalid light conditions format: try day or night' 
    assert (input_list[1] == 'DI') | (input_list[1] == 'LU') | (input_list[1] == 'MA') | (input_list[1] == 'ME') | (input_list[1] == 'JU') | (input_list[1] == 'VE') | (input_list[1] == 'SA'), 'Invalid week day format: try  DI, LU, MA, ME, JU, VE, SA ' 
    assert int(input_list[2]), 'Invalid month format: try number from one to 12' 
    assert (input_list[3] == "normal") | (input_list[3] == "pluie") | (input_list[3] == "vent") | (input_list[3] == "neige") | (input_list[3] == "verglas"), \
        'Invalid weather format: try  normal, pluie, neige, verglas '

    print('\nSuccessfully loaded features!')
    return(input_list)


def set_features(input_list):
    ''' Creating the input feature list for prediction model'''
    # format of input_list: ["day/night", "DI/LU/../SA", 1/2/.../12, "normal/pluie/averse/vent/naige/verglas"]
    # format of feature list: [ "day", "night", \ 
    #                           'DI', 'LU', 'MA', 'ME', 'JE', 'VE', 'SA', \ 
    #                           "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", \ 
    #                           "11-12","13-14-15","16-17-18", "19"]

    # assigning zeros
    features = [0 for i in range(25)]
    # day or night
    if input_list[0] == 'day':
        features[0] = 1
    else: 
        features[1] = 1
    # day of week
    labels = ["DI", "LU", "MA", "ME", "JU", "VE", "SA"]
    for i, l in enumerate(labels):
        if input_list[1] == l:
            features[i+2] = 1
    # month
    labels = [i+1 for i in range(13)]
    for i, l in enumerate(labels):
        if input_list[2] == l:
            features[i+9] = 1
    # weather
    w = input_list[3] #"normal", "pluie", "vent", "neige", "verglas"
    if w == "normal":
        features[21] = 1
    if w == "pluie":
        features[22] = 1
    if w == "neige":
        features[23] = 1
    if w == "verglas":
        features[24] = 1

    return features


def do_analyse(features):
    '''Running prediction model'''
    # p = os.getcwd() + '/model.joblib'
    
    model = joblib.load('//app/model.joblib') 
    prediction = model.predict([features])
    predicted_collisions_number = prediction.round(1)[0]
    return predicted_collisions_number

#----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Starting Spark session
    spark = SparkSession.builder \
        .master('spark://master:7077') \
        .appName("words count RDD") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    # Assigning input list name to a local variable and forming the feature list
    input_l = parser_assign()
    input_features = input_check(input_l) 
    features = set_features(input_features)
    
    # Run the prediction model
    result = do_analyse(features)

    # Output results
    folder  = "results_{:%B_%d_%Hh%Mm}".format(datetime.datetime.now())
    if not os.path.exists(folder):
        os.makedirs(folder)
    result_message = '\nShowing results: \nInput features: ' + str(input_features) + '\nPredicted number of collisions: ' + str(int(round(result)))
    print(result_message)
    # result_message.saveAsTextFile("hdfs://hadoop:8020/user/me/results")
 
    sc.stop()
