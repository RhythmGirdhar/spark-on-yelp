__author__ = 'Rhythm Girdhar'
__email__ = 'rgirdhar@usc.edu'

import sys
import json
import pyspark
from pyspark.context import SparkContext

def print_record(record):
    print(type(record))

def getUserID(record):
    user_id = record.get("user_id")
    return (user_id, 1)

def getBusinessID(record):
    user_id = record.get("business_id")
    return (user_id, 1)

def write_json(output_file, result):
    with open(output_file, 'w+', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # output_file = "/Users/rhythmgirdhar/Desktop/MS-CS/Data Mining/homework_1/spark-on-yelp/results/task1.json"

    sc = SparkContext()

    result = dict()
    # review_RDD = sc.textFile("/Users/rhythmgirdhar/Desktop/MS-CS/Data Mining/homework_1/spark-on-yelp/data/original_yelp_dataset/yelp_academic_dataset_review.json").map(lambda x: json.loads(x))

    review_RDD = sc.textFile(input_file).map(lambda x: json.loads(x))
    #A. The total number of reviews
    result["n_review"] = review_RDD.count()

    #B. The number of reviews in 2018
    count_RDD = review_RDD.filter(lambda x: x.get("date").split("-")[0] == "2018")
    result["n_review_2018"] = count_RDD.count()

    #C. The number of distinct users who wrote reviews
    userid_RDD = review_RDD.map(getUserID)
    distinct_userid_RDD = userid_RDD.distinct()
    result["n_user"] = distinct_userid_RDD.count()

    #D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
    reduced_userid_RDD = userid_RDD.reduceByKey(lambda x,y : x + y)
    top_10_users =  reduced_userid_RDD.takeOrdered(10, key = lambda x: (-x[1], x[0]))
    final_list = []
    for row in top_10_users:  
        final_list.append(list(row))

    result["top10_user"] = final_list

    #E. The number of distinct businesses that have been reviewed 
    businessid_RDD = review_RDD.map(getBusinessID)
    distinct_businessid_RDD = businessid_RDD.distinct()
    result["n_business"] = distinct_businessid_RDD.count()

    #F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
    reduced_businessid_RDD = businessid_RDD.reduceByKey(lambda x,y : x + y)
    top_10_businesses = reduced_businessid_RDD.takeOrdered(10, key = lambda x: (-x[1], x[0]))
    final_list = []
    for row in top_10_businesses:  
        final_list.append(list(row))

    result["top10_business"] = final_list

    write_json(output_file, result)
    