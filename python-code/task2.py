__author__ = 'Rhythm Girdhar'
__email__ = 'rgirdhar@usc.edu'

import sys
import json
import pyspark
from pyspark.context import SparkContext
import time

def getBusinessID(record):
    user_id = record.get("business_id")
    return (user_id, 1)

def write_json(output_file, result):
    with open(output_file, 'w+', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    # partition_type 

    custom_partitions = int(sys.argv[3])

    # output_file = "/Users/rhythmgirdhar/Desktop/MS CS/Data Mining/homework_1/spark-on-yelp/results/task2.json"

    sc = SparkContext()

    result = dict()

    # review_RDD = sc.textFile("/Users/rhythmgirdhar/Desktop/MS CS/Data Mining/homework_1/spark-on-yelp/data/original_yelp_dataset/yelp_academic_dataset_review.json").map(lambda x: json.loads(x))
    review_RDD = sc.textFile(str(input_file)).map(lambda x:json.loads(x))

    #default partition

    default_result = dict()

    start_time = time.time()
    default_RDD = review_RDD.map(getBusinessID)
    reduced_default_RDD = default_RDD.reduceByKey(lambda x,y:x+y)
    end_time = time.time()

    default_result['n_partition'] = reduced_default_RDD.getNumPartitions()
    default_result['n_items'] = reduced_default_RDD.glom().map(len).collect()
    default_result['exe_time'] = end_time - start_time

    result['default'] = default_result

    #custom partition
    # custom_map = dict()

    #using round robin partitioning technique
    def custom_partition(key):
        return hash(key) % custom_partitions

    custom_result = dict()

    start_time = time.time()
    custom_RDD = review_RDD.map(getBusinessID).partitionBy(custom_partitions, custom_partition)
    reduced_custom_RDD = custom_RDD.reduceByKey(lambda x,y:x+y)
    end_time = time.time()
    custom_result['n_partition'] = custom_partitions
    custom_result['n_items'] = reduced_custom_RDD.glom().map(len).collect()
    custom_result['exe_time'] = end_time - start_time

    #n_partitions = custom_businessid_RDD.getNumPartitions()
    #num_each_partition = custom_businessid_RDD.glom().map(len).collect()
    # reduced_businessid_RDD = custom_businessid_RDD.reduceByKey(lambda x,y : x + y)
    # top_10_businesses = reduced_businessid_RDD.takeOrdered(10, key = lambda x: (-x[1], x[0]))
    # t2_end = time.time()


    result["customized"] = custom_result


    # output_file = "/Users/rhythmgirdhar/Desktop/MS CS/Data Mining/homework_1/spark-on-yelp/results/task2.json"
    write_json(output_file, result)