__author__ = 'Rhythm Girdhar'
__email__ = 'rgirdhar@usc.edu'

import sys
import json
import pyspark
from pyspark.context import SparkContext
import time


def write_txt_file(output_file, result):
    with open(output_file, 'w+', encoding='utf-8') as f:
        header = "city,stars"
        f.write(header + "\n")
        f.write('\n'.join('%s,%s' % x for x in result))

def write_json(output_file, result):
    with open(output_file, 'w+', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":

    review_input_file = sys.argv[1]
    business_input_file = sys.argv[2]
    output_file_qa = sys.argv[3]
    output_file_qb = sys.argv[4]

    #output_file_qa = "/Users/rhythmgirdhar/Desktop/MS CS/Data Mining/homework_1/spark-on-yelp/results/task3a.txt"
    #output_file_qb = "/Users/rhythmgirdhar/Desktop/MS CS/Data Mining/homework_1/spark-on-yelp/results/task3b.json"

    #review_input_file = "/Users/rhythmgirdhar/Desktop/MS CS/Data Mining/homework_1/spark-on-yelp/data/original_yelp_dataset/yelp_academic_dataset_review.json"
    #business_input_file = "/Users/rhythmgirdhar/Desktop/MS CS/Data Mining/homework_1/spark-on-yelp/data/original_yelp_dataset/yelp_academic_dataset_business.json"

    result_dict = dict()

    sc = SparkContext()

    # <====== START PYTHON ======>
    start_python = time.time()

    review_RDD = sc.textFile(review_input_file).map(lambda x: json.loads(x))
    business_RDD = sc.textFile(business_input_file).map(lambda x: json.loads(x))

    review = review_RDD.map(lambda x: (x['business_id'], x['stars']))

    business = business_RDD.map(lambda x: (x['business_id'], x['city']))

    join = business.join(review).map(lambda x: x[1])

    base_tuple = (0,0)

    sum_count = join.aggregateByKey(base_tuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                        lambda a,b: (a[0] + b[0], a[1] + b[1]))

    avg_stars = sum_count.mapValues(lambda x: x[0]/x[1]).collect()

    top_10 = sorted(avg_stars, key=lambda kv: (-kv[1], kv[0]))[:10]

    for row in top_10:
        print(row)

    end_python = time.time()
    
    # <====== END PYTHON ======>
    result_dict["m1"] = end_python - start_python

    print("Python: ", end_python - start_python)



    # <====== START SPARK ======>
    start_spark = time.time()

    review_RDD = sc.textFile(review_input_file).map(lambda x: json.loads(x))
    business_RDD = sc.textFile(business_input_file).map(lambda x: json.loads(x))

    review = review_RDD.map(lambda x: (x['business_id'], x['stars']))

    business = business_RDD.map(lambda x: (x['business_id'], x['city']))

    join = business.join(review).map(lambda x: x[1])

    base_tuple = (0,0)

    sum_count = join.aggregateByKey(base_tuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                        lambda a,b: (a[0] + b[0], a[1] + b[1]))

    avg_stars = sum_count.mapValues(lambda x: x[0]/x[1])

    avg_stars = avg_stars.sortBy(lambda x: (-x[1], x[0]))

    top_10 =  avg_stars.take(10)

    for row in top_10:
        print(row)

    end_spark = time.time()

     # <====== END SPARK ======>
    
    result_dict["m2"] = end_spark - start_spark
    print("Spark: ", end_spark - start_spark)

    result = []

    for row in avg_stars.collect():
        result.append(row)
    
    result_dict["reason"] = "I don't know"
    write_txt_file(output_file_qa, result)
    write_json(output_file_qb, result_dict)