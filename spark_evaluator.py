#!/usr/bin/python
import json
import time
import pyspark
from evaluator import evaluate_entity


# load data from files to lists

rules_file = open('rules.txt', 'r')
rules = json.load(rules_file)

# prepare spark context
sc = pyspark.SparkContext()
print "debug: ", sc.getConf().toDebugString()
objects = sc.textFile("gs://rule-evaluation-bucket/input_spark.txt")

evaluate = lambda obj: evaluate_entity(obj, rules)

# run evaluation
result = objects.map(json.loads).map(evaluate)
     
# save results
result.saveAsTextFile("gs://rule-evaluation-bucket/output/")

rules_file.close()