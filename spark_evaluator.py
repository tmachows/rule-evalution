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

current_milli_time = lambda: int(round(time.time() * 1000))
evaluate = lambda obj: evaluate_entity(obj, rules)

start = current_milli_time()

# run evaluation
result = objects.map(json.loads).map(evaluate)

print "Evaluation complete in: ", current_milli_time() - start, " ms"
            
# save results
result.saveAsTextFile("gs://rule-evaluation-bucket/output/")

rules_file.close()