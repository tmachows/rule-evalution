import json
import time
from pyspark import SparkContext
from evaluator import evaluate_entity


# load data from files to lists
data_file = open('input.txt', 'r')
data = json.load(data_file)

rules_file = open('rules.txt', 'r')
rules = json.load(rules_file)

# prepare spark context
sc = SparkContext("local", "Rule Evaluation", pyFiles=['evaluator.py', 'input.txt'])
objects = sc.parallelize(data)

current_milli_time = lambda: int(round(time.time() * 1000))
start = current_milli_time()

# run evaluation
result = [obj for obj in objects.map(evaluate_entity).toLocalIterator()]

print "Evaluation complete in: ", current_milli_time() - start, " ms"
            
# save results
with open('output.txt', 'w') as output_file:
    json.dump(result, output_file)

data_file.close()
rules_file.close()