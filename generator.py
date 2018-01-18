import json
import random


number_of_variables_in_one_object = 20
objects_number = 50000
all_variables_names = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r'
    , 's', 't', 'u', 'v', 'w', 'y', 'z']
variables = random.sample(all_variables_names, number_of_variables_in_one_object)
max_value = 5
min_value = 0


# data generation methods
def generate_data(objects_number):
    generated_data = []
    for i in range(objects_number):
        generated_data.append(generate_object())
    return generated_data


def generate_object():
    return {'data': generate_mapping()}


def generate_mapping():
    dict = {}
    for var_name in variables:
        dict[var_name] = random.randint(min_value, max_value)
    return dict


rules_number = 20
operators = ['=', '<', '>']


# rules generation methods
def generate_rules(rules_number):
    generated_rules = []
    for i in range(rules_number):
        generated_rules.append(generate_rule())
    return generated_rules


def generate_rule():
    return {'then': generate_then_mapping(), 'if': generate_if_mapping()}


def generate_if_mapping():
    dict = {}
    dict['field'] = 'data.' + variables[random.randint(0, len(variables) - 1)]
    dict['operator'] = operators[random.randint(0, len(operators) - 1)]
    dict['value'] = random.randint(min_value, max_value)
    return dict


def generate_then_mapping():
    dict = {}
    dict['data.' + variables[random.randint(0, len(variables) - 1)]] = random.randint(min_value, max_value)
    return dict


generated_data = generate_data(objects_number)	
	
with open('input.txt', 'w') as generated_data_file:
    json.dump(generated_data, generated_data_file)
	
with open('input_spark.txt', 'w') as f:
    for generated_object in generated_data:
        f.write(json.dumps(generated_object))
        f.write("\n")
	  
with open('rules.txt', 'w') as generated_rules_file:
    json.dump(generate_rules(rules_number), generated_rules_file)
