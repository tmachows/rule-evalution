import json


def evaluate_part(data):
    for entity in data:
        for rule in rules:
            field_path = rule['if']['field']
            operator = rule['if']['operator']
            value = rule['if']['value']

            current_value = entity
            for key in field_path.split('.'):
                current_value = current_value[key]

            if ((operator == '>' and current_value > value) or
                (operator == '=' and current_value == value) or 
                (operator == '<' and current_value < value)):

                for key in rule['then']:
                    value = rule['then'][key]
                    
                    path = entity
                    parent = None
                    for path_part in key.split('.'):
                        parent = path
                        path = path[path_part]

                    parent[key.split('.')[-1]] = value
    return data[0]


data_file = open('input.txt', 'r')
data = json.load(data_file)

rules_file = open('rules.txt', 'r')
rules = json.load(rules_file)

parts = 20
result = []
data_length = len(data)

for i in range(parts):
    start_index = i * data_length / parts
    end_index = (i + 1) * data_length / parts
    result.append(evaluate_part(data[start_index:end_index]))
            
with open('output.txt', 'w') as output_file:
    json.dump(result, output_file)

data_file.close()
rules_file.close()