import json
import time


def evaluate_part(data, rules):
    return [evaluate_entity(entity, rules) for entity in data]

def evaluate_entity(entity, rules):
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
    return entity


if __name__ == "main":
    data_file = open('input.txt', 'r')
    data = json.load(data_file)

    rules_file = open('rules.txt', 'r')
    rules = json.load(rules_file)

    parts = 4
    result = []
    data_length = len(data)

    current_milli_time = lambda: int(round(time.time() * 1000))

    start = current_milli_time()

    for i in range(parts):
        start_index = i * data_length / parts
        end_index = (i + 1) * data_length / parts
        result.extend(evaluate_part(data[start_index:end_index], rules))

    print "Evaluation complete in: ", current_milli_time() - start, " ms"
                
    with open('output.txt', 'w') as output_file:
        json.dump(result, output_file)

    data_file.close()
    rules_file.close()