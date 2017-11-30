import json

data_file = open('input.txt', 'r')
data = json.load(data_file)

rules_file = open('rules.txt', 'r')
rules = json.load(rules_file)


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
   
            
with open('output.txt', 'w') as outfile:
    json.dump(data, outfile)

data_file.close()
rules_file.close()