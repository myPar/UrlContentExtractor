import json

print('enter prompt json-file path:')
input_path = input()

with open(input_path, 'r', encoding='utf-8') as f:
    data = f.read()

json_dict = json.loads(data)

for item in json_dict:
    print(item['role'])
    print(item['content'])
    print('---')