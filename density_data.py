import pandas as pd
import json

sessions = []
with open('IUM/data/raw_v2/sessions.jsonl') as f:
    for line in f:
        sessions.append(json.loads(line))


users = pd.read_json('IUM/data/raw_v2/users.jsonl', lines=True)
products = pd.read_json('IUM/data/raw_v2/products.jsonl', lines=True)


full_area = len(users) * len(products)
used_area = {""}
print(used_area)
index = 0
for session in sessions:
    if (int(sessions[index]["user_id"]), int(sessions[index]["product_id"])) not in used_area:
        used_area.add((int(sessions[index]["user_id"]), int(sessions[index]["product_id"])))
    index += 1

print(f"Used data level: {(len(used_area) - 1) / full_area}")
print(len(used_area))
