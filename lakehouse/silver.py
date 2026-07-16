# lakehouse/silver.py
import json

def bronze_to_silver(bronze_path, silver_path):
    seen = set()
    with open(bronze_path) as f, open(silver_path, "w") as out:
        for line in f:
            rec = json.loads(line)
            key = rec.get("product_id")
            if key and key not in seen and rec.get("name"):  # basic quality filter
                seen.add(key)
                out.write(json.dumps(rec) + "\n")