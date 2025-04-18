import json

with open('meta_Electronics.json', 'r') as f:
    for line in f:
        product = json.loads(line)
        print(product['asin'], product['title'], product.get('categories'))