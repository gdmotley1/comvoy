import sys, io, csv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

with open('scrape_output/inventory_2026-03-12.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Total rows: {len(rows)}")
print(f"Columns: {list(rows[0].keys())}")
print()

for r in rows[:3]:
    for k, v in r.items():
        print(f"  {k}: {v}")
    print()

prices = [int(r['price']) for r in rows if r['price'].isdigit()]
print(f"Prices: {len(prices)} vehicles, min=${min(prices):,} max=${max(prices):,} avg=${sum(prices)//len(prices):,}")
brands = set(r['brand'] for r in rows)
print(f"Brands: {brands}")
dealers = set(r['dealer_name'] for r in rows if r['dealer_name'])
print(f"Dealers with names: {len(dealers)}")
no_dealer = sum(1 for r in rows if not r['dealer_name'])
print(f"Missing dealer name: {no_dealer}")
