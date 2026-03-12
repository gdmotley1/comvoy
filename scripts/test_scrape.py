"""Quick test: scrape one combo and verify dealer matching."""
import sys, io, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrape_inventory import *
import requests

session = requests.Session()
session.headers.update(HEADERS)

vehicles, total, err = scrape_combo(session, 'Ford', 'service-trucks-for-sale-1hmi', 'Service Trucks', 'GA')
print(f'Total results: {total}, Vehicles: {len(vehicles)}, Error: {err}')
print()

no_dealer = [v for v in vehicles if not v['dealer_name']]
print(f'Missing dealer: {len(no_dealer)} / {len(vehicles)}')
print()

for v in vehicles[:8]:
    price = v['price'] or 'N/A'
    print(f"  {v['vin'][:10]}... | {v['dealer_name']:30s} | {v['city']}, {v['state']} | ${price}")
