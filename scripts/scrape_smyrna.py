"""Scrape full Smyrna Truck inventory from worktrucksolutions.com"""
import sys, io, requests, json, re, time, csv
from collections import Counter
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

all_vins = set()
all_vehicles = []

for page in range(1, 20):
    url = f'https://smyrnatruck.worktrucksolutions.com/vehicles?page={page}'
    resp = requests.get(url, headers=HEADERS, timeout=30)
    ld_scripts = re.findall(r'<script[^>]*application/ld.json[^>]*>(.*?)</script>', resp.text, re.DOTALL)
    page_count = 0
    for s in ld_scripts:
        s = s.strip().rstrip(';')
        try:
            d = json.loads(s)
        except:
            continue
        if not isinstance(d, dict):
            continue
        tp = d.get('type', d.get('@type', ''))
        if tp == 'SearchResultsPage':
            pass
        else:
            continue
        me = d.get('mainEntity', {})
        if not isinstance(me, dict):
            continue
        for item in me.get('itemListElement', []):
            if not isinstance(item, dict):
                continue
            for v in item.get('itemListElement', []):
                if not isinstance(v, dict):
                    continue
                vin = v.get('vehicleIdentificationNumber', '')
                if vin and vin not in all_vins:
                    all_vins.add(vin)
                    brand = v.get('brand', {})
                    model = v.get('model', {})
                    mfr = v.get('manufacturer', {})
                    offers = v.get('offers', {})
                    all_vehicles.append({
                        'vin': vin,
                        'name': v.get('name', ''),
                        'price': offers.get('price', '') if isinstance(offers, dict) else '',
                        'brand': brand.get('name', '') if isinstance(brand, dict) else '',
                        'model': model.get('name', '') if isinstance(model, dict) else '',
                        'builder': mfr.get('name', '') if isinstance(mfr, dict) else '',
                    })
                    page_count += 1
    print(f'Page {page}: {page_count} new (total: {len(all_vehicles)})')
    if page_count == 0:
        break
    time.sleep(0.5)

print(f'\nTotal: {len(all_vehicles)} unique vehicles')
print(f'\nBy builder:')
for b, c in Counter(v['builder'] for v in all_vehicles).most_common():
    print(f'  {b or "(empty)"}: {c}')

out = 'C:/Users/motle/claude-code/comvoy/scrape_output/smyrna_inventory_full.csv'
with open(out, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=['vin','name','price','brand','model','builder'])
    w.writeheader()
    w.writerows(all_vehicles)
print(f'\nSaved: {out}')

print(f'\nAll VINs:')
for v in sorted(all_vehicles, key=lambda x: x['name']):
    print(f'  {v["vin"]} | {v["name"]} | builder={v["builder"]}')
