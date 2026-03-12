"""
Comvoy Inventory Scraper v2 — JSON-LD + VIN Dedup
===================================================
Scrapes individual vehicle data from Comvoy.com search pages using
structured JSON-LD (no detail page hits needed).

Per vehicle: VIN, price, brand, model, body type, body builder,
condition, transmission, fuel, color, dealer, city, state, image URL,
listing URL, listing ID.

Modes:
  --full       Full scrape of all combos (default on first run)
  --diff       Full scrape + diff against previous inventory
  --state XX   Scrape only one state (for testing)
  --brand XX   Scrape only one brand (for testing)

Output (in scrape_output/):
  inventory_YYYY-MM-DD.csv        Full current inventory
  new_vehicles_YYYY-MM-DD.csv     VINs not in previous scrape
  sold_vehicles_YYYY-MM-DD.csv    VINs in previous but not current
  price_changes_YYYY-MM-DD.csv    Same VIN, different price
  scrape_report_YYYY-MM-DD.txt    Summary stats

Resume-capable: saves progress per combo, skips completed on restart.
"""

import sys
import io
import os
import re
import csv
import json
import time
import math
import argparse
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from collections import OrderedDict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ── Configuration ─────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

DELAY = 1.0            # seconds between requests
PER_PAGE = 30          # Comvoy results per page
MAX_EMPTY = 3          # consecutive empty JSON-LD pages before stopping
REQUEST_TIMEOUT = 30
PROGRESS_SAVE_EVERY = 10  # save progress every N combos

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, '..', 'scrape_output')

# ── Territory ─────────────────────────────────────────────────────────────────

STATES = ['AL', 'AR', 'FL', 'GA', 'KY', 'LA', 'MS', 'NC', 'OK', 'SC', 'TN', 'TX']

BRANDS = {
    'Ford': 'Ford',
    'Chevrolet': 'Chevrolet',
    'GMC': 'GMC',
    'Ram': 'Ram',
    'Freightliner': 'Freightliner',
    'Western+Star': 'Western Star',
    'Peterbilt': 'Peterbilt',
    'Kenworth': 'Kenworth',
    'International': 'International',
    'Volvo': 'Volvo',
    'Mack': 'Mack',
    'Isuzu': 'Isuzu',
    'Hino': 'Hino',
}

BODY_TYPES = {
    'box-trucks-1al0': 'Box Trucks',
    'box-vans-18fs': 'Box Vans',
    'bucket-trucks-for-sale-464k': 'Bucket Trucks',
    'chipper-trucks-17cg': 'Chipper Trucks',
    'combo-trucks-for-sale-17k0': 'Combo Trucks',
    'contractor-trucks-17or': 'Contractor Trucks',
    'crane-trucks-189l': 'Crane Trucks',
    'cutaway-vans-for-sale-18eh': 'Cutaways',
    'landscape-trucks-1a96': 'Dovetail Landscapes',
    'dump-trucks-1das': 'Dump Trucks',
    'enclosed-service-trucks-5jzo': 'Enclosed Service',
    'flatbed-dump-trucks-for-sale-5fvx': 'Flatbed Dump',
    'flatbed-trucks-1fcd': 'Flatbed Trucks',
    'hauler-trucks-for-sale-1dpb': 'Hauler Body',
    'hooklift-trucks-1dwy': 'Hooklift',
    'landscape-dump-trucks-1e7a': 'Landscape Dumps',
    'mechanic-trucks-for-sale-1eww': 'Mechanic Body',
    'refrigerated-trucks-1gta': 'Refrigerated',
    'roll-off-trucks-1hee': 'Roll-Off',
    'rollback-trucks-1hgc': 'Rollback',
    'service-trucks-for-sale-1hmi': 'Service Trucks',
    'stake-bed-trucks-1lk7': 'Stake Beds',
    'utility-vans-for-sale-1jlo': 'Service Utility Vans',
    'welding-trucks-1n67': 'Welder',
    'tow-trucks-1nbp': 'Wrecker',
}

CSV_FIELDS = [
    'vin', 'listing_id', 'condition', 'year', 'brand', 'model',
    'body_type', 'body_builder', 'price', 'transmission', 'fuel_type',
    'color', 'dealer_name', 'city', 'state', 'listing_url', 'image_url',
    'scrape_date',
]


# ── Parsing ───────────────────────────────────────────────────────────────────

def extract_vehicles_from_jsonld(soup, body_type_name, state):
    """Extract vehicle records from JSON-LD on a search results page."""
    vehicles = []

    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(data, dict):
            continue

        main_entity = data.get('mainEntity')
        if not isinstance(main_entity, dict):
            continue

        items = main_entity.get('itemListElement', [])
        if not items:
            continue

        # Could be a catalog wrapper or direct list
        vehicle_list = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get('type') == 'OfferCatalog' or item.get('@type') == 'OfferCatalog':
                vehicle_list.extend(item.get('itemListElement', []))
            elif item.get('type') == 'Vehicle' or item.get('@type') == 'Vehicle':
                vehicle_list.append(item)

        for v in vehicle_list:
            if not isinstance(v, dict):
                continue
            vtype = v.get('type', v.get('@type', ''))
            if vtype != 'Vehicle':
                continue

            vin = v.get('vehicleIdentificationNumber', '').strip()
            if not vin or len(vin) != 17:
                continue

            # Extract listing ID from URL
            url = v.get('url', '')
            listing_id = ''
            url_match = re.search(r'-(\d{5,})$', url)
            if url_match:
                listing_id = url_match.group(1)

            # Parse condition + year from name (e.g. "New 2026 Ford F-250")
            name = v.get('name', '')
            condition = 'New' if name.lower().startswith('new') else 'Used'
            year_match = re.search(r'(\d{4})', name)
            year = year_match.group(1) if year_match else ''

            # Extract dealer + location from URL path
            # /work-truck/winder-ga/new-2026-ford-...
            dealer_name, city = '', ''
            dealer_state = state
            loc_match = re.search(r'/work-truck/([^/]+)-([a-z]{2})/', url)
            if loc_match:
                city = loc_match.group(1).replace('-', ' ').title()
                dealer_state = loc_match.group(2).upper()

            # Offers
            offers = v.get('offers', {})
            price = offers.get('price', '')

            # Brand / model
            brand_obj = v.get('brand', {})
            brand = brand_obj.get('name', '') if isinstance(brand_obj, dict) else ''
            model_obj = v.get('model', {})
            model = model_obj.get('name', '') if isinstance(model_obj, dict) else ''

            # Body builder
            mfr = v.get('manufacturer', {})
            body_builder = mfr.get('name', '') if isinstance(mfr, dict) else ''

            vehicles.append({
                'vin': vin,
                'listing_id': listing_id,
                'condition': condition,
                'year': year,
                'brand': brand,
                'model': model,
                'body_type': body_type_name,
                'body_builder': body_builder,
                'price': price,
                'transmission': v.get('vehicleTransmission', ''),
                'fuel_type': v.get('fuelType', ''),
                'color': v.get('color', ''),
                'dealer_name': '',  # filled from HTML
                'city': city,
                'state': dealer_state,
                'listing_url': url,
                'image_url': v.get('image', ''),
            })

    return vehicles


def extract_dealers_from_html(soup):
    """
    Extract dealer entries from HTML — one per vehicle card (1:1 with JSON-LD).
    Uses the same proven approach as the original scrape_all_brands.py scraper.
    Returns list of {'dealer_name', 'city', 'state'} in page order.
    """
    dealers = []
    headings = soup.find_all('div', class_=re.compile(r'category-heading'))

    for heading in headings:
        dealer_name = heading.get('title', '').strip() or heading.get_text(strip=True)
        if not dealer_name or dealer_name.lower() == 'key features':
            continue

        parent = heading.parent
        if not parent:
            continue

        location_div = parent.find('div', class_=re.compile(r'category-content'))
        if not location_div:
            gp = parent.parent
            if gp:
                location_div = gp.find('div', class_=re.compile(r'category-content'))

        location = ''
        if location_div:
            location = location_div.get('title', '').strip() or location_div.get_text(strip=True)

        if not re.match(r'^.+,\s*[A-Z]{2}$', location):
            continue

        parts = location.rsplit(',', 1)
        city, state = parts[0].strip(), parts[1].strip()
        dealers.append({'dealer_name': dealer_name, 'city': city, 'state': state})

    return dealers


def merge_dealers_into_vehicles(dealers, vehicles):
    """
    Positional zip: HTML dealer entries and JSON-LD vehicles appear in
    the same order on the page (1:1). Zip them to assign dealer_name,
    city, and state from the proven HTML source.
    """
    for i, v in enumerate(vehicles):
        if i < len(dealers):
            v['dealer_name'] = dealers[i]['dealer_name']
            v['city'] = dealers[i]['city']
            v['state'] = dealers[i]['state']

    return vehicles


def get_total_results(soup):
    """Extract total result count from page."""
    text = soup.get_text()
    match = re.search(r'([\d,]+)\s+results?', text)
    if match:
        return int(match.group(1).replace(',', ''))
    return 0


# ── Scraping ──────────────────────────────────────────────────────────────────

def scrape_combo(session, brand_param, body_slug, body_name, state):
    """Scrape all pages for one brand+body+state combo. Returns list of vehicle dicts."""
    base_url = f"https://www.comvoy.com/vehicles/{body_slug}?make={brand_param}&state={state}"

    try:
        resp = session.get(base_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        return [], 0, str(e)

    soup = BeautifulSoup(resp.text, 'html.parser')
    total_results = get_total_results(soup)

    if total_results == 0:
        return [], 0, None

    total_pages = math.ceil(total_results / PER_PAGE)

    # Page 1
    vehicles = extract_vehicles_from_jsonld(soup, body_name, state)
    dealers = extract_dealers_from_html(soup)
    merge_dealers_into_vehicles(dealers, vehicles)

    # Remaining pages
    consecutive_empty = 0
    for page in range(2, total_pages + 1):
        time.sleep(DELAY)
        url = f"{base_url}&page={page}"
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        page_vehicles = extract_vehicles_from_jsonld(soup, body_name, state)

        if not page_vehicles:
            consecutive_empty += 1
            if consecutive_empty >= MAX_EMPTY:
                break
        else:
            consecutive_empty = 0
            page_dealers = extract_dealers_from_html(soup)
            merge_dealers_into_vehicles(page_dealers, page_vehicles)
            vehicles.extend(page_vehicles)

    return vehicles, total_results, None


# ── Progress ──────────────────────────────────────────────────────────────────

def progress_path():
    return os.path.join(OUTPUT_DIR, 'scrape_progress.json')


def load_progress():
    p = progress_path()
    if os.path.exists(p):
        with open(p, 'r') as f:
            data = json.load(f)
            return set(data.get('completed', [])), data.get('vehicles_file', '')
    return set(), ''


def save_progress(completed, vehicles_file):
    with open(progress_path(), 'w') as f:
        json.dump({
            'completed': list(completed),
            'vehicles_file': vehicles_file,
            'last_updated': datetime.now().isoformat(),
        }, f)


def clear_progress():
    p = progress_path()
    if os.path.exists(p):
        os.remove(p)


def combo_key(brand_param, body_slug, state):
    return f"{brand_param}|{body_slug}|{state}"


# ── Diff ──────────────────────────────────────────────────────────────────────

def load_inventory_csv(path):
    """Load an inventory CSV into a dict keyed by VIN."""
    inventory = OrderedDict()
    if not os.path.exists(path):
        return inventory
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            vin = row.get('vin', '').strip()
            if vin:
                inventory[vin] = row
    return inventory


def find_previous_inventory():
    """Find the most recent inventory CSV before today."""
    today = datetime.now().strftime('%Y-%m-%d')
    pattern = re.compile(r'inventory_(\d{4}-\d{2}-\d{2})\.csv')
    candidates = []

    if not os.path.exists(OUTPUT_DIR):
        return None

    for fname in os.listdir(OUTPUT_DIR):
        m = pattern.match(fname)
        if m and m.group(1) != today:
            candidates.append((m.group(1), os.path.join(OUTPUT_DIR, fname)))

    if not candidates:
        return None

    candidates.sort(reverse=True)
    return candidates[0][1]


def diff_inventories(current_path, previous_path, date_str):
    """Compare two inventory CSVs. Write new, sold, price-changed files."""
    current = load_inventory_csv(current_path)
    previous = load_inventory_csv(previous_path)

    current_vins = set(current.keys())
    previous_vins = set(previous.keys())

    new_vins = current_vins - previous_vins
    sold_vins = previous_vins - current_vins
    common_vins = current_vins & previous_vins

    # Price changes
    price_changes = []
    for vin in common_vins:
        old_price = previous[vin].get('price', '')
        new_price = current[vin].get('price', '')
        if old_price and new_price and old_price != new_price:
            row = dict(current[vin])
            row['old_price'] = old_price
            row['new_price'] = new_price
            try:
                row['price_diff'] = str(int(new_price) - int(old_price))
            except ValueError:
                row['price_diff'] = ''
            price_changes.append(row)

    # Write new vehicles
    new_path = os.path.join(OUTPUT_DIR, f'new_vehicles_{date_str}.csv')
    with open(new_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for vin in sorted(new_vins):
            writer.writerow(current[vin])

    # Write sold vehicles
    sold_path = os.path.join(OUTPUT_DIR, f'sold_vehicles_{date_str}.csv')
    with open(sold_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for vin in sorted(sold_vins):
            writer.writerow(previous[vin])

    # Write price changes
    pc_fields = CSV_FIELDS + ['old_price', 'new_price', 'price_diff']
    pc_path = os.path.join(OUTPUT_DIR, f'price_changes_{date_str}.csv')
    with open(pc_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=pc_fields)
        writer.writeheader()
        for row in price_changes:
            writer.writerow(row)

    return {
        'new': len(new_vins),
        'sold': len(sold_vins),
        'price_changes': len(price_changes),
        'retained': len(common_vins) - len(price_changes),
        'new_path': new_path,
        'sold_path': sold_path,
        'pc_path': pc_path,
    }


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg, log_f=None):
    ts = datetime.now().strftime('%H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if log_f:
        log_f.write(line + '\n')
        log_f.flush()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Comvoy Inventory Scraper v2')
    parser.add_argument('--state', help='Scrape only this state (e.g. GA)')
    parser.add_argument('--brand', help='Scrape only this brand URL param (e.g. Ford)')
    parser.add_argument('--body', help='Scrape only this body slug')
    parser.add_argument('--no-diff', action='store_true', help='Skip diff even if previous exists')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted scrape')
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    date_str = datetime.now().strftime('%Y-%m-%d')
    inventory_path = os.path.join(OUTPUT_DIR, f'inventory_{date_str}.csv')
    log_path = os.path.join(OUTPUT_DIR, f'scrape_log_{date_str}.txt')

    # Filter targets
    states = [args.state.upper()] if args.state else STATES
    brands = {args.brand: BRANDS.get(args.brand, args.brand)} if args.brand else BRANDS
    body_types = {args.body: BODY_TYPES.get(args.body, args.body)} if args.body else BODY_TYPES

    total_combos = len(brands) * len(body_types) * len(states)

    # Resume support
    completed = set()
    if args.resume:
        completed, prev_file = load_progress()
        if prev_file and prev_file == inventory_path:
            print(f"Resuming: {len(completed)} combos already done")
        else:
            completed = set()

    log_f = open(log_path, 'a', encoding='utf-8')
    log(f"{'='*70}", log_f)
    log(f"COMVOY INVENTORY SCRAPER v2 — {date_str}", log_f)
    log(f"{'='*70}", log_f)
    log(f"Brands: {len(brands)} | Bodies: {len(body_types)} | States: {len(states)}", log_f)
    log(f"Total combos: {total_combos} | Resuming: {len(completed)} done", log_f)
    log(f"Output: {inventory_path}", log_f)
    log(f"{'='*70}", log_f)

    # Master vehicle dict keyed by VIN (dedup across combos)
    all_vehicles = OrderedDict()

    # If resuming, load already-written vehicles
    if completed and os.path.exists(inventory_path):
        all_vehicles = load_inventory_csv(inventory_path)
        log(f"Loaded {len(all_vehicles)} vehicles from partial scrape", log_f)

    session = requests.Session()
    session.headers.update(HEADERS)

    combo_num = 0
    scraped_this_run = 0
    errors = 0
    start_time = time.time()

    for brand_param, brand_name in brands.items():
        for body_slug, body_name in body_types.items():
            for state in states:
                combo_num += 1
                key = combo_key(brand_param, body_slug, state)

                if key in completed:
                    continue

                scraped_this_run += 1
                remaining = total_combos - len(completed) - scraped_this_run
                elapsed = time.time() - start_time
                rate = scraped_this_run / elapsed * 60 if elapsed > 1 else 0
                eta = remaining / rate if rate > 0 else 0

                log(f"[{combo_num}/{total_combos}] {brand_name} | {body_name} | {state}  "
                    f"({len(all_vehicles)} vehicles, {rate:.1f}/min, ETA {eta:.0f}m)", log_f)

                vehicles, total_results, error = scrape_combo(
                    session, brand_param, body_slug, body_name, state
                )

                if error:
                    log(f"  ERROR: {error}", log_f)
                    errors += 1
                    time.sleep(DELAY)
                    continue

                # Merge into master dict (VIN dedup)
                new_count = 0
                for v in vehicles:
                    vin = v['vin']
                    if vin not in all_vehicles:
                        v['scrape_date'] = date_str
                        all_vehicles[vin] = v
                        new_count += 1

                if vehicles:
                    log(f"  -> {len(vehicles)} on page, {new_count} new unique "
                        f"(Comvoy: {total_results} results)", log_f)

                completed.add(key)

                # Periodic save
                if scraped_this_run % PROGRESS_SAVE_EVERY == 0:
                    save_progress(completed, inventory_path)
                    # Write current inventory to disk
                    _write_inventory(inventory_path, all_vehicles)

                time.sleep(DELAY)

    # Final write
    _write_inventory(inventory_path, all_vehicles)
    clear_progress()

    elapsed_total = time.time() - start_time

    log(f"\n{'='*70}", log_f)
    log(f"SCRAPE COMPLETE", log_f)
    log(f"{'='*70}", log_f)
    log(f"Total unique vehicles: {len(all_vehicles)}", log_f)
    log(f"Combos scraped: {scraped_this_run} | Errors: {errors}", log_f)
    log(f"Time: {elapsed_total/60:.1f} minutes", log_f)
    log(f"Output: {inventory_path}", log_f)

    # Diff against previous
    if not args.no_diff:
        prev_path = find_previous_inventory()
        if prev_path:
            log(f"\nDiffing against: {os.path.basename(prev_path)}", log_f)
            diff = diff_inventories(inventory_path, prev_path, date_str)
            log(f"  New vehicles:    {diff['new']}", log_f)
            log(f"  Sold vehicles:   {diff['sold']}", log_f)
            log(f"  Price changes:   {diff['price_changes']}", log_f)
            log(f"  Retained same:   {diff['retained']}", log_f)
        else:
            log(f"\nNo previous inventory found — skipping diff", log_f)

    log(f"{'='*70}", log_f)
    log_f.close()

    # Print summary to console
    print(f"\n✓ {len(all_vehicles)} vehicles saved to {inventory_path}")


def _write_inventory(path, vehicles_dict):
    """Write the master vehicle dict to CSV."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
        writer.writeheader()
        for v in vehicles_dict.values():
            writer.writerow(v)


if __name__ == '__main__':
    main()
