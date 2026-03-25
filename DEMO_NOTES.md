# Otto Demo Notes — March 19, 2026

## The Elevator Pitch
Otto is Comvoy's sales intelligence platform. It gives our reps instant access to the entire competitive landscape — every dealer, every vehicle, every price point — across our 12-state territory. Instead of reps spending hours on spreadsheets, they ask Otto a question and get an answer in seconds.

---

## What We're Sitting On (Data)

| Metric | Number |
|---|---|
| Vehicles tracked | ~12,700 |
| Dealers monitored | 605 |
| States covered | 12 (GA, TN, NC, SC, AL, TX, LA, OK, AR, MS, FL, VA) |
| Chassis brands | 13 (Ford, RAM, Freightliner, International, Hino, Isuzu, Kenworth, Peterbilt, etc.) |
| Body types | 12+ (service, flatbed, dump, utility, van, stake, cab & chassis, etc.) |
| Scrape cadence | Weekly — full market snapshot every 7 days |
| Data freshness | March 18, 2026 (latest scrape) |
| Baseline comparison | March 12 vs March 18 — week-over-week diffs |

### Week-over-Week Movement (March 12 → 18)
- 96% of VINs carried over (stable market)
- 557 vehicles sold/removed, 551 new listings
- 923 price changes (642 drops, 281 increases — net downward pressure)
- 15 new dealers appeared, 7 dropped off

---

## What Otto Can Do (Demo-Ready Capabilities)

### 1. Conversational Intelligence
Ask Otto anything in plain English:
- "Who are the biggest service truck dealers in Georgia?"
- "Show me dealers near Atlanta with flatbeds"
- "What's the average price of a dump truck in Texas?"
- "Which dealers have been growing their inventory?"

### 2. Lead Scoring
Every dealer gets a score (0-100) based on 4 factors:
- **Penetration** (30%) — How much of their business do we already have? Low = more runway
- **Product Fit** (25%) — Do they sell body types we build?
- **Growth Signal** (25%) — Is their inventory growing or shrinking?
- **Fleet Scale** (20%) — How big are they?

Tiers: **Hot** (70+) | **Warm** (40-69) | **Cold** (<40)

### 3. Interactive Map
- Every dealer plotted on a tactical map
- Color-coded by lead score tier (hot/warm/cold)
- Comvoy locations marked with white diamonds
- Click any dealer for instant intel

### 4. Territory Dashboard
- KPI cards (total vehicles, dealers, avg price, Comvoy market share)
- Brand and builder market share breakdowns
- Price distribution curves
- Body type mix analysis
- Comvoy vs. market pricing comparison
- Top dealers by inventory size
- Filter by any of the 12 states

### 5. Dealer Briefings
Full dossier on any dealer:
- Inventory size, mix, and trajectory
- Pricing position vs. market
- Chassis brands and body types they carry
- Google reviews, ratings, hours
- Competitive intel — who else supplies them

### 6. Pricing Intelligence
- Market-wide price analytics by body type, brand, state
- Where Comvoy sits vs. the market average
- Price change tracking (who's dropping, who's raising)
- $5K floor filters out junk/placeholder listings

### 7. Route Planning & Trip Manager
- "Plan a 3-day trip through North Carolina"
- Optimizes dealer visits by proximity and lead score
- Built-in trip management UI

### 8. Inventory Tracking (VIN-Level)
- Search specific vehicles by VIN, body type, brand, price range
- See what's new, what sold, what changed price
- Dealer-level inventory drill-down

---

## Questions You Might Get

**"Where does the data come from?"**
Public commercial vehicle listings — we scrape the market weekly and track every VIN.

**"How accurate is it?"**
We track 12,700+ vehicles with 96% week-over-week continuity. Pricing comes directly from dealer listings.

**"What makes this different from just searching online?"**
Otto aggregates the entire market into one view. A rep can't manually check 605 dealers — Otto does it automatically and scores them.

**"Can reps use this on their phones?"**
Yes, the interface is mobile-responsive — map, chat, and dashboard all work on mobile.

**"How does lead scoring work?"**
Four factors: how much runway we have with them (penetration), whether they sell what we build (product fit), whether they're growing (growth signal), and how big they are (fleet scale). No subjective judgment — pure data.

**"What's the ROI?"**
Time savings: reps no longer spend hours researching dealers manually. Better targeting: the scoring system surfaces high-potential dealers they might never have found. Price intelligence: know exactly where the market is before quoting.

**"What's next?"**
- **VIN-level sales tracking** — passive sale detection via VIN disappearance between scrapes (no manual entry)
- **Sell-through velocity** — days on lot, sell-through rate, revenue estimates per dealer
- **Rep annotation layer** — lightweight notes (last visited, contact name, context) on dealer records for smarter briefings
- **At-risk account alerts** — auto-detect when Smyrna penetration drops at a dealer
- **Competitive displacement tracking** — surface when dealers are switching builders
- ~~Salesforce integration~~ — **deferred** (March 2026 decision: SF Account data too messy, risk outweighs value, Otto can derive same insights from scrape data)

---

## Demo Flow Suggestion

1. **Start on the greeting screen** — show Otto's personality
2. **Ask a natural question** — "Who are the top flatbed dealers in Georgia?"
3. **Switch to the map** — show the tactical view, click a hot dealer
4. **Open the dashboard** — filter by a state, walk through the panels
5. **Ask for a briefing** — "Give me a briefing on [dealer name]"
6. **Show pricing** — "What's the average price of service trucks in Tennessee?"
7. **Lead scores** — "Show me hot leads in Texas"
8. **Trip planning** — "Plan a route from Atlanta hitting the best dealers"

Keep it conversational. Otto shines when you just talk to it like a colleague.
