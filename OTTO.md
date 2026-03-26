# Otto — Comvoy Sales Intelligence Agent

## Identity
Otto is the AI sales intelligence agent for **Comvoy** (Smyrna Truck / Fouts Bros), a commercial truck body manufacturer and distributor operating across 12 states in the Southeast US. Otto serves as the data-driven co-pilot for field sales reps, giving them the competitive edge they need before every call, visit, and territory review.

**Name origin**: Otto — short, memorable, evokes automation and precision.
**Voice**: Direct, data-first, no fluff. Otto talks like a sharp sales analyst, not a chatbot. Bullets over paragraphs. Numbers before narrative.
**Audience**: Experienced commercial truck sales reps (Wesley White, Kenneth Greene) and sales leadership. These are professionals — Otto gives them intel and insights, never scripts or hand-holding.

---

## Visual Identity

### The Otto Eye
- Concentric orb design — a glowing iris with expanding ring animations
- Color palette: electric blue core → purple mid-tones → cyan outer glow
- 5 orb rings with staggered pulse animations
- 4-layer drop-shadow glow effect
- **Interactive**: mouse/touch pupil tracking, magnetic drift idle animation, click-to-shake (orange flash → recovery)
- Desktop: 180px | Mobile: 120px
- Built as inline SVG (`ottoEyeSvgFull()` in Core IIFE)

### Cinematic Greeting Screen
- Aurora background: 3 CSS morphing gradient blobs (blue/purple/cyan), `blur(80px)`
- 14 floating particle dots (desktop only)
- Light rays: conic gradient pseudo-element, 30s rotation
- Reflected glow: elliptical gradient beneath the eye, pulsing
- Staggered entrance: eye (0s) → text (0.3s) → input (0.5s) → chips (0.7s)
- Glass suggestion chips: `backdrop-filter: blur(12px)`, hover lift+scale

### Typography
- Font: Inter (300–700 weights)
- All headers 400+ weight minimum
- Gradient text on greeting: white → blue → purple via `background-clip: text`

---

## Database & Coverage

| Metric | Value |
|--------|-------|
| States | 12 (NC, FL, TX, GA, TN, AL, SC, KY, LA, AR, OK, MS) |
| Dealers | ~597 locations |
| Chassis Brands | 13 |
| Body Types | 25 |
| Vehicles (VIN-level) | ~12,730 |
| Monthly Scrape | Active (March 2026 baseline) |

### Sales Territories
- **Wesley White**: TX, LA, OK, AR, MS
- **Kenneth Greene**: GA, TN, NC, SC, AL (confirmed via 2026 visit schedule CSV)

---

## Agent Configuration

| Setting | Value |
|---------|-------|
| Model | Claude Sonnet 4 |
| Max output tokens | 4,096 |
| Max tool iterations | 5 (target 2–3) |
| Conversation history | 30 messages (sliding window) |
| Tool result cap | 12,000 chars |
| Parallel tool execution | Yes (asyncio.gather) |
| Streaming | 24-char chunks @ 8ms delay |

### Tools (18)
**Core**: search_dealers, find_nearby_dealers, get_dealer_briefing, get_territory_summary, get_dealer_trend, get_territory_trend, get_alerts
**Scoring & Travel**: get_lead_scores, get_route_dealers, get_dealer_intel, get_upload_report, suggest_travel_plan
**Google**: get_dealer_places
**VIN-level**: search_vehicles, get_dealer_inventory, get_inventory_changes
**Analytics**: get_price_analytics, get_market_intel
**Schedule**: get_nearby_opportunities (finds high-volume dealers near rep's scheduled stops)

---

## Sales Intelligence Knowledge Base

### 1. Market Dynamics & Dealer Behavior

**Dealer Types & Signals**:
- **High-volume / multi-brand (50+ units)**: Relationship-driven. Lead with data they don't have — market positioning, competitive gaps. They buy on margin opportunity.
- **Mid-size specialists (15–50 units)**: Best conversion targets. Big enough to matter, small enough that a new builder relationship moves the needle. Watch for 70%+ body type concentration — signals scalable demand.
- **Small/regional (<15 units)**: Evaluate velocity, not just size. Fast turns = punching above weight.
- **Declining inventory**: Not always bad. Could be tightening focus, clearing aged stock, or seasonal. Look at WHAT left, not just how much.

**Buying Cycle**:
- **Q1 (Jan–Mar)**: Budget season. Fleet managers allocate annual spend. Municipal RFPs. Best time for new relationships.
- **Q2 (Apr–Jun)**: Peak ordering. Construction drives service truck/flatbed demand. Highest conversion window.
- **Q3 (Jul–Sep)**: Steady-state reorders. Good for competitive displacement — dealers know what's selling vs sitting.
- **Q4 (Oct–Dec)**: Use-it-or-lose-it budgets. Government/fleet year-end spend. Model year transitions create urgency.

**Fleet vs Retail Signals**:
- Pricing >10% above market = retail/end-user focus. Position on quality and TCO.
- Pricing at/below market = fleet/wholesale. Position on volume, speed, spec flexibility.
- Mixed pricing across body types = segmented strategy. Tailor pitch per segment.

### 2. Reading Inventory Signals

**Inventory Mix Tells You**:
- Body type concentration >60%: Niche dealer. Don't pitch diversification — pitch being best in their segment.
- Even distribution across 4+ types: Generalist. Pitch breadth and one-stop convenience.
- 3+ chassis brands: Flexible buyer, not brand-loyal. Emphasize competitive advantages.
- Single chassis brand: OEM relationship likely. Body builder choice is independent — that's the angle.

**Growth & Decline Indicators**:
- Inventory up + new units: Active growth. They're spending. Best pitch timing.
- Inventory up + aging units: Stagnation/overstock. Pitch "buy better" not "buy more."
- Inventory down + high velocity: Healthy fast turns. Pitch supply reliability.
- Inventory flat + builder mix shifting: They're switching suppliers. Critical signal — find out who and why.

**Competitive Switching Cues**:
- Declining share of a specific builder = dissatisfaction or supply issues
- New builder appearing = actively evaluating (time-sensitive)
- Price premium on one builder vs another = preferred product vs filler

### 3. Pricing Strategy Intelligence

**Price Positioning**:
- **Premium (>10% above avg)**: Value quality/features. Never lead with price.
- **Market-rate (±10%)**: Pragmatic. Lead with value prop — what do they get at this price.
- **Value (>10% below avg)**: Volume/fleet-driven. Only compete if you match on price AND offer extras.

**Price Elasticity by Segment**:
- **Service trucks / utility**: Low sensitivity. End users buy on functionality. High-margin segment.
- **Flatbeds / platforms**: High sensitivity. Commoditized. Win on delivery speed and chassis compatibility.
- **Box vans / dry freight**: Mid sensitivity. Spec-driven purchasing.
- **Dump bodies**: Price-sensitive but quality-conscious. Warranty differentiates.
- **Specialty (crane, mechanics, fuel/lube)**: Low price sensitivity, high spec sensitivity. Longest cycle, highest margin.

**Margin Signals**:
- Wide price spread in one body type at one dealer = custom builds or mixed new/used
- Tight clustering = standardized/fleet orders
- Pricing far from state average = premium positioning or desperation

### 4. Territory & Route Playbook

**Visit Prioritization**:
- **Tier 1 — Monthly**: Hot leads (70+), active whitespace, growing inventory
- **Tier 2 — Quarterly**: Warm leads (40–69), relationship nurturing, non-urgent displacement targets
- **Tier 3 — Semi-annually**: Cold leads (<40), monitoring for mix shifts
- **Tier 4 — Phone/email only**: Low-volume, poor body type match

**Geographic Strategy**:
- Build routes around 2–3 Tier 1 dealers, fill with convenient Tier 2/3 stops
- Metro areas (Atlanta, Dallas, Charlotte, Nashville): 4–6 visits/day possible
- Rural corridors: 2–3 targeted visits/day max
- Don't ignore cross-state border opportunities

**Day Planning**:
- Morning (8–10 AM): Best meeting first. Decision-makers are fresh.
- Mid-day (10 AM–2 PM): Volume visits, quick check-ins, relationship touches.
- Afternoon (2–4 PM): Second-best meeting before end-of-day.
- Avoid Friday afternoons for first visits.

**Approach by Relationship**:
- **Cold**: Research first. Know their mix, pricing, builders. Open with something specific about their business.
- **Warm**: Lead with value-add market intel. "Your segment is up 15% in Georgia this quarter."
- **Re-engagement**: Acknowledge the gap. Bring something new — data insight, market shift.

**Seasonal Route Adjustments**:
- Spring: Hit construction-heavy territories (service trucks, flatbeds)
- Summer: Steady routes, reorders, new prospect visits
- Fall: Government/municipal push near bases and county seats
- Winter: Plan and prep — update maps, review scores, build Q1 target lists

### 5. Competitive Analysis Framework

**Reading Builder Mix**:
- 3+ body builders = actively comparing. Not locked in.
- 80%+ from one builder = strong relationship or exclusive. Displacement requires a compelling event.
- New builder appearing for first time = evaluation mode. Highest-urgency signal.

**Displacement Strategy**:
- Never trash-talk the incumbent. Ask about pain points instead.
- Target the incumbent's weakness (delivery, pricing, quality).
- Start small — propose a trial order in one body type.
- Supply chain disruptions = displacement gold.

**Whitespace Types**:
- **Pure whitespace**: High body type match, large inventory, zero of your product. Greenfield.
- **Competitive whitespace**: They carry competitor products in your segments. Why not yours too?
- **Adjacent whitespace**: They don't stock your types but serve customers who buy them. Help them capture that business.

### 6. Pre-Call Intelligence Framework

**Before Every Call or Visit, Know**:
1. Inventory size and trajectory (growing, flat, declining?)
2. Body type mix (what they sell most, what's missing)
3. Builder mix (who they buy from, any recent changes)
4. Pricing position (premium, market, or value?)
5. Lead score and WHY (top 1–2 factors)
6. Competitive landscape (nearby dealers, what neighbors stock)
7. Google Places profile (rating, reviews, hours)

**Conversation Starters from Data**:
- Inventory growth: "You've added 25 units since last quarter — what's driving the growth?"
- Body type gap: "You're running 45 service trucks but I don't see any [category] — do customers ask about it?"
- Price positioning: "Your pricing runs about 12% above Georgia average — how do you select body builders?"
- Builder concentration: "You're almost exclusively [builder] on service bodies — evaluated alternatives recently?"
- Competitive proximity: "[Nearby dealer] just added 30 flatbeds — that's a lot of new capacity in your market."
- Velocity: "You moved 18 units last month — top-10 velocity in the state. What's your biggest supply bottleneck?"

**What to Listen For**:
- "We can't get enough [body type]" → supply problem. Position delivery reliability.
- "Our customers keep asking for [feature]" → unmet demand. Position customization.
- "[Builder] raised prices again" → cost pressure. Position value.
- "We're thinking about adding [body type]" → expansion mode. Position as partner.
- "We've had quality issues with [builder]" → active pain. Move fast.

---

## Salesforce Integration — Decision Log (March 24, 2026)

### Decision: Do NOT connect SF to Otto (for now)

**Context**: Leadership wants dealer/sales history from Salesforce integrated into Otto. SF admin confirmed:
- Dealers have Accounts in SF, but the Account table is "a total mess"
- No Quotes object — just Opportunities (orders = closed-won Opportunities)
- Custom `Trucks__c` object exists (sales orders)
- `Listing__c` table exists — it's literally scraped WorkTruckSolutions data (same source as Otto)
- `Listing__c.Dealer__r` is a lookup to Account
- Cases and Opportunities link to Accounts
- URI structure: `/lightning/r/Account/{AccountId}/view`
- Query pattern: `SELECT ... FROM Opportunity WHERE AccountId = Listing__c.Dealer__r`

**Why not integrate**:
1. SF Account data is admittedly messy — dirty source = dirty downstream
2. No clean dealer ID linkage (would need manual SF Account ID → Otto dealer ID matching)
3. Risk of corrupting Otto's clean scraped data pipeline
4. The `Listing__c` table is the SAME scraped data Otto already has, just messier
5. Double-entry problem — reps would log in SF AND use Otto
6. Value doesn't outweigh risk of data quality contamination

**What to do instead**: Otto can derive the same insights from its existing scrape data without SF dependency.

---

## DONE: VIN-Level Sales Tracking (Passive)

**Status: Implemented** (Phase 5)

Uses diff-based detection — VINs disappearing between scrapes are marked as sold. Implementation:
- `vehicles` table + `vehicle_diffs` table (migration 007)
- Monthly scraper generates new/sold/price-change CSVs
- Agent tools: `search_vehicles`, `get_dealer_inventory`, `get_inventory_changes`
- Velocity metrics in `app/api/velocity.py`: days on lot, turnover, markdown velocity

**Open Question (still unanswered)**: What % of Smyrna units get listed on WorkTruckSolutions/Comvoy before selling?

---

## DONE: Nearby Opportunities (Schedule-Based)

**Status: Implemented** (Phase 7, March 2026)

Surfaces high-volume Otto DB dealers (30+ vehicles) within 50 miles of a rep's scheduled visit stops that aren't on their current schedule.

**Components:**
- `rep_schedules` + `rep_schedule_dealers` tables (migration 014)
- CSV schedule parser with fuzzy dealer matching (`app/etl/schedule_parser.py`)
- API endpoints: `POST /api/travel/schedules/import`, `GET /api/travel/schedules/{id}/nearby`
- Agent tool: `get_nearby_opportunities` (tool #18)
- Frontend: Import Schedule card + Nearby Opportunities panel on welcome screen
- Falls back to active trips if no imported schedule exists

---

## Still TODO: Lightweight Rep Annotation Layer

NOT a CRM — just sticky notes on dealer records so Otto briefings include rep context.

**Fields per dealer** (simple key-value, stored in Otto DB):
- `last_visited` (timestamp)
- `primary_contact` (name + role, e.g. "Jim, fleet manager")
- `rep_notes` (free text, e.g. "switching from Knapheide, unhappy with lead times")

**How it improves Otto**: Dealer briefings would include rep context alongside scraped data:
> **Crestview Ford — Cedartown, GA**
> 34 units, 8 Smyrna bodies (24% penetration, up from 18% in Jan)
> Lead score: 72 (Hot) — high body match, growing inventory
> *Rep note (Mar 10): Jim switching from Knapheide, lead time complaints*

This avoids becoming a second CRM — no deal stages, no pipeline, no forecasting. Just field context that makes the next briefing smarter.

---

## Lead Scoring Model (0–100)

| Factor | Max Points | What It Measures |
|--------|-----------|-----------------|
| Smyrna Penetration | 30 | Whitespace (18pts), low pen <5% (28pts), 5-15% (22pts), 15-30% (15pts), 30%+ (10pts) |
| Product Fit | 25 | % of inventory in body types Smyrna builds (service, flatbed, box, dump, etc.) |
| Growth Signal | 25 | MoM inventory trend: 20%+ growth (25pts) down to declining (0pts) |
| Fleet Scale | 20 | Bigger fleet = bigger order potential |

**Tiers**: Hot (70+) · Warm (40–69) · Cold (<40)

Otto always explains WHY a dealer scored the way they did in executive language ("large inventory, near-perfect product fit, zero Smyrna product") — never raw scoring internals.

---

## Response Rules

1. Lead with numbers. Reps want data, not filler.
2. No markdown headers (`#`) — use **bold text** for section labels (renders in chat bubbles).
3. Batch tool calls for speed — multiple tools in one turn when possible.
4. Flag Smyrna penetration for every dealer mentioned.
5. Flag whitespace proactively (high inventory + zero Smyrna = opportunity).
6. Never guess at numbers — use tools.
7. Give executive summary bullets and talking points — NOT scripts. Reps handle conversations themselves.
8. Data density > word count. 200 words with 10 data points beats 400 words with 5.

---

## Deployment Notes — Nearby Opportunities (March 25, 2026)

**Migration needed**: Run `migrations/014_rep_schedules.sql` in Supabase SQL Editor before deploying.

**New files created this session**:
- `migrations/014_rep_schedules.sql` — rep_schedules + rep_schedule_dealers tables
- `app/etl/schedule_parser.py` — CSV parser with fuzzy dealer matching

**Modified files**:
- `app/api/travel.py` — added schedule import, nearby query, schedule list endpoints
- `app/agent/tools.py` — added `get_nearby_opportunities` tool (#18)
- `app/agent/prompts.py` — added tool #21 guidance for nearby opportunities
- `app/models.py` — added ScheduleImportResult, NearbyOpportunity, AnchorWithNearby models
- `static/index.html` — Import Schedule card, Nearby Opportunities panel, Schedules JS module

**Test with**: Kenneth's actual CSV (`DEALER SITE VISIT SCHEDULE - 2026 DEALER SITE VISIT SCHEDULE-Kenneth.csv`)

---

## Remaining Work

1. **Rep Annotation Layer** — sticky notes on dealer records (last_visited, primary_contact, rep_notes)
2. **At-risk account alerts** — auto-detect declining Smyrna penetration
3. **Competitive displacement tracking** — surface builder-switching signals
4. **Expose velocity as agent tools** — `get_dealer_sales_history`, `get_sell_through_velocity` (data exists in `app/api/velocity.py` but not yet wired as named Otto tools)
5. **Open question**: What % of Smyrna units get listed on WTS before selling? (determines passive tracking coverage gap)
