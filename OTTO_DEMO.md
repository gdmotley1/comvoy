# Otto — Sales Intelligence for Comvoy

Otto is a conversational sales intelligence tool built on top of Comvoy's live market data. It tracks **597 dealers**, **~12,700 vehicles**, **13 chassis brands**, and **12 states** — and lets you query all of it in plain English.

Instead of digging through spreadsheets, reps ask Otto what they need and get actionable answers in seconds: lead scores, trip plans, competitive intel, pre-call briefings, territory comparisons.

---

## Try It Yourself

Go to **https://gdmotley1.github.io/comvoy/** and click **New Chat**. Here are 5 prompts to try (copy/paste any of them):

**1. Build a trip plan**
```
Kenneth is in Dallas this week. Build him a 3-day trip hitting the highest-value dealers in Texas.
```
Then try: *"Skip the Penske stops and only show hot leads."*

**2. Spot threats**
```
Who are we losing? Show me any dealers where Smyrna products disappeared.
```
Then try: *"Brief me on Cavender Grande Ford in San Antonio. They dropped Smyrna — what happened and how do we win them back?"*

**3. Competitive intel**
```
Who is winning the service truck market in Georgia and what dealers should we be targeting to take share?
```
Then try: *"Show me every dealer in North Carolina with 50+ vehicles that has zero Smyrna penetration. Rank by lead score."*

**4. Compare territories**
```
Compare Georgia vs North Carolina — which territory has more opportunity for Smyrna right now?
```
Then try: *"Build Wesley a trip through the top NC targets."*

**5. Pre-call prep**
```
I have a call with Randy Marion Chevrolet in Mooresville NC tomorrow. Give me everything I need to know.
```
Then try: *"They also have Randy Marion Ford nearby. Pull that briefing too so I can pitch both stores."*

---

## What's Under the Hood

- **Lead Scoring** — Every dealer scored 0-100 across four factors: Smyrna penetration, product fit, growth signal, fleet scale. Dealers are tagged by opportunity type (conquest, expand, defend, at-risk, whitespace).
- **Trip Planning** — Clusters high-value dealers geographically and builds multi-day routes. Iterative — you can add/remove stops, change days, shift starting points.
- **Competitive Intel** — Body builder market share, brand concentration, pricing vs market. Shows where Smyrna stands against Reading, Knapheide, Royal, etc.
- **Dealer Briefings** — Full pre-call intel: inventory mix, builder breakdown, pricing position, Google rating, phone number, opportunity type and why.
- **Threat Detection** — Flags dealers that dropped Smyrna products between scrapes. At-risk accounts get forced to hot priority.
- **Territory Dashboard** — Visual analytics tab with KPIs, market share charts, pricing curves, and lead pipeline. Filter by state.

---

## Where It's Headed

This is a work in progress. The foundation is built — data pipeline, scoring engine, agent, dashboard — but a lot depends on your strategic vision for how we want to use it:

- **What KPIs matter most?** We can track metrics we're not capturing today — velocity (days on lot), win/loss rates, seasonal patterns, rep activity.
- **How should we structure territories?** Fixed state assignments, dynamic scoring zones, account-based ownership?
- **What actions should Otto trigger?** Automated at-risk alerts, weekly digests, Salesforce integration, email campaigns?
- **What does the competitive picture need?** Deeper builder analysis, pricing intelligence, market trend forecasting?

The tool gets sharper the more we define what "winning" looks like. Looking forward to your input on where to take it.
