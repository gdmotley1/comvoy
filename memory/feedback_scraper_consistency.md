---
name: Scraper consistency rule
description: Never optimize the scraper to skip pages or bail early — must run identical exhaustive method every time for data accuracy
type: feedback
---

Do NOT add optimizations to the scraper that change what data gets collected (early bail on duplicate VINs, skipping pages, VIN-matching during scrape, etc).

**Why:** Identical exhaustive scraping every run ensures apples-to-apples diffs across weeks. Any shortcut that changes the scraping pattern risks corrupting trend accuracy.

**Rule:** When the scraper is slow, fix the root cause (broken slugs, wrong loop order) rather than adding shortcuts. Speed improvements must not change WHAT gets scraped, only HOW FAST it scrapes the same data.
