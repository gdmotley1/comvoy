"""Generate professional Mechanic Body Lead Briefing & Call Sheet HTML documents."""
import json
from datetime import date

with open("mechanic_body_leads.json") as f:
    dealers = json.load(f)

today = date.today().strftime("%B %d, %Y")

# --- Fouts inventory summary ---
fouts_inventory = [
    ("2025 Kenworth T280", 13, "$212,000"),
    ("2025 Freightliner M2 106", 12, "$193,998–$226,000"),
    ("2026 Ford F-550", 7, "$178,000"),
    ("2024 Ford F-450", 4, "$122,222–$130,000"),
    ("2024 Freightliner M2 106", 2, "$214,000"),
]

# Split dealers into priority tiers
tier1 = [d for d in dealers if d["mech_count"] >= 5]  # Already stocking 5+
tier2 = [d for d in dealers if 2 <= d["mech_count"] < 5]  # Stocking some
tier3 = [d for d in dealers if d["mech_count"] == 1]  # Just started

smyrna_dealers = [d for d in dealers if d.get("has_smyrna")]


def tier_badge(tier):
    colors = {"hot": "#dc2626", "warm": "#f59e0b", "cold": "#6b7280"}
    color = colors.get(tier, "#6b7280")
    return f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;background:{color};color:#fff;font-size:11px;font-weight:600;text-transform:uppercase;">{tier}</span>'


def smyrna_badge():
    return '<span style="display:inline-block;padding:2px 8px;border-radius:4px;background:#2563eb;color:#fff;font-size:11px;font-weight:600;margin-left:4px;">SMYRNA</span>'


def phone_link(phone):
    if not phone:
        return "N/A"
    return f'<a href="tel:{phone}" style="color:#1e40af;text-decoration:none;">{phone}</a>'


def website_link(url):
    if not url:
        return ""
    short = url.replace("https://", "").replace("http://", "").split("?")[0].rstrip("/")
    if len(short) > 40:
        short = short[:40] + "..."
    return f'<a href="{url}" target="_blank" style="color:#1e40af;text-decoration:none;font-size:12px;">{short}</a>'


def body_type_bar(breakdown, total):
    """Mini horizontal bar chart of body type mix."""
    if not breakdown:
        return ""
    html = '<div style="margin-top:6px;">'
    for bt in breakdown[:5]:
        pct = (bt["count"] / total * 100) if total else 0
        is_mech = bt["name"] == "Mechanic Body"
        bg = "#2563eb" if is_mech else "#e5e7eb"
        fg = "#fff" if is_mech else "#374151"
        html += f'''<div style="display:flex;align-items:center;margin-bottom:2px;font-size:11px;">
            <div style="width:120px;text-overflow:ellipsis;overflow:hidden;white-space:nowrap;color:#6b7280;">{bt["name"]}</div>
            <div style="flex:1;background:#f3f4f6;border-radius:3px;height:14px;margin:0 6px;position:relative;">
                <div style="width:{max(pct, 2):.0f}%;background:{bg};height:100%;border-radius:3px;"></div>
            </div>
            <div style="width:30px;text-align:right;font-weight:{'700' if is_mech else '400'};color:{fg if is_mech else '#374151'};">{bt["count"]}</div>
        </div>'''
    html += "</div>"
    return html


def dealer_card(d, index):
    """Render a full dealer briefing card."""
    lot = d.get("lot_size", "?")
    tier = d.get("tier", "?")
    mech = d["mech_count"]
    phone = d.get("phone", "")
    website = d.get("website", "")
    rating = d.get("rating")
    reviews = d.get("review_count")
    smyrna = d.get("has_smyrna", False)
    smyrna_units = d.get("smyrna_units", 0)
    rank = d.get("rank")
    brands = d.get("brand_breakdown", [])
    body_types = d.get("body_type_breakdown", [])

    rating_html = ""
    if rating:
        stars = "★" * int(rating) + ("½" if rating % 1 >= 0.5 else "")
        rating_html = f'<span style="color:#f59e0b;">{stars}</span> {rating} ({reviews:,} reviews)'

    brand_tags = ""
    if brands:
        brand_tags = " ".join(
            f'<span style="display:inline-block;padding:1px 6px;border-radius:3px;background:#f3f4f6;font-size:11px;margin:1px;">{b["name"]} ({b["count"]})</span>'
            for b in brands[:4]
        )

    smyrna_html = ""
    if smyrna:
        smyrna_html = f"""
        <div style="margin-top:8px;padding:8px 12px;background:#eff6ff;border-left:3px solid #2563eb;border-radius:4px;font-size:12px;">
            <strong style="color:#1e40af;">Existing Smyrna Relationship</strong> — {smyrna_units} Smyrna unit{'s' if smyrna_units != 1 else ''} currently on lot
        </div>"""

    # Talking points
    points = []
    if mech >= 10:
        points.append(f"Already a major mechanic body dealer with {mech} units — they know the product and have the customer base. Push for additional Fouts/Warner bodies.")
    elif mech >= 5:
        points.append(f"Solid mechanic body presence ({mech} units). Room to grow — pitch Fouts/Warner as a competitive alternative or supplement.")
    elif mech >= 2:
        points.append(f"Stocking {mech} mechanic bodies — actively selling the category. Good candidate to expand with Fouts/Warner product.")

    if smyrna:
        points.append("Already carries Smyrna product — established trust with our organization. Easiest path to placing Fouts mechanic bodies.")

    if lot and lot != "?" and lot >= 100:
        points.append(f"Major operation with {lot} total vehicles — high-volume dealer with infrastructure to move units.")

    # Check if mechanic body is their top body type
    if body_types and body_types[0]["name"] == "Mechanic Body":
        points.append("Mechanic Body is their #1 body type — this is core to their business.")

    # Match chassis brands to Fouts inventory
    brand_names = [b["name"].lower() for b in brands]
    fouts_chassis = []
    if "ford" in brand_names:
        fouts_chassis.append("Ford F-550/F-450")
    if "freightliner" in brand_names:
        fouts_chassis.append("Freightliner M2 106")
    if "kenworth" in brand_names:
        fouts_chassis.append("Kenworth T280")
    if fouts_chassis:
        points.append(f"Already sells {', '.join(fouts_chassis)} chassis — direct match to Fouts inventory.")

    points_html = ""
    if points:
        points_html = '<div style="margin-top:10px;"><strong style="font-size:12px;color:#374151;">Talking Points:</strong><ul style="margin:4px 0 0 0;padding-left:18px;">'
        for p in points:
            points_html += f'<li style="font-size:12px;color:#4b5563;margin-bottom:3px;">{p}</li>'
        points_html += "</ul></div>"

    return f"""
    <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px;margin-bottom:16px;page-break-inside:avoid;background:#fff;">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div>
                <h3 style="margin:0;font-size:16px;color:#111827;">
                    {index}. {d["name"]}
                    {smyrna_badge() if smyrna else ""}
                </h3>
                <div style="color:#6b7280;font-size:13px;margin-top:2px;">{d["city"]}, {d["state"]}</div>
            </div>
            <div style="text-align:right;">
                {tier_badge(tier)}
                <div style="font-size:22px;font-weight:700;color:#1e40af;margin-top:4px;">{mech} <span style="font-size:12px;font-weight:400;color:#6b7280;">mech bodies</span></div>
            </div>
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-top:12px;padding:10px;background:#f9fafb;border-radius:6px;">
            <div>
                <div style="font-size:11px;color:#6b7280;text-transform:uppercase;">Contact</div>
                <div style="font-size:13px;font-weight:600;">{phone_link(phone)}</div>
                <div>{website_link(website)}</div>
            </div>
            <div>
                <div style="font-size:11px;color:#6b7280;text-transform:uppercase;">Lot Size / Rank</div>
                <div style="font-size:13px;font-weight:600;">{lot} vehicles{f' (#{rank} in state)' if rank else ''}</div>
                <div style="font-size:12px;color:#6b7280;">{d.get('brand_count', '?')} brands, {d.get('body_type_count', '?')} body types</div>
            </div>
            <div>
                <div style="font-size:11px;color:#6b7280;text-transform:uppercase;">Rating</div>
                <div style="font-size:13px;">{rating_html if rating_html else 'N/A'}</div>
            </div>
        </div>

        <div style="margin-top:10px;">
            <div style="font-size:11px;color:#6b7280;text-transform:uppercase;margin-bottom:4px;">Chassis Brands</div>
            {brand_tags}
        </div>

        <div style="margin-top:8px;">
            <div style="font-size:11px;color:#6b7280;text-transform:uppercase;margin-bottom:2px;">Body Type Mix</div>
            {body_type_bar(body_types, lot if lot != '?' else 0)}
        </div>

        {smyrna_html}
        {points_html}
    </div>"""


# ============================================================
# BRIEFING DOCUMENT
# ============================================================
briefing_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fouts CV Mechanic Body — Dealer Lead Briefing</title>
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: 'Inter', -apple-system, sans-serif; background: #f3f4f6; color: #111827; }}
    .container {{ max-width: 900px; margin: 0 auto; padding: 20px; }}
    @media print {{
        body {{ background: #fff; }}
        .container {{ padding: 0; }}
        .no-print {{ display: none !important; }}
    }}
</style>
</head>
<body>

<div class="container">

    <!-- HEADER -->
    <div style="background:linear-gradient(135deg,#1e3a5f 0%,#2563eb 100%);border-radius:12px;padding:32px;color:#fff;margin-bottom:24px;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <div style="font-size:13px;text-transform:uppercase;letter-spacing:1px;opacity:0.8;">Fouts Commercial Vehicles</div>
                <h1 style="font-size:28px;margin-top:4px;">Mechanic Body Dealer Leads</h1>
                <div style="font-size:14px;opacity:0.9;margin-top:4px;">Placement Strategy Briefing &mdash; {today}</div>
            </div>
            <div style="text-align:right;">
                <div style="font-size:42px;font-weight:700;">38</div>
                <div style="font-size:13px;opacity:0.8;">Units to Place</div>
            </div>
        </div>
    </div>

    <!-- INVENTORY SUMMARY -->
    <div style="background:#fff;border-radius:8px;padding:20px;margin-bottom:24px;border:1px solid #e5e7eb;">
        <h2 style="font-size:16px;color:#111827;margin-bottom:12px;">Current Fouts Mechanic Body Inventory</h2>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <thead>
                <tr style="border-bottom:2px solid #e5e7eb;">
                    <th style="text-align:left;padding:8px 12px;color:#6b7280;font-weight:600;">Chassis</th>
                    <th style="text-align:center;padding:8px 12px;color:#6b7280;font-weight:600;">Units</th>
                    <th style="text-align:right;padding:8px 12px;color:#6b7280;font-weight:600;">Price</th>
                </tr>
            </thead>
            <tbody>
                {''.join(f"""<tr style="border-bottom:1px solid #f3f4f6;">
                    <td style="padding:8px 12px;font-weight:500;">{chassis}</td>
                    <td style="padding:8px 12px;text-align:center;font-weight:700;color:#1e40af;">{count}</td>
                    <td style="padding:8px 12px;text-align:right;color:#374151;">{price}</td>
                </tr>""" for chassis, count, price in fouts_inventory)}
                <tr style="border-top:2px solid #e5e7eb;">
                    <td style="padding:8px 12px;font-weight:700;">Total</td>
                    <td style="padding:8px 12px;text-align:center;font-weight:700;color:#1e40af;">38</td>
                    <td style="padding:8px 12px;text-align:right;font-weight:600;color:#374151;">$122,222&ndash;$226,000</td>
                </tr>
            </tbody>
        </table>
        <div style="margin-top:12px;font-size:12px;color:#6b7280;">
            All units are Warner Mechanics Bodies, new condition, built at Fouts plant in Milledgeville, GA.
            Chassis: Ford, Freightliner, Kenworth. Body builder: Warner Truck Bodies.
        </div>
    </div>

    <!-- MARKET OVERVIEW -->
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:12px;margin-bottom:24px;">
        <div style="background:#fff;border-radius:8px;padding:16px;border:1px solid #e5e7eb;text-align:center;">
            <div style="font-size:28px;font-weight:700;color:#1e40af;">{len(dealers)}</div>
            <div style="font-size:12px;color:#6b7280;">Dealers w/ Mech Bodies</div>
        </div>
        <div style="background:#fff;border-radius:8px;padding:16px;border:1px solid #e5e7eb;text-align:center;">
            <div style="font-size:28px;font-weight:700;color:#dc2626;">{len(tier1)}</div>
            <div style="font-size:12px;color:#6b7280;">Stocking 5+ Units</div>
        </div>
        <div style="background:#fff;border-radius:8px;padding:16px;border:1px solid #e5e7eb;text-align:center;">
            <div style="font-size:28px;font-weight:700;color:#059669;">{len(smyrna_dealers)}</div>
            <div style="font-size:12px;color:#6b7280;">Existing Smyrna Partners</div>
        </div>
        <div style="background:#fff;border-radius:8px;padding:16px;border:1px solid #e5e7eb;text-align:center;">
            <div style="font-size:28px;font-weight:700;color:#7c3aed;">{sum(d['mech_count'] for d in dealers)}</div>
            <div style="font-size:12px;color:#6b7280;">Total Mech Bodies in Market</div>
        </div>
    </div>

    <!-- PRIORITY TIER 1 -->
    <div style="margin-bottom:8px;">
        <h2 style="font-size:18px;color:#111827;border-bottom:3px solid #dc2626;padding-bottom:8px;display:inline-block;">
            Priority 1: Heavy Mechanic Body Dealers (5+ units)
        </h2>
        <p style="font-size:13px;color:#6b7280;margin-top:4px;">
            These {len(tier1)} dealers already sell mechanic bodies at volume. They know the product, have the customer base, and can absorb additional inventory immediately.
        </p>
    </div>
    {''.join(dealer_card(d, i+1) for i, d in enumerate(tier1))}

    <!-- PRIORITY TIER 2 -->
    <div style="margin-top:32px;margin-bottom:8px;">
        <h2 style="font-size:18px;color:#111827;border-bottom:3px solid #f59e0b;padding-bottom:8px;display:inline-block;">
            Priority 2: Growing Mechanic Body Dealers (2&ndash;4 units)
        </h2>
        <p style="font-size:13px;color:#6b7280;margin-top:4px;">
            These {len(tier2)} dealers are actively selling mechanic bodies but at lower volume. Good candidates to scale up with Fouts/Warner product.
        </p>
    </div>
    {''.join(dealer_card(d, i+len(tier1)+1) for i, d in enumerate(tier2))}

    <!-- PRIORITY TIER 3 -->
    <div style="margin-top:32px;margin-bottom:8px;">
        <h2 style="font-size:18px;color:#111827;border-bottom:3px solid #6b7280;padding-bottom:8px;display:inline-block;">
            Priority 3: Entry-Level Mechanic Body Dealers (1 unit)
        </h2>
        <p style="font-size:13px;color:#6b7280;margin-top:4px;">
            These {len(tier3)} dealers have dipped a toe into mechanic bodies. May be testing the market or just got their first unit.
        </p>
    </div>
    {''.join(dealer_card(d, i+len(tier1)+len(tier2)+1) for i, d in enumerate(tier3))}

    <!-- FOOTER -->
    <div style="margin-top:32px;padding:16px;background:#f9fafb;border-radius:8px;font-size:12px;color:#6b7280;text-align:center;border:1px solid #e5e7eb;">
        Generated by Otto &mdash; Comvoy Sales Intelligence &mdash; {today}<br>
        Data source: Comvoy market snapshot (March 30, 2026) &bull; Fouts CV inventory from foutscv.com
    </div>

</div>
</body>
</html>"""

with open("fouts_mechanic_body_briefing.html", "w", encoding="utf-8") as f:
    f.write(briefing_html)

print(f"Briefing written: fouts_mechanic_body_briefing.html")
print(f"  Tier 1 (5+ units): {len(tier1)} dealers")
print(f"  Tier 2 (2-4 units): {len(tier2)} dealers")
print(f"  Tier 3 (1 unit): {len(tier3)} dealers")


# ============================================================
# CALL SHEET
# ============================================================
def call_row(d, priority):
    smyrna_flag = "&#9733;" if d.get("has_smyrna") else ""
    tier = d.get("tier", "?")
    tier_color = {"hot": "#dc2626", "warm": "#f59e0b", "cold": "#6b7280"}.get(tier, "#6b7280")
    phone = d.get("phone", "")
    phone_html = f'<a href="tel:{phone}" style="color:#1e40af;text-decoration:none;white-space:nowrap;">{phone}</a>' if phone else "N/A"

    # Chassis match
    brand_names = [b["name"].lower() for b in d.get("brand_breakdown", [])]
    chassis_match = []
    if "ford" in brand_names:
        chassis_match.append("Ford")
    if "freightliner" in brand_names:
        chassis_match.append("FL")
    if "kenworth" in brand_names:
        chassis_match.append("KW")
    chassis_html = ", ".join(chassis_match) if chassis_match else "-"

    notes = []
    if d.get("has_smyrna"):
        notes.append("Smyrna partner")
    if d["mech_count"] >= 10:
        notes.append("Heavy mech buyer")
    elif d["mech_count"] >= 5:
        notes.append("Active mech buyer")
    if d.get("lot_size", 0) >= 100:
        notes.append(f"Large lot ({d['lot_size']})")

    return f"""<tr style="border-bottom:1px solid #e5e7eb;">
        <td style="padding:6px 8px;font-size:12px;text-align:center;font-weight:600;color:#1e40af;">{priority}</td>
        <td style="padding:6px 8px;font-size:12px;font-weight:500;">{d["name"]}{f' <span style="color:#2563eb;">★</span>' if d.get("has_smyrna") else ""}</td>
        <td style="padding:6px 8px;font-size:12px;color:#6b7280;">{d["city"]}, {d["state"]}</td>
        <td style="padding:6px 8px;font-size:12px;">{phone_html}</td>
        <td style="padding:6px 8px;font-size:12px;text-align:center;"><span style="color:{tier_color};font-weight:600;">{tier.upper()}</span></td>
        <td style="padding:6px 8px;font-size:12px;text-align:center;font-weight:700;color:#1e40af;">{d["mech_count"]}</td>
        <td style="padding:6px 8px;font-size:12px;text-align:center;">{d.get("lot_size", "?")}</td>
        <td style="padding:6px 8px;font-size:11px;color:#6b7280;">{chassis_html}</td>
        <td style="padding:6px 8px;font-size:11px;color:#6b7280;">{"; ".join(notes)}</td>
        <td style="padding:6px 8px;font-size:11px;text-align:center;">&#9744;</td>
    </tr>"""


call_sheet_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fouts CV Mechanic Body — Call Sheet</title>
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: 'Inter', -apple-system, sans-serif; background: #fff; color: #111827; }}
    .container {{ max-width: 1100px; margin: 0 auto; padding: 20px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    thead th {{ position: sticky; top: 0; background: #1e3a5f; color: #fff; }}
    tr:hover {{ background: #f9fafb; }}
    @media print {{
        thead th {{ background: #1e3a5f !important; color: #fff !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
        .no-print {{ display: none !important; }}
        body {{ font-size: 11px; }}
    }}
</style>
</head>
<body>

<div class="container">

    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;padding-bottom:16px;border-bottom:3px solid #1e3a5f;">
        <div>
            <h1 style="font-size:22px;color:#1e3a5f;">Mechanic Body Placement — Call Sheet</h1>
            <div style="font-size:13px;color:#6b7280;">Fouts Commercial Vehicles &bull; {today} &bull; 38 units to place &bull; &#9733; = Smyrna partner</div>
        </div>
        <div style="text-align:right;">
            <div style="font-size:13px;color:#6b7280;">{len(dealers)} dealers</div>
            <div style="font-size:13px;color:#6b7280;">{sum(1 for d in dealers if d.get('has_smyrna'))} existing partners</div>
        </div>
    </div>

    <!-- SMYRNA PARTNERS FIRST -->
    <h2 style="font-size:14px;color:#2563eb;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;">
        &#9733; Existing Smyrna Partners — Call First
    </h2>
    <table style="margin-bottom:24px;">
        <thead>
            <tr>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">#</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Dealer</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Location</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Phone</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Tier</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Mech</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Lot</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Chassis</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Notes</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Done</th>
            </tr>
        </thead>
        <tbody>
            {''.join(call_row(d, i+1) for i, d in enumerate(sorted(smyrna_dealers, key=lambda x: x["mech_count"], reverse=True)))}
        </tbody>
    </table>

    <!-- ALL OTHER DEALERS -->
    <h2 style="font-size:14px;color:#1e3a5f;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;">
        All Mechanic Body Dealers — By Current Stock
    </h2>
    <table>
        <thead>
            <tr>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">#</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Dealer</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Location</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Phone</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Tier</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Mech</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Lot</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Chassis</th>
                <th style="padding:8px 8px;font-size:11px;text-align:left;font-weight:600;">Notes</th>
                <th style="padding:8px 8px;font-size:11px;text-align:center;font-weight:600;">Done</th>
            </tr>
        </thead>
        <tbody>
            {''.join(call_row(d, i+1) for i, d in enumerate(sorted([d for d in dealers if not d.get("has_smyrna")], key=lambda x: x["mech_count"], reverse=True)))}
        </tbody>
    </table>

    <div style="margin-top:24px;padding:12px;font-size:11px;color:#6b7280;text-align:center;border-top:1px solid #e5e7eb;">
        Generated by Otto &mdash; Comvoy Sales Intelligence &mdash; {today} &bull; Data: March 30, 2026 snapshot
    </div>

</div>
</body>
</html>"""

with open("fouts_mechanic_body_call_sheet.html", "w", encoding="utf-8") as f:
    f.write(call_sheet_html)

print(f"Call sheet written: fouts_mechanic_body_call_sheet.html")
print(f"  Smyrna partners section: {len(smyrna_dealers)} dealers")
print(f"  Other dealers: {len(dealers) - len(smyrna_dealers)} dealers")
