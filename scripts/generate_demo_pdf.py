"""Generate Otto Demo PDF — clean dark theme, 2 pages, generous spacing."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor, Color
from reportlab.pdfgen import canvas
import os

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "static", "Otto_Demo.pdf")
W, H = letter  # 612 x 792

# Colors
BG = HexColor("#0a0e18")
WHITE = HexColor("#ffffff")
T85 = Color(1, 1, 1, 0.85)
T50 = Color(1, 1, 1, 0.50)
T35 = Color(1, 1, 1, 0.35)
T25 = Color(1, 1, 1, 0.25)
T10 = Color(1, 1, 1, 0.10)
T06 = Color(1, 1, 1, 0.06)
T04 = Color(1, 1, 1, 0.04)
PROMPT_BG = Color(1, 1, 1, 0.03)
ACCENT_BLUE = HexColor("#4f8fff")

LM = 56
RM = W - 56
CW = RM - LM  # content width ~500
COL_GAP = 18
COL_W = (CW - COL_GAP) / 2


def bg(c):
    c.setFillColor(BG)
    c.rect(0, 0, W, H, fill=1, stroke=0)


def top_accent_line(c):
    """Thin blue accent line at top of page like Intel version."""
    c.setFillColor(Color(0.31, 0.56, 1, 0.6))
    c.rect(0, H - 3, W, 3, fill=1, stroke=0)


def section_label(c, text, y):
    label = text.upper()
    c.setFont("Helvetica-Bold", 15)
    c.setFillColor(Color(0.31, 0.56, 1, 0.7))
    x = LM
    for ch in label:
        c.drawString(x, y, ch)
        x += c.stringWidth(ch, "Helvetica-Bold", 15) + 2.8
    return y - 36


def wrap_text(c, text, x, y, max_w, font="Helvetica", size=10, color=T35, leading=14):
    c.setFont(font, size)
    c.setFillColor(color)
    words = text.split()
    lines = []
    cur = ""
    for w in words:
        test = cur + (" " if cur else "") + w
        if c.stringWidth(test, font, size) <= max_w:
            cur = test
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    for line in lines:
        c.drawString(x, y, line)
        y -= leading
    return y


def card(c, x, y, w, h):
    c.setFillColor(T04)
    c.roundRect(x, y, w, h, 8, fill=1, stroke=0)
    c.setStrokeColor(T06)
    c.setLineWidth(0.5)
    c.roundRect(x, y, w, h, 8, fill=0, stroke=1)


def card_with_title(c, x, y, w, h, title, desc):
    card(c, x, y, w, h)
    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(WHITE)
    c.drawString(x + 20, y + h - 28, title)
    wrap_text(c, desc, x + 20, y + h - 48, w - 40, "Helvetica", 10.5, T35, 15)


# ===================== PAGE 1 =====================
def page1(c):
    bg(c)
    top_accent_line(c)
    y = H - 60

    # Title
    c.setFont("Helvetica-Bold", 42)
    c.setFillColor(WHITE)
    c.drawString(LM, y, "Otto")
    y -= 32

    y = wrap_text(c,
        "A sales intelligence tool for Comvoy. It sits on live market data across "
        "our 12-state territory and lets you ask questions in plain English. Instead "
        "of pulling reports or digging through spreadsheets, you talk to it and get answers.",
        LM, y, CW, "Helvetica", 12, T50, 18)
    y -= 30

    y = section_label(c, "What Otto Does", y)

    caps = [
        ("Chat",
         "Ask anything about dealers, territories, or market data in plain English. "
         "Otto pulls live inventory, scores leads, builds briefings, and plans trips \u2014 "
         "all through conversation."),
        ("Map",
         "Every dealer in the network plotted across 12 states. Color-coded by lead score. "
         "Click any pin for inventory, penetration, and a full briefing."),
        ("Dashboard",
         "Market share, pricing curves, lead pipeline, and Smyrna positioning \u2014 all in one view. "
         "Filter by state to drill into any territory."),
        ("Trips",
         "Build and manage multi-day rep routes. Dealers are ranked by lead score so every stop "
         "is the highest-value target in the area."),
    ]

    card_h = 90
    row_gap = 14

    for i, (title, desc) in enumerate(caps):
        col = i % 2
        row = i // 2
        cx = LM + col * (COL_W + COL_GAP)
        cy = y - (row + 1) * (card_h + row_gap) + row_gap
        card_with_title(c, cx, cy, COL_W, card_h, title, desc)

    y -= 2 * (card_h + row_gap) + 24

    # — Try It section —
    y = section_label(c, "Try It", y)

    c.setFont("Helvetica", 11)
    c.setFillColor(T50)
    c.drawString(LM, y, "Open ")
    xw = c.stringWidth("Open ", "Helvetica", 11)
    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(WHITE)
    link = "gdmotley1.github.io/comvoy"
    link_x = LM + xw
    link_w = c.stringWidth(link, "Helvetica-Bold", 11)
    c.drawString(link_x, y, link)
    c.linkURL("https://gdmotley1.github.io/comvoy/",
              (link_x, y - 3, link_x + link_w, y + 13), relative=0)
    xw += link_w
    c.setFont("Helvetica", 11)
    c.setFillColor(T50)
    c.drawString(LM + xw, y, "  and click New Chat. Paste either of these to see Otto in action.")
    y -= 26

    prompts = [
        ("Plan a rep's week",
         "Scores every dealer in range, builds a routed trip plan, and ranks stops by value.",
         "Kenneth is in Dallas this week. Build him a 3-day trip hitting the highest-value dealers in Texas.",
         "Skip the Penske stops and only show hot leads."),
        ("Prep for a call",
         "Full dealer briefing in seconds \u2014 inventory, pricing, builder mix, Smyrna penetration, and Google reviews.",
         "I have a call with Randy Marion Chevrolet in Mooresville NC tomorrow. Give me everything I need to know.",
         "They also have Randy Marion Ford nearby. Pull that briefing too so I can pitch both stores."),
    ]

    for title, desc, prompt_text, followup in prompts:
        # Measure prompt text lines
        p_font = "Helvetica-Oblique"
        p_size = 10
        p_max_w = CW - 50
        words = prompt_text.split()
        p_lines = 1
        line = ""
        for w in words:
            test = line + (" " if line else "") + w
            if c.stringWidth(test, p_font, p_size) <= p_max_w:
                line = test
            else:
                p_lines += 1
                line = w

        # Measure followup lines
        f_prefix = "Follow up:  "
        f_font = "Helvetica"
        f_size = 9
        f_text = '"' + followup + '"'
        f_first_max = CW - 50 - c.stringWidth(f_prefix, "Helvetica-Bold", f_size)
        f_rest_max = CW - 50
        f_lines = 1
        line = ""
        first = True
        for w in f_text.split():
            test = line + (" " if line else "") + w
            mw = f_first_max if first else f_rest_max
            if c.stringWidth(test, f_font, f_size) <= mw:
                line = test
            else:
                f_lines += 1
                first = False
                line = w

        # Card height — generous padding
        ch = 20 + 15 + 6 + (p_lines * 14 + 12) + 6 + (f_lines * 13) + 14

        # Draw card
        card(c, LM, y - ch, CW, ch)

        # Left accent line
        c.setFillColor(Color(0.31, 0.56, 1, 0.4))
        c.rect(LM, y - ch, 3, ch, fill=1, stroke=0)

        ty = y - 18

        # Title
        c.setFont("Helvetica-Bold", 12.5)
        c.setFillColor(WHITE)
        c.drawString(LM + 16, ty, title)
        ty -= 16

        # Desc
        c.setFont("Helvetica", 9.5)
        c.setFillColor(T35)
        c.drawString(LM + 16, ty, desc)
        ty -= 18

        # Prompt text block
        p_block_h = p_lines * 14 + 10
        c.setFillColor(PROMPT_BG)
        c.roundRect(LM + 12, ty - p_block_h + 10, CW - 24, p_block_h, 4, fill=1, stroke=0)

        c.setFont(p_font, p_size)
        c.setFillColor(Color(1, 1, 1, 0.65))
        line = ""
        for w in prompt_text.split():
            test = line + (" " if line else "") + w
            if c.stringWidth(test, p_font, p_size) <= p_max_w:
                line = test
            else:
                c.drawString(LM + 24, ty, line)
                ty -= 14
                line = w
        if line:
            c.drawString(LM + 24, ty, line)
            ty -= 14

        ty -= 8

        # Follow up
        c.setFont("Helvetica-Bold", f_size)
        c.setFillColor(T35)
        c.drawString(LM + 16, ty, f_prefix)
        fw = c.stringWidth(f_prefix, "Helvetica-Bold", f_size)
        c.setFont(f_font, f_size)
        c.setFillColor(T25)

        line = ""
        fx = LM + 16 + fw
        first = True
        for w in f_text.split():
            test = line + (" " if line else "") + w
            mw = f_first_max if first else f_rest_max
            if c.stringWidth(test, f_font, f_size) <= mw:
                line = test
            else:
                if first:
                    c.drawString(fx, ty, line)
                    first = False
                else:
                    c.drawString(LM + 16, ty, line)
                ty -= 13
                line = w
        if line:
            if first:
                c.drawString(fx, ty, line)
            else:
                c.drawString(LM + 16, ty, line)

        y -= ch + 12


# ===================== PAGE 2 =====================
def page2(c):
    bg(c)
    top_accent_line(c)
    y = H - 60

    y = section_label(c, "What the Data Unlocks Over Time", y)

    y = wrap_text(c,
        "Otto scrapes the entire Comvoy network weekly and tracks every VIN and "
        "price point. As the dataset grows, it opens up analysis that doesn't "
        "exist anywhere else in this industry:",
        LM, y, CW, "Helvetica", 12, T50, 18)
    y -= 20

    unlocks = [
        ("Sales Velocity",
         "Which body types move fastest, by state and dealer. How long units sit. "
         "Where demand outpaces supply."),
        ("Pricing Trends",
         "Price movement by segment. Which dealers are raising prices vs cutting them. "
         "Market rate shifts by quarter."),
        ("Inventory Cycles",
         "Seasonal buying patterns by state and body type. When dealers restock, draw down, "
         "and how to time outreach."),
        ("Competitive Movement",
         "Which builders are gaining or losing share. Dealer-level brand switching. "
         "Early warning signals."),
    ]

    card_h = 82
    row_gap = 14

    for i, (title, desc) in enumerate(unlocks):
        col = i % 2
        row = i // 2
        cx = LM + col * (COL_W + COL_GAP)
        cy = y - (row + 1) * (card_h + row_gap) + row_gap
        card_with_title(c, cx, cy, COL_W, card_h, title, desc)

    y -= 2 * (card_h + row_gap) + 30

    y = section_label(c, "Your Input Needed", y)

    inputs = [
        ("Metrics",
         "What KPIs matter most? What should we be measuring that we aren't today?"),
        ("Territory Structure",
         "How should we assign and prioritize territories? Fixed states, dynamic zones, account-based."),
        ("Automation",
         "What should trigger without being asked? At-risk alerts, weekly digests, CRM sync."),
        ("Competitive Depth",
         "How granular do we want to go? Builder tracking, pricing forecasts, displacement alerts."),
    ]

    card_h = 76
    for i, (title, desc) in enumerate(inputs):
        col = i % 2
        row = i // 2
        cx = LM + col * (COL_W + COL_GAP)
        cy = y - (row + 1) * (card_h + row_gap) + row_gap
        card_with_title(c, cx, cy, COL_W, card_h, title, desc)

    y -= 2 * (card_h + row_gap) + 20

    # Closing line
    c.setStrokeColor(T06)
    c.setLineWidth(0.5)
    c.line(LM, y, RM, y)
    y -= 22

    wrap_text(c,
        "The tool gets sharper the more we define what we're optimizing for. "
        "Looking forward to your direction on it.",
        LM, y, CW, "Helvetica", 11.5, T35, 17)


def main():
    out = os.path.abspath(OUTPUT)
    c = canvas.Canvas(out, pagesize=letter)
    c.setTitle("Otto")
    c.setAuthor("Comvoy")

    page1(c)
    c.showPage()
    page2(c)
    c.showPage()

    c.save()
    print(f"PDF saved to: {out}")


if __name__ == "__main__":
    main()
