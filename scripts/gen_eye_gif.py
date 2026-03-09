"""Generate animated Otto eye GIF for email header.

Creates an 80x80px animated GIF with:
  - Dark circular eye body
  - Blue/cyan iris glow with color cycling
  - Pupil that looks around in a smooth loop
  - Specular highlights
  - ~24 frames, loops forever, ~3s cycle

Output: static/otto-eye.gif
"""

import math
from PIL import Image, ImageDraw, ImageFilter

# ── Config ──
SIZE = 72           # px (smaller = lighter GIF)
CENTER = SIZE // 2  # 40
FRAMES = 18         # fewer frames = smaller file
FRAME_MS = 160      # ms per frame (~2.9s loop)
BG = (12, 12, 24)   # #0c0c18 — matches email background

# Colors from the website SVG
IRIS_BLUE = (79, 143, 255)      # #4f8fff
IRIS_CYAN = (0, 212, 255)       # #00d4ff
IRIS_PURPLE = (168, 85, 247)    # #a855f7
SHELL_DARK = (10, 14, 26)       # #0a0e1a
PUPIL_DARK = (6, 8, 16)         # #060810


def lerp_color(c1, c2, t):
    """Linear interpolate between two RGB colors."""
    return tuple(int(a + (b - a) * t) for a, b in zip(c1, c2))


def get_iris_color(frame_idx):
    """Cycle iris color: blue → cyan → purple → blue (matches site animation)."""
    t = frame_idx / FRAMES
    cycle = t * 3  # 3 color stops
    if cycle < 1:
        return lerp_color(IRIS_BLUE, IRIS_CYAN, cycle)
    elif cycle < 2:
        return lerp_color(IRIS_CYAN, IRIS_PURPLE, cycle - 1)
    else:
        return lerp_color(IRIS_PURPLE, IRIS_BLUE, cycle - 2)


def draw_eye(frame_idx):
    """Draw a single frame of the Otto eye."""
    # Work at 2x for anti-aliasing, then downscale
    S = SIZE * 3
    C = S // 2
    img = Image.new("RGB", (S, S), BG)
    draw = ImageDraw.Draw(img)

    iris_color = get_iris_color(frame_idx)

    # ── Outer glow (soft blue ring) ──
    for r in range(int(S * 0.48), int(S * 0.50)):
        alpha = 0.15
        glow_col = tuple(int(c * alpha + bg * (1 - alpha)) for c, bg in zip(iris_color, BG))
        draw.ellipse([C - r, C - r, C + r, C + r], outline=glow_col, width=1)

    # ── Outer shell (dark circle with iris-colored border) ──
    shell_r = int(S * 0.465)
    draw.ellipse(
        [C - shell_r, C - shell_r, C + shell_r, C + shell_r],
        fill=SHELL_DARK, outline=iris_color, width=max(3, S // 40)
    )

    # ── Ambient iris fill (very subtle) ──
    ambient_r = int(S * 0.45)
    for dr in range(0, ambient_r, 2):
        alpha = 0.04 * (1 - dr / ambient_r)
        col = tuple(int(c * alpha + bg * (1 - alpha)) for c, bg in zip(iris_color, SHELL_DARK))
        draw.ellipse([C - dr, C - dr, C + dr, C + dr], outline=col, width=2)

    # ── Iris ring outer (rotating, faded) ──
    iris_outer_r = int(S * 0.365)
    for dr in range(iris_outer_r - 4, iris_outer_r + 4):
        alpha = 0.18 * max(0, 1 - abs(dr - iris_outer_r) / 4)
        col = tuple(int(c * alpha + bg * (1 - alpha)) for c, bg in zip(iris_color, SHELL_DARK))
        draw.ellipse([C - dr, C - dr, C + dr, C + dr], outline=col, width=1)

    # ── Iris ring inner ──
    iris_inner_r = int(S * 0.265)
    for w in range(max(4, S // 30)):
        r = iris_inner_r + w
        alpha = 0.4 * max(0, 1 - abs(w - 2) / 3)
        col = tuple(int(c * alpha + bg * (1 - alpha)) for c, bg in zip(iris_color, SHELL_DARK))
        draw.ellipse([C - r, C - r, C + r, C + r], outline=col, width=1)

    # ── Iris glow (pulsing center) ──
    t = frame_idx / FRAMES
    pulse = 0.6 + 0.25 * math.sin(t * 2 * math.pi)  # 0.35 to 0.85
    glow_r = int(S * (0.15 + 0.03 * math.sin(t * 2 * math.pi)))
    for dr in range(glow_r, 0, -1):
        alpha = pulse * (dr / glow_r) * 0.6
        col = tuple(int(c * alpha + bg * (1 - alpha)) for c, bg in zip(iris_color, SHELL_DARK))
        draw.ellipse([C - dr, C - dr, C + dr, C + dr], fill=col)

    # ── Pupil position (looking around in a smooth figure-8) ──
    max_shift = S * 0.06  # how far pupil moves from center
    angle = t * 2 * math.pi
    # Figure-8 / lemniscate pattern for natural looking movement
    px = C + max_shift * math.sin(angle)
    py = C + max_shift * math.sin(angle * 2) * 0.6

    # ── Pupil (dark circle) ──
    pupil_r = int(S * 0.10)
    draw.ellipse(
        [px - pupil_r, py - pupil_r, px + pupil_r, py + pupil_r],
        fill=PUPIL_DARK
    )

    # ── Pupil inner glow (bright dot in center of pupil) ──
    glow_inner_r = int(S * 0.06)
    for dr in range(glow_inner_r, 0, -1):
        alpha = 0.5 * (1 - dr / glow_inner_r) ** 2
        col = tuple(int(255 * alpha + PUPIL_DARK[i] * (1 - alpha)) for i in range(3))
        draw.ellipse(
            [px - dr, py - dr, px + dr, py + dr],
            fill=col
        )

    # ── Specular highlights (follow pupil slightly) ──
    spec_offset = max_shift * 0.3
    sx = px - S * 0.06 + spec_offset * math.sin(angle) * 0.3
    sy = py - S * 0.06 + spec_offset * math.sin(angle * 2) * 0.2

    # Large specular
    spec_r1 = int(S * 0.045)
    for dr in range(spec_r1, 0, -1):
        alpha = 0.3 * (1 - dr / spec_r1) ** 1.5
        col = tuple(int(255 * alpha + SHELL_DARK[i] * (1 - alpha)) for i in range(3))
        draw.ellipse([sx - dr, sy - dr, sx + dr, sy + dr], fill=col)

    # Small specular (sharper)
    sx2 = sx + S * 0.025
    sy2 = sy + S * 0.025
    spec_r2 = int(S * 0.022)
    for dr in range(spec_r2, 0, -1):
        alpha = 0.55 * (1 - dr / spec_r2) ** 2
        col = tuple(int(255 * alpha + SHELL_DARK[i] * (1 - alpha)) for i in range(3))
        draw.ellipse([sx2 - dr, sy2 - dr, sx2 + dr, sy2 + dr], fill=col)

    # ── Subtle arc details (iris texture) ──
    arc_r = int(S * 0.30)
    rotation = frame_idx * (360 / FRAMES) * 0.5  # slow rotation
    for arc_start in [0, 180]:
        a = arc_start + rotation
        col = tuple(int(c * 0.2 + bg * 0.8) for c, bg in zip(iris_color, SHELL_DARK))
        draw.arc(
            [C - arc_r, C - arc_r, C + arc_r, C + arc_r],
            a, a + 90, fill=col, width=max(2, S // 60)
        )

    # ── Downscale with antialiasing ──
    img = img.resize((SIZE, SIZE), Image.LANCZOS)

    # ── Light gaussian blur for softness ──
    img = img.filter(ImageFilter.GaussianBlur(radius=0.4))

    return img


def main():
    frames = [draw_eye(i) for i in range(FRAMES)]

    # Convert to palette mode for smaller GIF
    out_path = r"C:\Users\motle\claude-code\comvoy\static\otto-eye.gif"
    frames[0].save(
        out_path,
        save_all=True,
        append_images=frames[1:],
        duration=FRAME_MS,
        loop=0,  # infinite loop
        optimize=True,
    )

    import os
    size_kb = os.path.getsize(out_path) / 1024
    print(f"Generated {out_path}")
    print(f"  {FRAMES} frames, {FRAME_MS}ms each ({FRAMES * FRAME_MS / 1000:.1f}s loop)")
    print(f"  {SIZE}x{SIZE}px, {size_kb:.1f} KB")


if __name__ == "__main__":
    main()
