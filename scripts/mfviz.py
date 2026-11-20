"""Shared look for every generated image: palette, fonts and small drawing helpers.

Fonts: Inter and JetBrains Mono (SIL Open Font License 1.1), downloaded once from the
google/fonts repository into ~/.cache/mf-fonts. They are not committed to this repository.
"""

from __future__ import annotations

import urllib.request
from pathlib import Path

from PIL import ImageFont

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
IMG = DOCS / "images"
RESULTS = DOCS / "results"
SHOTS = DOCS / "screenshots"

BG = (13, 17, 28)
BG2 = (28, 18, 48)
PANEL = (24, 30, 46)
LINE = (52, 62, 86)
WHITE = (240, 244, 250)
GREY = (154, 164, 178)
ICE = (79, 195, 247)
ORANGE = (255, 122, 69)
GREEN = (61, 220, 151)
PURPLE = (167, 139, 250)
YELLOW = (250, 204, 21)
RED = (248, 81, 73)

HEX = {k: "#%02x%02x%02x" % v for k, v in dict(BG=BG, PANEL=PANEL, LINE=LINE, WHITE=WHITE, GREY=GREY, ICE=ICE,
                                                ORANGE=ORANGE, GREEN=GREEN, PURPLE=PURPLE, YELLOW=YELLOW,
                                                RED=RED).items()}

_FONTS = {
    "Inter.ttf": "https://github.com/google/fonts/raw/main/ofl/inter/Inter%5Bopsz,wght%5D.ttf",
    "JetBrainsMono.ttf": "https://github.com/google/fonts/raw/main/ofl/jetbrainsmono/JetBrainsMono%5Bwght%5D.ttf",
}
_CACHE = Path.home() / ".cache" / "mf-fonts"


def _font_path(name: str) -> Path:
    p = _CACHE / name
    if not p.exists():
        _CACHE.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(_FONTS[name], p)
    return p


def font(size: int, weight: int = 400, mono: bool = False) -> ImageFont.FreeTypeFont:
    f = ImageFont.truetype(str(_font_path("JetBrainsMono.ttf" if mono else "Inter.ttf")), size)
    try:
        axes = f.get_variation_axes()
        f.set_variation_by_axes([min(max(size, 14), 32), weight] if len(axes) == 2 else [weight])
    except Exception:  # noqa: BLE001 - static font fallback
        pass
    return f


def mpl_setup():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    for name in _FONTS:
        font_manager.fontManager.addfont(str(_font_path(name)))
    plt.rcParams.update({
        "font.family": "Inter", "font.size": 11, "axes.facecolor": HEX["PANEL"], "figure.facecolor": HEX["BG"],
        "axes.edgecolor": HEX["LINE"], "axes.labelcolor": HEX["WHITE"], "text.color": HEX["WHITE"],
        "xtick.color": HEX["GREY"], "ytick.color": HEX["GREY"], "grid.color": HEX["LINE"], "axes.grid": True,
        "grid.alpha": 0.6, "axes.titlesize": 14, "legend.frameon": False,
        "savefig.dpi": 160, "savefig.bbox": "tight", "savefig.facecolor": HEX["BG"],
    })
    return plt
