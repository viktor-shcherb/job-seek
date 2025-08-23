# logo_postprocess.py
# Ready-to-paste module for crisp logos in Streamlit.
# - SVGs stay vectors (injects white outline + padding; saved to disk; returns file path)
# - Rasters (PNG/WebP/JPEG/ICO/…) are trimmed→padded→haloed→resized ONCE to display_px*dpr,
#   saved to disk (PNG), and the file path is returned.
# - Streamlit should render at native CSS width (e.g., st.image(path, width=display_px)).

from __future__ import annotations

import base64
import gzip
import io
import os
import re
import hashlib
import tempfile
from pathlib import Path
from typing import Optional, Union

import numpy as np
import requests
import streamlit as st
from PIL import Image, ImageFilter
from scipy.ndimage import distance_transform_edt

Src = Union[str, Path, bytes, bytearray, io.BytesIO, Image.Image]


# ───────────────────────────────────────────────────────────────────────────────
# HTTP + I/O helpers
# ───────────────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _http() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "logo-postprocessor/2.1"})
    return s


def _fetch_bytes_any(src: Src, *, timeout: int = 5) -> bytes:
    """Fetch bytes from URL/path/data URI/bytes/BytesIO."""
    if isinstance(src, (bytes, bytearray)):
        return bytes(src)
    if isinstance(src, io.BytesIO):
        return src.getvalue()

    s = str(src)

    if s.startswith("data:"):
        m = re.match(
            r"data:(?:[^;,]+)?(?:;charset=[^;,]+)?(?P<b64>;base64)?,(?P<data>.*)",
            s,
            re.I | re.S,
        )
        if not m:
            return b""
        if m.group("b64"):
            return base64.b64decode(m.group("data"))
        from urllib.parse import unquote_to_bytes
        return unquote_to_bytes(m.group("data"))

    if s.startswith(("http://", "https://")):
        r = _http().get(s, timeout=timeout)
        r.raise_for_status()
        return r.content

    return Path(s).read_bytes()


def _is_svg_bytes(b: bytes) -> bool:
    """Heuristically detect SVG (including svgz)."""
    head = b[:4096].lstrip().lower()
    if head.startswith(b"\x1f\x8b"):  # gzip (svgz)
        try:
            head = gzip.decompress(b[:65536]).lstrip().lower()
        except Exception:
            return False
    return head.startswith(b"<svg") or (b"<svg" in head[:2048])


# ───────────────────────────────────────────────────────────────────────────────
# Cache paths
# ───────────────────────────────────────────────────────────────────────────────

def _cache_root() -> Path:
    root = Path(tempfile.gettempdir()) / "logo_postprocess_cache"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _digest_key(raw: bytes, params: tuple) -> str:
    h = hashlib.blake2b(digest_size=20)
    h.update(raw)
    h.update(repr(params).encode("utf-8", "ignore"))
    return h.hexdigest()


def _save_bytes(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)
    return path


# ───────────────────────────────────────────────────────────────────────────────
# SVG tooling (keep vector; inject padding + white outline filter)
# ───────────────────────────────────────────────────────────────────────────────

def _parse_viewbox(svg: str) -> Optional[list[float]]:
    m = re.search(r'viewBox\s*=\s*"([^"]+)"', svg, re.I)
    if not m:
        return None
    try:
        nums = [float(x) for x in re.split(r"[,\s]+", m.group(1).strip()) if x]
        return nums if len(nums) == 4 else None
    except Exception:
        return None


def _parse_px_attr(svg: str, name: str) -> Optional[float]:
    m = re.search(fr'{name}\s*=\s*"([^"]+)"', svg, re.I)
    if not m:
        return None
    raw = m.group(1).strip().lower()
    try:
        if raw.endswith("px"):
            return float(raw[:-2])
        if re.match(r"^\d+(\.\d+)?$", raw):
            return float(raw)
    except Exception:
        pass
    return None


def _ensure_viewbox(svg: str) -> tuple[str, Optional[list[float]]]:
    vb = _parse_viewbox(svg)
    if vb:
        return svg, vb
    w = _parse_px_attr(svg, "width")
    h = _parse_px_attr(svg, "height")
    if w and h:
        svg = re.sub(r"<svg\b", lambda m: m.group(0) + f' viewBox="0 0 {w} {h}"', svg, count=1, flags=re.I)
        return svg, [0.0, 0.0, float(w), float(h)]
    return svg, None


def _expand_viewbox(svg: str, pad_pct: int) -> tuple[str, Optional[list[float]]]:
    svg, vb = _ensure_viewbox(svg)
    if not vb or pad_pct <= 0:
        return svg, vb
    minx, miny, w, h = vb
    pad = max(w, h) * (pad_pct / 100.0)
    new_vb = f'{minx - pad:g} {miny - pad:g} {w + 2*pad:g} {h + 2*pad:g}'
    svg = re.sub(r'viewBox\s*=\s*"[^"]+"', f'viewBox="{new_vb}"', svg, count=1, flags=re.I)
    return svg, [minx - pad, miny - pad, w + 2*pad, h + 2*pad]


def _inject_outline_filter(svg: str, *, halo_units: float, filter_id: str = "whiteOutline") -> str:
    defs_block = (
        f'<defs>'
        f'  <filter id="{filter_id}" x="-20%" y="-20%" width="140%" height="140%" color-interpolation-filters="sRGB">'
        f'    <feMorphology in="SourceAlpha" operator="dilate" radius="{max(0.0, halo_units):g}" result="spread"/>'
        f'    <feFlood flood-color="white" result="white"/>'
        f'    <feComposite in="white" in2="spread" operator="in" result="outline"/>'
        f'    <feMerge><feMergeNode in="outline"/><feMergeNode in="SourceGraphic"/></feMerge>'
        f'  </filter>'
        f'</defs>'
    )
    svg = re.sub(r"(<svg\b[^>]*>)", r"\1" + defs_block, svg, count=1, flags=re.I)
    svg = re.sub(r"(<svg\b[^>]*>)", r'\1<g filter="url(#' + filter_id + ')">', svg, count=1, flags=re.I)
    svg = re.sub(r"(</svg>)", r"</g>\1", svg, count=1, flags=re.I)
    return svg


# ───────────────────────────────────────────────────────────────────────────────
# Raster image utilities
# ───────────────────────────────────────────────────────────────────────────────

def _content_bbox(alpha: np.ndarray, *, threshold: int = 0):
    solid = alpha > threshold
    rows = np.where(solid.any(axis=1))[0]
    cols = np.where(solid.any(axis=0))[0]
    if rows.size == 0 or cols.size == 0:
        return None
    top, bottom = rows[0], rows[-1] + 1
    left, right = cols[0], cols[-1] + 1
    return (left, top, right, bottom)


def _pad_rgba(img: Image.Image, pad_px: int) -> Image.Image:
    if pad_px <= 0:
        return img
    w, h = img.size
    canvas = Image.new("RGBA", (w + 2 * pad_px, h + 2 * pad_px), (0, 0, 0, 0))
    canvas.paste(img, (pad_px, pad_px), img.split()[-1])
    return canvas


def add_white_band(img: Image.Image, *, width: float | int = 1, feather: float | int = 1) -> Image.Image:
    """Add a thin white halo outside the opaque region (raster only)."""
    if width <= 0:
        return img
    rgba = np.array(img)
    alpha = rgba[..., 3] > 0
    dist_out = distance_transform_edt(~alpha)
    border = (dist_out > 0) & (dist_out <= width)
    if feather > 0:
        fade_zone = (dist_out > width) & (dist_out <= width + feather)
        fade_alpha = np.clip((width + feather - dist_out) / feather, 0, 1)
    else:
        fade_zone = None

    out = rgba.copy()
    if border.any():
        out[border, :3] = 255
        out[border, 3] = 255
    if fade_zone is not None and fade_zone.any():
        fa = (fade_alpha[fade_zone] * 255).astype(np.uint8)
        out[fade_zone, :3] = 255
        out[fade_zone, 3] = np.maximum(out[fade_zone, 3], fa)
    return Image.fromarray(out, "RGBA")


def _resize_max_side(img: Image.Image, target_max_side: int) -> Image.Image:
    w, h = img.size
    if max(w, h) == target_max_side:
        return img
    scale = target_max_side / max(w, h)
    new_size = (max(1, round(w * scale)), max(1, round(h * scale)))
    return img.resize(new_size, resample=Image.LANCZOS)


def _unsharp(img: Image.Image, *, radius: float = 0.5, percent: int = 110, threshold: int = 0) -> Image.Image:
    return img.filter(ImageFilter.UnsharpMask(radius=radius, percent=percent, threshold=threshold))


def _rasterize_svg(svg_bytes: bytes, *, px: int) -> Image.Image:
    """
    Fallback rasterizer only if we end up in the raster branch with an SVG.
    Tries resvg_py first (no native deps), then CairoSVG (needs libcairo).
    """
    try:
        import resvg_py
        png_bytes = bytes(resvg_py.svg_to_bytes(svg_string=svg_bytes.decode("utf-8"), width=px))
        return Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    except Exception:
        pass
    try:
        import cairosvg
        if svg_bytes.startswith(b"\x1f\x8b"):
            svg_bytes = gzip.decompress(svg_bytes)
        png_bytes = cairosvg.svg2png(bytestring=svg_bytes, output_width=px)
        return Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    except Exception as e:
        raise RuntimeError("SVG rasterization failed via resvg_py and CairoSVG.") from e


@st.cache_data(show_spinner=False)
def load_rgba(src: Union[str, Path], *, timeout: int = 5, svg_px: int = 512) -> Image.Image:
    raw = _fetch_bytes_any(src, timeout=timeout)
    if _is_svg_bytes(raw):
        return _rasterize_svg(raw, px=svg_px)
    img = Image.open(io.BytesIO(raw))
    return img.convert("RGBA")


def load_rgba_flexible(src: Src, *, timeout: int = 5, svg_px: int = 512) -> Image.Image:
    if isinstance(src, Image.Image):
        return src.convert("RGBA")
    if isinstance(src, (bytes, bytearray, io.BytesIO)):
        b = src if isinstance(src, (bytes, bytearray)) else src.getvalue()
        if _is_svg_bytes(b):
            return _rasterize_svg(bytes(b), px=svg_px)
        return Image.open(io.BytesIO(b)).convert("RGBA")
    return load_rgba(str(src), timeout=timeout, svg_px=svg_px)


# ───────────────────────────────────────────────────────────────────────────────
# Public API
# ───────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def preprocess_logo(
    src: Src,
    *,
    timeout: int = 5,
    display_px: int = 256,
    dpr: int = 1,
    pad_pct: int = 16,
    halo_px: int = 10,
    halo_feather: int = 0,
    alpha_threshold: int = 8,
    sharpen_after_resize: bool = True,
) -> str:
    """
    Process a logo and SAVE it to disk to avoid browser-side resizing blur.

    Returns:
      - SVG inputs    → str file path to processed .svg (kept vector; outline + padding injected)
      - Raster inputs → str file path to processed .png at EXACT max-side = display_px*dpr

    Recommended usage in Streamlit:
        path = preprocess_logo(url_or_path, display_px=256, dpr=2)
        st.image(path, width=256)  # render at CSS 256px; bitmap is 512px wide (crisp on HiDPI)

    Notes:
      • We resize rasters ONCE server-side to the target backing resolution so the browser doesn't.
      • Padding (%) and halo (CSS px) are honored after resize.
    """
    # Fetch once and decide path
    raw = _fetch_bytes_any(src, timeout=timeout)

    # Generate a stable digest over content + params for caching
    params_key = (
        "v3", display_px, dpr, pad_pct, halo_px, halo_feather, alpha_threshold, sharpen_after_resize
    )

    # ── SVG branch: keep vector, inject outline + padding, save .svg ──
    if _is_svg_bytes(raw):
        text = gzip.decompress(raw).decode("utf-8", "replace") if raw[:2] == b"\x1f\x8b" else raw.decode("utf-8", "replace")

        # Ensure/expand viewBox for padding based on display size
        text, vb = _ensure_viewbox(text)
        text, vb2 = _expand_viewbox(text, pad_pct=pad_pct)

        # Outline thickness in SVG units so it renders as ~halo_px CSS pixels
        if vb2:
            _, _, w, h = vb2
        elif vb:
            _, _, w, h = vb
        else:
            w = h = max(display_px * max(1, dpr), 1)
        target_backing_px = display_px * max(1, dpr)
        unit_per_css_px = (max(float(w), float(h)) / max(1.0, float(target_backing_px)))
        halo_units = float(halo_px) * unit_per_css_px

        if halo_px > 0:
            text = _inject_outline_filter(text, halo_units=halo_units)

        # Save to cache file
        key = _digest_key(raw, params_key)
        out_path = _cache_root() / f"logo-{key}.svg"
        if not out_path.exists():
            _save_bytes(out_path, text.encode("utf-8"))
        return str(out_path)

    # ── Raster branch: trim → compute scaled pad/halo → apply → resize ONCE → (optional) sharpen → save PNG ──
    # Decode to RGBA (if file is actually SVG disguised, loader will rasterize as fallback)
    svg_raster_px = max(2 * display_px, 192)
    img = load_rgba_flexible(src if isinstance(src, (bytes, bytearray, io.BytesIO)) else raw, timeout=timeout, svg_px=svg_raster_px) \
          if isinstance(src, (bytes, bytearray, io.BytesIO)) else load_rgba_flexible(src, timeout=timeout, svg_px=svg_raster_px)

    # 1) Trim to first contentful pixel on each side
    rgba = np.array(img)
    a = rgba[..., 3]
    bbox = _content_bbox(a, threshold=alpha_threshold)
    if bbox is not None:
        img = img.crop(bbox)

    # Content size after crop (in image pixels)
    C = max(img.size[0], img.size[1])

    # 2) Compute padding that equals pad_pct of DISPLAY width after render
    #    pad_img = (pad_css * C) / (D - 2*pad_css) with D = display_px
    D = float(max(1, display_px))
    pad_css = (pad_pct / 100.0) * D
    pad_css = min(pad_css, (D - 1.0) / 2.0)  # clamp (avoid >50% total padding)
    denom = (D - 2.0 * pad_css)
    pad_img = (pad_css * C / denom) if denom > 0 else 0.0
    img = _pad_rgba(img, int(round(pad_img)))

    # 3) Compute halo width/feather in image pixels so they render as desired CSS pixels
    #    px_per_css = W_img / D, where W_img is max side after padding
    W_img = max(img.size[0], img.size[1])
    px_per_css = W_img / D
    halo_img = float(halo_px) * px_per_css
    feather_img = float(halo_feather) * px_per_css
    if halo_px > 0:
        img = add_white_band(img, width=halo_img, feather=feather_img)

    # 4) Resize ONCE to target backing resolution so the browser doesn't resample
    target_backing_px = int(display_px * max(1, dpr))
    img = _resize_max_side(img, target_backing_px)

    # 5) Optional unsharp to restore edge contrast post-resize
    if sharpen_after_resize:
        img = _unsharp(img, radius=0.5, percent=110, threshold=0)

    # 6) Save PNG to cache and return path
    key = _digest_key(raw, params_key + ("png",))
    out_path = _cache_root() / f"logo-{key}.png"
    if not out_path.exists():
        img.save(out_path, format="PNG", optimize=True)
    return str(out_path)
