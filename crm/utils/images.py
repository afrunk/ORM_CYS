"""Image helper utilities (thumbnail/preview generation & removal)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple

from flask import current_app
from PIL import Image


# Thumbnail (list view)
THUMB_SUBDIR = "thumbs"
THUMB_PREFIX = "thumb_"
THUMB_SIZE: Tuple[int, int] = (160, 160)

# Large preview (overlay / detail)
PREVIEW_SUBDIR = "previews"
PREVIEW_PREFIX = "preview_"
PREVIEW_SIZE: Tuple[int, int] = (1080, 1080)

WEBP_QUALITY_THUMB = 74
WEBP_QUALITY_PREVIEW = 82


def _static_root() -> str:
    return os.path.abspath(os.path.join(current_app.root_path, "..", "static"))


def _uploads_root() -> str:
    return os.path.join(_static_root(), "uploads")


def _variant_names(image_filename: str, prefix: str) -> tuple[str, str]:
    """Return (webp_name, legacy_name) for the resized file."""
    stem = Path(image_filename).stem or os.path.splitext(image_filename)[0]
    webp_name = f"{prefix}{stem}.webp"
    legacy_name = f"{prefix}{image_filename}"
    return webp_name, legacy_name


def _ensure_variant(
    image_filename: str,
    *,
    prefix: str,
    subdir: str,
    size: Tuple[int, int],
    quality: int,
) -> Optional[str]:
    """Create (if needed) and return the relative static path for a resized WEBP variant."""
    if not image_filename:
        return None

    uploads_dir = _uploads_root()
    original_path = os.path.join(uploads_dir, image_filename)
    if not os.path.exists(original_path):
        return None

    variant_dir = os.path.join(uploads_dir, subdir)
    os.makedirs(variant_dir, exist_ok=True)

    variant_name, legacy_name = _variant_names(image_filename, prefix)
    variant_path = os.path.join(variant_dir, variant_name)
    legacy_path = os.path.join(variant_dir, legacy_name)

    try:
        regenerate = True
        if os.path.exists(variant_path):
            regenerate = os.path.getmtime(original_path) > os.path.getmtime(variant_path)

        if regenerate:
            with Image.open(original_path) as img:
                img_copy = img.copy()
                img_copy.thumbnail(size, Image.Resampling.LANCZOS)

                # Convert to a WEBP‑friendly mode
                if img_copy.mode not in ("RGB", "L"):
                    img_copy = img_copy.convert("RGB")

                img_copy.save(
                    variant_path,
                    format="WEBP",
                    optimize=True,
                    quality=quality,
                    method=6,
                )

        # Best‑effort: clean legacy file if it exists and is not the same as the new path
        if os.path.exists(legacy_path) and legacy_path != variant_path:
            try:
                os.remove(legacy_path)
            except OSError:
                pass

        return os.path.join("uploads", subdir, variant_name).replace("\\", "/")
    except Exception as exc:  # pragma: no cover - best effort logging
        current_app.logger.warning(
            "Failed to generate %s variant for %s: %s", prefix, image_filename, exc
        )
        if os.path.exists(variant_path):
            return os.path.join("uploads", subdir, variant_name).replace("\\", "/")
        if os.path.exists(legacy_path):
            return os.path.join("uploads", subdir, legacy_name).replace("\\", "/")
        return None


def ensure_thumbnail(image_filename: str) -> Optional[str]:
    """Create (if needed) and return the relative static path for the thumbnail."""
    return _ensure_variant(
        image_filename,
        prefix=THUMB_PREFIX,
        subdir=THUMB_SUBDIR,
        size=THUMB_SIZE,
        quality=WEBP_QUALITY_THUMB,
    )


def ensure_preview(image_filename: str) -> Optional[str]:
    """Create (if needed) and return the relative static path for the preview image."""
    return _ensure_variant(
        image_filename,
        prefix=PREVIEW_PREFIX,
        subdir=PREVIEW_SUBDIR,
        size=PREVIEW_SIZE,
        quality=WEBP_QUALITY_PREVIEW,
    )


def _remove_variant(image_filename: Optional[str], *, prefix: str, subdir: str) -> None:
    """Delete generated variant files (WEBP and legacy) for a given original image."""
    if not image_filename:
        return

    variant_name, legacy_name = _variant_names(image_filename, prefix)
    variant_dir = os.path.join(_uploads_root(), subdir)

    for name in (variant_name, legacy_name):
        variant_path = os.path.join(variant_dir, name)
        if os.path.exists(variant_path):
            try:
                os.remove(variant_path)
            except OSError:
                current_app.logger.debug("Failed to delete variant %s", variant_path)


def remove_thumbnail(image_filename: Optional[str]) -> None:
    """Remove thumbnail if it exists."""
    _remove_variant(image_filename, prefix=THUMB_PREFIX, subdir=THUMB_SUBDIR)


def remove_preview(image_filename: Optional[str]) -> None:
    """Remove preview image if it exists."""
    _remove_variant(image_filename, prefix=PREVIEW_PREFIX, subdir=PREVIEW_SUBDIR)


