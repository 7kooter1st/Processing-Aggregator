import base64
import io
import logging

from PIL import Image

from app.config import settings

logger = logging.getLogger(__name__)


def downscale_image_b64(image_b64: str) -> str:
    """
    Shrink OCR page images so gemma4 vision does not OOM / return HTTP 500.
    Accepts raw base64 (with or without data: URL prefix).
    """
    max_width = settings.ocr_image_max_width
    if max_width <= 0 or not image_b64:
        return image_b64

    raw = image_b64
    if "," in raw and raw.strip().startswith("data:"):
        raw = raw.split(",", 1)[1]

    try:
        data = base64.b64decode(raw, validate=False)
    except Exception:
        logger.warning("[OCR] cannot decode image base64; sending as-is")
        return image_b64

    try:
        with Image.open(io.BytesIO(data)) as img:
            img = img.convert("RGB")
            if img.width > max_width:
                ratio = max_width / img.width
                new_size = (max_width, max(1, int(img.height * ratio)))
                img = img.resize(new_size, Image.Resampling.LANCZOS)

            out = io.BytesIO()
            quality = settings.ocr_image_jpeg_quality
            img.save(out, format="JPEG", quality=quality, optimize=True)
            encoded = base64.b64encode(out.getvalue()).decode("ascii")
            logger.info(
                "[OCR] image prepared %sx%s -> jpeg_b64_len=%s (was %s)",
                img.width,
                img.height,
                len(encoded),
                len(image_b64),
            )
            return encoded
    except Exception:
        logger.exception("[OCR] image downscale failed; sending original")
        return image_b64
