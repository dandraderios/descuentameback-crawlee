import asyncio
import json
import os
import re
import logging
import httpx
import gzip
import uuid
import jwt
from dataclasses import dataclass
from typing import Optional, Dict, Any
from io import BytesIO
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from PIL import Image
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import RequestQueue
from vercel_storage import blob
from dotenv import load_dotenv
import njsparser
from urllib.parse import urlparse

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("njsparser").setLevel(logging.ERROR)
logger.info(
    "🧪 JWT module loaded: %s | version=%s",
    getattr(jwt, "__file__", "unknown"),
    getattr(jwt, "__version__", "unknown"),
)

PROMOTEXT_URL = "https://be-paris-backend-cl-bff-browser.ccom.paris.cl/global/GetPromotextConfig"
PROMOTEXT_CACHE: Dict[str, Any] = {"data": None, "ts": 0.0}
PROMOTEXT_TTL_SECONDS = 900

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://descuenta.me",
        "https://www.descuenta.me",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CrawlRequest(BaseModel):
    url: str
    store: str
    country: str = "cl"
    generate_feed: bool = True
    generate_story: bool = True
    link_afiliados: Optional[str] = None


# Configuración
FULL_PAGE = False
BLOB_TOKEN = os.getenv("BLOB_READ_WRITE_TOKEN")
IMAGE_QUALITY = int(os.getenv("IMAGE_QUALITY", 100))
API_BASE_URL = os.getenv(
    "API_BASE_URL", "http://localhost:8001"
)  # o https://api.descuenta.me
INTERNAL_AUTH_EMAIL = os.getenv("INTERNAL_AUTH_EMAIL", "crawler@descuenta.me")
INTERNAL_AUTH_SUB = os.getenv("INTERNAL_AUTH_SUB", "internal-crawler")
AUTH_JWT_SECRET = os.getenv("AUTH_JWT_SECRET", "")
AUTH_JWT_ALGORITHM = os.getenv("AUTH_JWT_ALGORITHM", "HS256")
AUTH_JWT_EXPIRES_SECONDS = int(os.getenv("AUTH_JWT_EXPIRES_SECONDS", "3600"))


class BlobUploader:
    """Maneja la subida de archivos a Vercel Blob"""

    def __init__(self):
        self.token = BLOB_TOKEN

    async def upload_json(self, json_data: dict, store: str, clean_name: str):
        """Sube JSON comprimido a Vercel Blob"""
        if not self.token:
            logger.error("❌ Token no configurado")
            return None

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # 🔥 COMPRIMIR EL JSON
            json_string = json.dumps(json_data, indent=2)

            # Comprimir con gzip
            compressed_buffer = BytesIO()
            with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as f:
                f.write(json_string.encode("utf-8"))

            compressed_data = compressed_buffer.getvalue()

            logger.info(f"📦 JSON original: {len(json_string)} bytes")
            logger.info(
                f"📦 JSON comprimido: {len(compressed_data)} bytes ({len(compressed_data)/len(json_string)*100:.1f}%)"
            )

            # Subir archivo comprimido con extensión .gz
            pathname = f"{store}/json/{clean_name}_{timestamp}.json.gz"

            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: blob.put(
                    pathname=pathname,
                    body=compressed_data,
                    options={
                        "token": self.token,
                        "addRandomSuffix": True,
                        "contentType": "application/gzip",  # Importante!
                        "contentEncoding": "gzip",
                    },
                ),
            )

            url = getattr(resp, "url", None) or (
                resp.get("url") if isinstance(resp, dict) else str(resp)
            )

            logger.info(f"✅ JSON comprimido subido: {url}")
            return url

        except Exception as e:
            logger.error(f"❌ Error subiendo JSON comprimido: {e}")
            return None

    async def upload_screenshot(
        self, screenshot_bytes: bytes, store: str, clean_name: str
    ):
        """Sube screenshot a Vercel Blob"""
        if not self.token:
            logger.error("❌ Token no configurado")
            return None

        try:
            # Procesar imagen
            image = Image.open(BytesIO(screenshot_bytes))
            if image.mode in ("RGBA", "P"):
                image = image.convert("RGB")

            buffer = BytesIO()
            image.save(buffer, format="PNG", quality=IMAGE_QUALITY)
            buffer.seek(0)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pathname = f"{store}/screenshots/{clean_name}_{timestamp}.png"
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: blob.put(
                    pathname=pathname,
                    body=buffer.getvalue(),
                    options={"token": self.token},
                ),
            )

            url = getattr(resp, "url", None) or (
                resp.get("url") if isinstance(resp, dict) else str(resp)
            )
            logger.info(f"✅ Screenshot subido: {url}")
            return url
        except Exception as e:
            logger.error(f"❌ Error subiendo screenshot: {e}")
            return None

    async def upload_html(self, html: str, store: str, clean_name: str):
        """Sube HTML a Vercel Blob"""
        if not self.token:
            logger.error("❌ Token no configurado")
            return None

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pathname = f"{store}/html/{clean_name}_{timestamp}.html"
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: blob.put(
                    pathname=pathname,
                    body=html.encode("utf-8"),
                    options={"token": self.token, "contentType": "text/html"},
                ),
            )

            url = getattr(resp, "url", None) or (
                resp.get("url") if isinstance(resp, dict) else str(resp)
            )
            logger.info(f"✅ HTML subido: {url}")
            return url
        except Exception as e:
            logger.error(f"❌ Error subiendo HTML: {e}")
            return None

    async def upload_html_gz(self, html: str, store: str, clean_name: str):
        """Sube HTML comprimido (gzip) a Vercel Blob"""
        if not self.token:
            logger.error("❌ Token no configurado")
            return None

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            compressed_buffer = BytesIO()
            with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as f:
                f.write(html.encode("utf-8"))
            compressed_data = compressed_buffer.getvalue()

            pathname = f"{store}/html/{clean_name}_{timestamp}.html.gz"
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: blob.put(
                    pathname=pathname,
                    body=compressed_data,
                    options={
                        "token": self.token,
                        "addRandomSuffix": True,
                        "contentType": "application/gzip",
                        "contentEncoding": "gzip",
                    },
                ),
            )

            url = getattr(resp, "url", None) or (
                resp.get("url") if isinstance(resp, dict) else str(resp)
            )
            logger.info(f"✅ HTML comprimido subido: {url}")
            return url
        except Exception as e:
            logger.error(f"❌ Error subiendo HTML comprimido: {e}")
            return None


def clean_url(url: str) -> str:
    """Limpia la URL para usar como nombre de archivo"""
    clean = (
        url.replace("https://", "")
        .replace("http://", "")
        .replace("/", "_")
        .replace(".", "_")
    )
    return clean if clean and clean != "_" else "homepage"


def generate_internal_admin_token() -> Optional[str]:
    """Genera JWT interno para llamadas server-to-server al backend protegido."""
    if not AUTH_JWT_SECRET:
        logger.warning("⚠️ AUTH_JWT_SECRET no configurado, llamada sin token")
        return None

    now = datetime.now(timezone.utc)
    exp = now + timedelta(seconds=AUTH_JWT_EXPIRES_SECONDS)
    payload = {
        "sub": INTERNAL_AUTH_SUB,
        "email": INTERNAL_AUTH_EMAIL,
        "name": "Internal Crawler",
        "role": "admin",
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, AUTH_JWT_SECRET, algorithm=AUTH_JWT_ALGORITHM)


def extract_product_id(url: str) -> Optional[str]:
    """Extrae ID de URL de Falabella"""
    patterns = [r"/(\d+)$", r"/product/(\d+)/", r"productId=(\d+)"]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def extract_meli_product_id(url: str) -> Optional[str]:
    """Extrae ID desde URL de MercadoLibre (MLCU########## o MLC-##########)."""
    match = re.search(r"\b(MLCU\d+)\b", url, re.IGNORECASE)
    if match:
        return match.group(1).upper()

    match = re.search(r"MLC-(\d+)", url)
    if match:
        return match.group(1)
    match = re.search(r"/MLC(\d+)", url)
    if match:
        return match.group(1)
    match = re.search(r"\bMLC(\d+)\b", url)
    if match:
        return match.group(1)
    return None


def extract_paris_product_id(url: str) -> Optional[str]:
    """Extrae ID desde el slug de paris.cl (...-ID.html)."""
    try:
        path = urlparse(url).path
        last = os.path.basename(path)
        if last.endswith(".html"):
            last = last[:-5]
        if "-" in last:
            return last.split("-")[-1]
        return last or None
    except Exception:
        return None


def extract_ripley_product_id(url: str) -> Optional[str]:
    """Extrae ID desde URL de Ripley (MPM#### o #######p)."""
    try:
        match = re.search(r"(mpm\d+)", url, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        path = urlparse(url).path
        last = os.path.basename(path)
        if last.endswith(".html"):
            last = last[:-5]
        match = re.search(r"(\d+)p$", last, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r"(\d+)$", last)
        if match:
            return match.group(1)
    except Exception:
        return None
    return None


def normalize_url(raw_url: str) -> str:
    url = raw_url.strip()
    if not url:
        return url
    if not (url.startswith("http://") or url.startswith("https://")):
        return f"https://{url}"
    return url


def normalize_store(raw_store: str) -> str:
    store = (raw_store or "").strip().lower()
    if store in {"mercadolibre", "mercado-libre", "ml", "mlc"}:
        return "meli"
    return store


def get_product_id_by_store(store: str, url: str) -> Optional[str]:
    if store == "falabella":
        return extract_product_id(url)
    if store == "meli":
        return extract_meli_product_id(url)
    if store == "paris":
        return extract_paris_product_id(url)
    if store == "ripley":
        return extract_ripley_product_id(url)
    return None


def extract_nordic_ctx(raw_script: str) -> Optional[Dict[str, Any]]:
    """Extrae el objeto de _n.ctx.r desde el script de MercadoLibre."""
    if not raw_script:
        return None

    match = re.search(r"_n\.ctx\.r\s*=", raw_script)
    if not match:
        return None

    start = raw_script.find("{", match.end())
    if start == -1:
        return None

    depth = 0
    in_str = False
    escape = False
    str_char = ""
    end = None

    for i in range(start, len(raw_script)):
        ch = raw_script[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == str_char:
                in_str = False
        else:
            if ch in ('"', "'"):
                in_str = True
                str_char = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

    if end is None:
        return None

    obj_text = raw_script[start:end]
    try:
        return json.loads(obj_text)
    except Exception:
        return None


async def extract_store_json(
    context: PlaywrightCrawlingContext, store_name: str
) -> Optional[Dict[str, Any]]:
    """Extrae JSON dependiendo del store."""
    store = store_name.lower().strip()

    if store == "falabella":
        script_selector = "script#__NEXT_DATA__"
        try:
            element = context.page.locator(script_selector)
            if await element.count() > 0:
                raw = await element.text_content()
                if raw:
                    return json.loads(raw)
        except Exception:
            return None
        return None

    if store == "meli":
        script_selector = "script#__NORDIC_RENDERING_CTX__"
        try:
            element = context.page.locator(script_selector)
            if await element.count() > 0:
                raw = await element.text_content()
                if raw:
                    return extract_nordic_ctx(raw)
        except Exception:
            return None
        return None

    # Fallback: intenta ambos
    try:
        element = context.page.locator("script#__NEXT_DATA__")
        if await element.count() > 0:
            raw = await element.text_content()
            if raw:
                return json.loads(raw)
    except Exception:
        pass

    try:
        element = context.page.locator("script#__NORDIC_RENDERING_CTX__")
        if await element.count() > 0:
            raw = await element.text_content()
            if raw:
                return extract_nordic_ctx(raw)
    except Exception:
        return None

    try:
        element = context.page.locator("script#__NORDIC_CORE_CTX__")
        if await element.count() > 0:
            raw = await element.text_content()
            if raw:
                return extract_nordic_ctx(raw)
    except Exception:
        return None

    return None


def extract_preloaded_state(html: str) -> Optional[Dict[str, Any]]:
    if not html:
        return None
    match = re.search(r"window\.__PRELOADED_STATE__\s*=\s*", html)
    if not match:
        return None

    start = html.find("{", match.end())
    if start == -1:
        return None

    depth = 0
    in_str = False
    escape = False
    str_char = ""
    end = None

    for i in range(start, len(html)):
        ch = html[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == str_char:
                in_str = False
        else:
            if ch in ('"', "'"):
                in_str = True
                str_char = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

    if end is None:
        return None

    raw = html[start:end]
    try:
        return json.loads(raw)
    except Exception:
        return None


@dataclass
class StoreHandler:
    name: str
    clean_name_fn: callable
    extract_json_fn: callable
    needs_html: bool = False
    upload_html: bool = False
    screenshot_wait_ms: int = 0
    html_wait_ms: int = 0
    json_wait_selector: Optional[str] = None
    json_wait_timeout_ms: int = 0


def build_store_handlers() -> Dict[str, StoreHandler]:
    async def extract_falabella(ctx, html, country):
        return await extract_store_json(ctx, "falabella")

    async def extract_meli(ctx, html, country):
        return await extract_store_json(ctx, "meli")

    async def extract_paris(ctx, html, country):
        promotext = await fetch_promotext_config()
        return await asyncio.to_thread(
            build_paris_final_json, html, ctx.request.url, country, promotext
        )

    async def extract_ripley(ctx, html, country):
        state = extract_preloaded_state(html or "")
        if isinstance(state, dict):
            return state.get("product")
        return None

    return {
        "falabella": StoreHandler(
            name="falabella",
            clean_name_fn=lambda url: extract_product_id(url) or clean_url(url),
            extract_json_fn=extract_falabella,
        ),
        "meli": StoreHandler(
            name="meli",
            clean_name_fn=lambda url: extract_meli_product_id(url) or clean_url(url),
            extract_json_fn=extract_meli,
            needs_html=True,
            upload_html=True,
            screenshot_wait_ms=1500,
            html_wait_ms=1500,
            json_wait_selector="script#__NORDIC_RENDERING_CTX__, script#__NORDIC_CORE_CTX__",
            json_wait_timeout_ms=10000,
        ),
        "paris": StoreHandler(
            name="paris",
            clean_name_fn=lambda url: extract_paris_product_id(url) or clean_url(url),
            extract_json_fn=extract_paris,
            needs_html=True,
            upload_html=False,
            screenshot_wait_ms=0,
            html_wait_ms=2000,
        ),
        "ripley": StoreHandler(
            name="ripley",
            clean_name_fn=lambda url: extract_ripley_product_id(url) or clean_url(url),
            extract_json_fn=extract_ripley,
            needs_html=True,
            upload_html=False,
            screenshot_wait_ms=0,
            html_wait_ms=1500,
        ),
    }


def format_clp(amount: int | None) -> str | None:
    if amount is None:
        return None
    return f"${amount:,}".replace(",", ".")


def normalize_promotext(data: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}

    def add_entry(k: Any, v: Any) -> None:
        if k is None:
            return
        key = str(k).lower()
        if key and isinstance(v, dict) and "name" in v:
            normalized[key] = v

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if "key" in obj and "name" in obj:
                add_entry(obj.get("key"), obj)
            # direct key->config
            for k, v in obj.items():
                add_entry(k, v)
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(data)
    return normalized


def coupon_from_collections(
    collections: object, promotext: Optional[Dict[str, Any]]
) -> Optional[str]:
    if not isinstance(collections, list):
        return None
    for key in collections:
        k = str(key).lower()
        if "colaborador" in k:
            continue
        if promotext and k in promotext and promotext[k].get("active"):
            name = promotext[k].get("name")
            return name.upper() if name else None
    return None


async def fetch_promotext_config() -> Dict[str, Any]:
    now = datetime.now().timestamp()
    cached = PROMOTEXT_CACHE.get("data")
    ts = PROMOTEXT_CACHE.get("ts", 0.0)
    if cached and (now - ts) < PROMOTEXT_TTL_SECONDS:
        return cached
    try:
        logger.info(f"🧩 Fetching PromotextConfig: {PROMOTEXT_URL}")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(PROMOTEXT_URL)
            resp.raise_for_status()
            data = resp.json()
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], dict):
                data = data["data"]
            data = normalize_promotext(data)
            logger.info(
                f"🧩 PromotextConfig keys: {len(data)} sample_keys={list(data.keys())[:5]}"
            )
            PROMOTEXT_CACHE["data"] = data
            PROMOTEXT_CACHE["ts"] = now
            return data
    except Exception as e:
        logger.warning(f"⚠️ No se pudo obtener PromotextConfig: {e}")
    return cached or {}


def find_category_in_fd(fd: object) -> Optional[str]:
    if not isinstance(fd, dict):
        return None
    for _, item in fd.items():
        if not isinstance(item, dict):
            continue
        if item.get("cls") != "Element":
            continue
        val = item.get("value")
        if isinstance(val, dict) and val.get("name") == "tipoProductoAll":
            return val.get("value")
    return None


def find_product_in_fd(fd: object) -> Optional[Dict[str, Any]]:
    """Busca un dict de producto en el Flight Data dump (njsparser.default)."""
    best = None

    def score_product(d: dict) -> int:
        keys = set(d.keys())
        score = 0
        for k in ("masterVariant", "brand", "name", "description", "variants"):
            if k in keys:
                score += 1
        master = d.get("masterVariant")
        if isinstance(master, dict):
            if "prices" in master:
                score += 2
            if "images" in master:
                score += 1
        return score

    def consider(prod: dict) -> None:
        nonlocal best
        s = score_product(prod)
        if s >= 3:
            if best is None or s > best[0]:
                best = (s, prod)

    def walk(x: object) -> None:
        if isinstance(x, dict):
            if "product" in x and isinstance(x["product"], dict):
                consider(x["product"])
            consider(x)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    if isinstance(fd, dict):
        for _, item in fd.items():
            if isinstance(item, dict) and item.get("cls") == "DataContainer":
                walk(item.get("value"))
        if best:
            return best[1]

    walk(fd)
    return best[1] if best else None


def build_paris_final_json(
    html: str, url: str, country: str, promotext: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if not html:
        return None
    fd = njsparser.BeautifulFD(html)
    fd_dict = json.loads(json.dumps(fd, default=njsparser.default))
    product = find_product_in_fd(fd_dict)
    if not product:
        return None

    category = find_category_in_fd(fd_dict)

    brand = product.get("brand")
    brand_name = brand.get("name") if isinstance(brand, dict) else brand

    master = product.get("masterVariant") or {}
    sku = master.get("sku")
    product_id = extract_paris_product_id(url)

    prices = master.get("prices") or {}
    regular = prices.get("regular") or {}
    offer = prices.get("offer") or {}
    payment = prices.get("paymentMethod") or {}

    def cents(obj: dict) -> int | None:
        try:
            return obj.get("value", {}).get("centAmount")
        except Exception:
            return None

    regular_amt = cents(regular)
    offer_amt = cents(offer)
    payment_amt = cents(payment)

    current_amt = offer_amt if offer_amt is not None else regular_amt
    currency = None
    for obj in (offer, payment, regular):
        c = obj.get("value", {}).get("currencyCode") if isinstance(obj, dict) else None
        if c:
            currency = c
            break

    discount = None
    payment_discount = (
        payment.get("discountOnRegular") if isinstance(payment, dict) else None
    )
    offer_discount = offer.get("discountOnRegular") if isinstance(offer, dict) else None

    if payment_discount is not None:
        discount = round(payment_discount * 100)
    elif offer_discount is not None:
        discount = round(offer_discount * 100)
    elif regular_amt and current_amt and regular_amt > 0 and offer_amt is not None:
        discount = round((regular_amt - current_amt) / regular_amt * 100)

    images = master.get("images") or []
    product_images = [
        img.get("url") for img in images if isinstance(img, dict) and img.get("url")
    ]
    collections = master.get("collections") or []
    try:
        logger.info(f"🧩 Paris collections: {collections}")
    except Exception:
        pass
    coupon = coupon_from_collections(collections, promotext)
    if coupon is None and collections:
        keys_count = len(promotext) if promotext else 0
        sample = collections[:5] if isinstance(collections, list) else []
        matches = 0
        matched_keys = []
        if promotext:
            matched_keys = list({str(c).lower() for c in collections} & set(promotext.keys()))
            matches = len(matched_keys)
        logger.info(
            f"🧩 Coupon not found. collections_sample={sample} promotext_keys={keys_count} matches={matches} matched_keys={matched_keys[:5]}"
        )
    elif coupon is not None:
        logger.info(f"🧩 Coupon matched: {coupon}")

    return {
        "id": None,
        "product_id": product_id,
        "store": {
            "store_id": "paris",
            "store_name": "paris",
            "country": country,
            "product_url": url,
        },
        "product_name": product.get("name"),
        "brand": brand_name,
        "sku": sku,
        "category": category
        or product.get("departmentName")
        or product.get("subDepartmentName"),
        "description": product.get("description"),
        "prices": {
            "cenco_card_price": format_clp(payment_amt),
            "current_price": format_clp(current_amt),
            "old_price": format_clp(regular_amt) if offer_amt is not None else None,
            "discount": str(discount) if discount is not None else None,
            "coupon": coupon,
            "currency": currency,
        },
        "product_images": product_images,
        "status": "published",
        "market_place": "paris",
        "link_market": url,
    }


async def run_crawler(
    url: str,
    store_name: str,
    country: str = "cl",
    link_afiliados: Optional[str] = None,
    generate_feed: bool = True,
    generate_story: bool = True,
):
    """Ejecuta el crawler optimizado para velocidad"""
    logger.info(f"⚙️ Iniciando crawler para: {url}")
    store_key = normalize_store(store_name)
    handlers = build_store_handlers()

    async def extract_default(ctx, html, country):
        return await extract_store_json(ctx, store_key)

    handler = handlers.get(
        store_key,
        StoreHandler(
            name=store_key,
            clean_name_fn=clean_url,
            extract_json_fn=extract_default,
        ),
    )

    # === CONFIGURACIÓN OPTIMIZADA (sin parámetros inválidos) ===
    crawler_options = dict(
        max_requests_per_crawl=1,  # Solo 1 request
        headless=True,
        browser_type="chromium",
        max_request_retries=0,
        browser_launch_options={
            "chromium_sandbox": False,
            "args": ["--no-sandbox", "--disable-setuid-sandbox"],
        },
        # Eliminamos max_concurrency y enable_autoscaled_pool que causaban error
    )

    # Usar una cola única por ejecución para evitar deduplicación entre llamadas
    request_queue = await RequestQueue.open(name=f"crawl-{uuid.uuid4()}")
    crawler_options["request_manager"] = request_queue

    if store_key == "meli":
        crawler_options.update(
            {
                "headless": True,
                "retry_on_blocked": False,
                "use_session_pool": False,
                "browser_launch_options": {
                    "channel": "chrome",
                    "chromium_sandbox": False,
                    "args": ["--no-sandbox", "--disable-setuid-sandbox"],
                },
                "browser_new_context_options": {
                    "locale": "es-CL",
                    "timezone_id": "America/Santiago",
                    "viewport": {"width": 1440, "height": 1800},
                    "user_agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                },
                "goto_options": {"wait_until": "domcontentloaded"},
                "navigation_timeout": timedelta(seconds=120),
            }
        )

    crawler = PlaywrightCrawler(id=str(uuid.uuid4()), **crawler_options)

    # Inicializar uploader
    uploader = BlobUploader()

    # Variables para guardar las URLs resultantes
    json_url = None
    screenshot_url = None
    html_url = None

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        nonlocal json_url, screenshot_url, html_url
        context.log.info(f"🚀 Procesando: {context.request.url}")

        # SIN ESPERAS ADICIONALES - IGUAL QUE screen.py
        clean_name = handler.clean_name_fn(context.request.url)
        if not clean_name:
            clean_name = "homepage"

        json_url = None
        screenshot_url = None

        html = None
        if handler.needs_html:
            try:
                try:
                    await context.page.wait_for_load_state("networkidle", timeout=30000)
                except Exception:
                    context.log.warning("⚠️ networkidle timeout, continuo igual")
                if handler.html_wait_ms > 0:
                    await context.page.wait_for_timeout(handler.html_wait_ms)
                html = await context.page.content()
                if html and handler.upload_html:
                    html_url = await uploader.upload_html_gz(
                        html, store_key, clean_name
                    )
            except Exception as e:
                context.log.warning(f"⚠️ Error HTML: {e}")

        # --- 1. JSON (store-specific) ---
        try:
            if handler.json_wait_selector:
                try:
                    await context.page.wait_for_selector(
                        handler.json_wait_selector,
                        timeout=handler.json_wait_timeout_ms or 10000,
                    )
                except Exception:
                    pass
            data_json = await handler.extract_json_fn(context, html, country)
            if data_json:
                context.log.info("✅ JSON extraído")
                json_url = await uploader.upload_json(data_json, store_key, clean_name)
            else:
                context.log.info("ℹ️ Sin JSON para este store")
        except Exception as e:
            context.log.warning(f"⚠️ Error JSON: {e}")

        # --- 2. SCREENSHOT - IGUAL QUE screen.py ---
        try:
            await context.page.set_viewport_size({"width": 1440, "height": 1800})
            if handler.screenshot_wait_ms > 0:
                await context.page.wait_for_timeout(handler.screenshot_wait_ms)
            screenshot_bytes = await context.page.screenshot(full_page=FULL_PAGE)
            context.log.info(f"📸 Screenshot capturado")

            # Subir screenshot
            screenshot_url = await uploader.upload_screenshot(
                screenshot_bytes, store_key, clean_name
            )

        except Exception as e:
            context.log.error(f"❌ Error screenshot: {e}")

        context.log.info(
            f"🏁 Completado - JSON: {bool(json_url)}, Screenshot: {bool(screenshot_url)}"
        )

    # Ejecutar crawler - SIN LOGS EXTRAS
    await crawler.run([url], purge_request_queue=True)
    logger.info(f"✅ Crawler terminado para {url}")
    # 🔥 NUEVO: Después del crawler, llamar a la API
    if json_url and screenshot_url:
        logger.info("🎯 Archivos subidos correctamente, llamando a API...")
        product_id = get_product_id_by_store(store_key, url)
        logger.info(f"📦 product_id enviado: {product_id}")
        await call_generate_api(
            original_url=url,
            store=store_key,
            country=country,
            screenshot_url=screenshot_url,
            metadata_url=json_url,
            product_id=product_id,
            link_afiliados=link_afiliados,
            generate_feed=generate_feed,
            generate_story=generate_story,
        )
    else:
        logger.warning(
            "⚠️ No se pudieron subir todos los archivos, omitiendo llamada a API"
        )


# 🔥 NUEVA FUNCIÓN: Llamar a la API para generar imágenes
async def call_generate_api(
    original_url: str,
    store: str,
    country: str,
    screenshot_url: str,
    metadata_url: str,
    product_id: Optional[str] = None,
    link_afiliados: Optional[str] = None,
    generate_feed: bool = True,
    generate_story: bool = True,
):
    """Llama al endpoint /generate-from-storage después del crawling"""
    try:
        api_url = f"{API_BASE_URL}/api/v1/generate-from-storage"

        payload = {
            "url": original_url,
            "store": store,
            "country": country,
            "generate_feed": generate_feed,
            "generate_story": generate_story,
            "screenshot_url": screenshot_url,
            "metadata_url": metadata_url,
        }
        if product_id:
            payload["product_id"] = product_id
        if link_afiliados:
            payload["link_afiliados"] = link_afiliados

        token = generate_internal_admin_token()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
            logger.info("🔐 JWT interno generado, llamada autenticada")
        else:
            logger.warning("⚠️ Llamada a generate-from-storage sin JWT")

        logger.info(f"📤 Llamando a API: {api_url}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(api_url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()

        logger.info(f"✅ API respondió: {result.get('message', 'OK')}")
        return result

    except httpx.HTTPStatusError as e:
        logger.error(
            f"❌ Error HTTP en API: {e.response.status_code} - {e.response.text}"
        )
    except httpx.RequestError as e:
        logger.error(f"❌ Error de conexión con API: {e}")


@app.post("/crawl")
async def start_crawl_post(payload: CrawlRequest, background_tasks: BackgroundTasks):
    """Endpoint para crawling rápido por POST"""
    normalized_url = normalize_url(payload.url)
    normalized_store = normalize_store(payload.store)
    if normalized_store == "falabella":
        product_id = extract_product_id(normalized_url)
    elif normalized_store == "meli":
        product_id = extract_meli_product_id(normalized_url)
    elif normalized_store == "paris":
        product_id = extract_paris_product_id(normalized_url)
    elif normalized_store == "ripley":
        product_id = extract_ripley_product_id(normalized_url)
    else:
        product_id = None
    background_tasks.add_task(
        run_crawler,
        normalized_url,
        normalized_store,
        country=payload.country,
        link_afiliados=payload.link_afiliados,
        generate_feed=payload.generate_feed,
        generate_story=payload.generate_story,
    )
    return {
        "status": "iniciado",
        "url": normalized_url,
        "store": normalized_store,
        "country": payload.country,
        "generate_feed": payload.generate_feed,
        "generate_story": payload.generate_story,
        "link_afiliados": payload.link_afiliados,
        "product_id": product_id,
    }


@app.get("/health/check")
async def health_check():
    return {
        "success": True,
        "data": {
            "status": "healthy",
            "version": "1.0.0",
            "timestamp": datetime.now().timestamp(),
        },
        "error": None,
        "message": "Health check",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
