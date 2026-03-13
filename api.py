import logging
import os
import time
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from stores import scrape_falabella, scrape_meli, scrape_paris, scrape_ripley

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")
APP_STARTED_AT = time.time()

SUPPORTED_STORES = {"falabella", "meli", "mercadolibre", "paris", "ripley"}
PRICE_CHECKER_TOKEN = os.getenv("PRICE_CHECKER_TOKEN", "").strip()
ENVIRONMENT = os.getenv("ENVIRONMENT", "").strip().lower()
QSTASH_URL = os.getenv("QSTASH_URL", "https://qstash.upstash.io").rstrip("/")
QSTASH_TOKEN = os.getenv("QSTASH_TOKEN", "").strip()
QSTASH_TIMEOUT_SECONDS = float(os.getenv("QSTASH_TIMEOUT_SECONDS", "1.5"))
QSTASH_RETRIES = int(os.getenv("QSTASH_RETRIES", "2"))
PRICE_CHECK_RUN_URL = os.getenv("PRICE_CHECK_RUN_URL", "").strip()
PRICE_FIELDS = [
    "current_price",
    "old_price",
    "cmr_price",
    "card_price",
    "cenco_card_price",
    "ripley_card_price",
    "discount",
]


class ProductPriceCheckRequest(BaseModel):
    batch_size: int = Field(1, ge=1, le=50)
    store: Optional[str] = None
    product_id: Optional[str] = None
    keep_published_on_better_price: bool = False
    async_mode: bool = Field(
        default=False,
        description="Si true, encola el trabajo y responde rapido",
    )
    allow_sync_fallback: bool = Field(
        default=False,
        description="Si la cola falla, permite ejecutar sincronamente",
    )


class MongoDB:
    def __init__(self) -> None:
        self.client: Optional[MongoClient] = None
        self.db = None
        self.products: Optional[Collection] = None
        self.notifications: Optional[Collection] = None
        self.connect()

    def connect(self) -> None:
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            raise ValueError("MONGODB_URI no está configurada")

        try:
            self.client = MongoClient(mongo_uri)
            self.db = self.client.get_default_database()
            self.products = self.db.products
            self.notifications = self.db.notifications
            logger.info("✅ Conectado a MongoDB")
        except ConnectionFailure as exc:
            logger.error("❌ Error conectando a MongoDB: %s", exc)
            raise

    def close(self) -> None:
        if self.client is not None:
            self.client.close()


mongo_db = MongoDB()
app = FastAPI(title="Descuentame Price Checker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def normalize_store(raw_store: Optional[str]) -> str:
    store = (raw_store or "").strip().lower()
    if store in {"mercadolibre", "mercado-libre", "ml", "mlc"}:
        return "meli"
    return store


def build_store_query_values(raw_store: Optional[str]) -> List[str]:
    normalized_store = normalize_store(raw_store)
    if normalized_store == "meli":
        return ["meli", "mercadolibre", "mercado-libre", "ml", "mlc"]
    return [normalized_store] if normalized_store else []


def require_internal_token(x_internal_token: Optional[str] = Header(default=None)) -> None:
    if not PRICE_CHECKER_TOKEN:
        return
    if x_internal_token != PRICE_CHECKER_TOKEN:
        raise HTTPException(status_code=401, detail="x-internal-token invalido")


def price_to_int(value: Optional[str]) -> Optional[int]:
    if value in (None, "", [], {}):
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    if not digits:
        return None
    amount = int(digits)
    return amount if amount > 0 else None


def compare_prices(db_prices: Dict[str, Any], html_prices: Dict[str, Any]) -> tuple[str, Dict[str, str]]:
    matches: Dict[str, str] = {}
    statuses: List[str] = []

    for field in PRICE_FIELDS:
        db_raw = db_prices.get(field)
        html_raw = html_prices.get(field)
        db_int = price_to_int(db_raw)
        html_int = price_to_int(html_raw)

        if db_int is None or html_int is None:
            status = "no-checkeado"
        elif db_int == html_int:
            status = "vigente"
        else:
            status = "expirado"

        matches[field] = status
        statuses.append(status)

    if "expirado" in statuses:
        return "expirado", matches
    if "vigente" in statuses:
        return "vigente", matches
    return "no-checkeado", matches


def summarize_price_decision(
    db_prices: Dict[str, Any],
    html_prices: Dict[str, Any],
    matches: Dict[str, str],
) -> Dict[str, List[str]]:
    expired_reasons: List[str] = []
    vigente_reasons: List[str] = []
    unchecked_reasons: List[str] = []

    for field, status in matches.items():
        db_value = db_prices.get(field)
        html_value = html_prices.get(field)
        if status == "expirado":
            expired_reasons.append(
                f"{field}: DB={db_value} vs HTML={html_value}"
            )
        elif status == "vigente":
            vigente_reasons.append(
                f"{field}: DB={db_value} igual a HTML={html_value}"
            )
        else:
            unchecked_reasons.append(
                f"{field}: DB={db_value} / HTML={html_value}"
            )

    return {
        "expired_reasons": expired_reasons,
        "vigente_reasons": vigente_reasons,
        "unchecked_reasons": unchecked_reasons,
    }


def get_best_price(prices: Dict[str, Any]) -> Optional[int]:
    candidates = [
        price_to_int(prices.get("cmr_price")),
        price_to_int(prices.get("card_price")),
        price_to_int(prices.get("cenco_card_price")),
        price_to_int(prices.get("ripley_card_price")),
        price_to_int(prices.get("current_price")),
    ]
    valid_candidates = [value for value in candidates if value is not None]
    return min(valid_candidates) if valid_candidates else None


def should_keep_product_published(
    product_id: str,
    db_prices: Dict[str, Any],
    html_prices: Dict[str, Any],
    enabled: bool,
) -> bool:
    if not enabled:
        logger.info(
            "🧮 keep_published_on_better_price deshabilitado | product_id=%s",
            product_id,
        )
        return False

    scraped_discount = price_to_int(html_prices.get("discount"))
    if scraped_discount is None or scraped_discount < 50:
        logger.info(
            "🧮 keep_published_on_better_price descartado por descuento | product_id=%s scraped_discount=%s",
            product_id,
            scraped_discount,
        )
        return False

    db_best_price = get_best_price(db_prices)
    html_best_price = get_best_price(html_prices)
    if db_best_price is None or html_best_price is None:
        logger.info(
            "🧮 keep_published_on_better_price descartado por precios incompletos | product_id=%s db_best=%s html_best=%s",
            product_id,
            db_best_price,
            html_best_price,
        )
        return False

    is_better_price = html_best_price < db_best_price
    logger.info(
        "🧮 keep_published_on_better_price evaluado | product_id=%s scraped_discount=%s db_best=%s html_best=%s is_better_price=%s",
        product_id,
        scraped_discount,
        db_best_price,
        html_best_price,
        is_better_price,
    )
    return is_better_price


def create_price_expired_notification(
    product: Dict[str, Any], result_item: Dict[str, Any], checked_at: datetime
) -> None:
    if mongo_db.notifications is None:
        return

    notification_doc = {
        "type": "price_expired",
        "title": "Producto archivado por precio expirado",
        "message": (
            f"El producto {product.get('product_id')} de "
            f"{((product.get('store') or {}).get('store_id') or '').strip()} fue archivado "
            "porque cambio su precio respecto a la base de datos."
        ),
        "product_id": str(product.get("product_id") or ""),
        "product_hash_id": str(product.get("_id") or ""),
        "product_name": str(product.get("product_name") or ""),
        "store": ((product.get("store") or {}).get("store_id") or "").strip(),
        "price_status": "expirado",
        "metadata": {
            "db_prices": result_item.get("db_prices") or {},
            "html_prices": result_item.get("html_prices") or {},
            "price_matches": result_item.get("price_matches") or {},
        },
        "read_by": [],
        "created_at": checked_at,
    }
    mongo_db.notifications.insert_one(notification_doc)


async def enqueue_price_check_job(payload: Dict[str, Any], run_url: str) -> Dict[str, Any]:
    if not QSTASH_TOKEN:
        return {"ok": False, "error": "QSTASH_TOKEN no configurado"}
    if not is_valid_qstash_destination(run_url):
        return {"ok": False, "error": f"URL destino invalida para QStash: {run_url}"}

    publish_url = f"{QSTASH_URL}/v2/publish/{run_url}"
    headers = {
        "Authorization": f"Bearer {QSTASH_TOKEN}",
        "Content-Type": "application/json",
        "Upstash-Retries": str(QSTASH_RETRIES),
    }
    if PRICE_CHECKER_TOKEN:
        headers["Upstash-Forward-X-Internal-Token"] = PRICE_CHECKER_TOKEN

    try:
        async with httpx.AsyncClient(timeout=QSTASH_TIMEOUT_SECONDS) as client:
            response = await client.post(publish_url, headers=headers, json=payload)
            if response.is_success:
                return {
                    "ok": True,
                    "message_id": response.headers.get("Upstash-Message-Id"),
                }
            body_preview = (response.text or "")[:250]
            return {
                "ok": False,
                "error": f"QStash status={response.status_code}: {body_preview}",
            }
    except Exception as exc:
        return {"ok": False, "error": f"QStash error: {exc}"}


def is_valid_qstash_destination(target_url: str) -> bool:
    parsed = urlparse(target_url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"}:
        return False
    if host in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}:
        return False
    return True


async def scrape_current_page_data(store_id: str, product_url: str) -> Dict[str, Any]:
    normalized_store = normalize_store(store_id)
    logger.info(
        "🔀 Estrategia seleccionada | store=%s normalized_store=%s url=%s",
        store_id,
        normalized_store,
        product_url,
    )
    if normalized_store == "falabella":
        return await scrape_falabella(product_url)
    if normalized_store == "paris":
        return await scrape_paris(product_url)
    if normalized_store == "meli":
        return await scrape_meli(product_url)
    if normalized_store == "ripley":
        return await scrape_ripley(product_url)
    return {
        "url": product_url,
        "title": None,
        "h1s": [],
        "h2s": [],
        "h3s": [],
        "_strategy": "unsupported",
    }


async def run_price_checker_sync(request: ProductPriceCheckRequest) -> Dict[str, Any]:
    request_started_at = time.perf_counter()
    if mongo_db.products is None:
        raise HTTPException(status_code=500, detail="MongoDB no disponible")

    requested_store = normalize_store(request.store)
    requested_store_values = build_store_query_values(request.store)
    requested_product_id = (request.product_id or "").strip()

    query: Dict[str, Any] = {"status": {"$ne": "archived"}}
    if requested_store:
        if requested_store not in SUPPORTED_STORES:
            return {
                "summary": {
                    "processed": 0,
                    "success": 0,
                    "errors": 0,
                    "vigente": 0,
                    "expirado": 0,
                    "no_checkeado": 0,
                    "not_implemented": 1,
                },
                "results": [
                    {
                        "store": requested_store,
                        "message": "price-check no implementado para esta tienda",
                    }
                ],
            }
        query["store.store_id"] = {"$in": requested_store_values}

    if requested_product_id:
        query["product_id"] = requested_product_id

    projection = {
        "product_id": 1,
        "product_name": 1,
        "store": 1,
        "link_market": 1,
        "prices.current_price": 1,
        "prices.old_price": 1,
        "prices.cmr_price": 1,
        "prices.card_price": 1,
        "prices.cenco_card_price": 1,
        "prices.ripley_card_price": 1,
        "prices.discount": 1,
        "created_at": 1,
        "status": 1,
    }
    limit = 1 if requested_product_id else request.batch_size
    candidate_limit = limit if (requested_store or requested_product_id) else max(limit * 5, 25)
    candidate_products = list(
        mongo_db.products.find(query, projection)
        .sort([("price_checked_at", 1), ("created_at", 1)])
        .limit(candidate_limit)
    )

    if not candidate_products:
        return {
            "summary": {
                "processed": 0,
                "success": 0,
                "errors": 0,
                "vigente": 0,
                "expirado": 0,
                "no_checkeado": 0,
                "not_implemented": 0,
            },
            "results": [],
        }

    skipped_unsupported: List[Dict[str, Any]] = []
    products: List[Dict[str, Any]] = []
    for product in candidate_products:
        store_id = normalize_store(((product.get("store") or {}).get("store_id") or "").strip())
        if store_id in SUPPORTED_STORES:
            products.append(product)
        else:
            skipped_unsupported.append(
                {
                    "product_id": str(product.get("product_id") or ""),
                    "store": store_id or "unknown",
                    "message": "price-check no implementado para esta tienda",
                }
            )
        if len(products) >= limit:
            break

    if not products:
        if skipped_unsupported:
            logger.info(
                "ℹ️ No se encontraron tiendas implementadas en los candidatos | skipped=%s",
                [item["store"] for item in skipped_unsupported[:5]],
            )
            return {
                "summary": {
                    "processed": 0,
                    "success": 0,
                    "errors": 0,
                    "vigente": 0,
                    "expirado": 0,
                    "no_checkeado": 0,
                    "not_implemented": 1,
                },
                "results": [skipped_unsupported[0]],
            }
        return {
            "summary": {
                "processed": 0,
                "success": 0,
                "errors": 0,
                "vigente": 0,
                "expirado": 0,
                "no_checkeado": 0,
                "not_implemented": 0,
            },
            "results": [],
        }

    if skipped_unsupported and not requested_store:
        logger.info(
            "ℹ️ Se omitieron productos de tiendas no implementadas para no frenar el flujo | skipped=%s selected=%s",
            [item["store"] for item in skipped_unsupported[:5]],
            [normalize_store((((item.get('store') or {})).get('store_id') or '').strip()) for item in products],
        )

    results: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for product in products:
        product_started_at = time.perf_counter()
        mongo_id = product.get("_id")
        product_id = str(product.get("product_id") or "")
        store_id = normalize_store(((product.get("store") or {}).get("store_id") or "").strip())
        product_url = (product.get("link_market") or "").strip()
        db_prices = product.get("prices") or {}
        logger.info(
            "🚀 Iniciando scraping | product_id=%s store=%s url=%s",
            product_id,
            store_id,
            product_url,
        )

        result_item: Dict[str, Any] = {
            "product_id": product_id,
            "store": store_id,
            "product_url": product_url,
            "data": None,
            "db_prices": {field: db_prices.get(field) for field in PRICE_FIELDS},
            "html_prices": {field: None for field in PRICE_FIELDS},
            "price_matches": {},
            "price_status": "no-checkeado",
            "archived": False,
        }
        update_doc: Dict[str, Any] = {
            "price_status": "no-checkeado",
            "price_checked_at": now,
            "updated_at": now,
            "price_check": {
                "db_prices": {field: db_prices.get(field) for field in PRICE_FIELDS},
                "html_prices": {field: None for field in PRICE_FIELDS},
                "matches": {},
                "status": "no-checkeado",
                "checked_at": now,
                "source": "descuentame-crawlee",
            },
        }

        try:
            if not product_url or not store_id:
                raise ValueError("Producto sin URL o store_id")

            result_item["data"] = await scrape_current_page_data(store_id, product_url)
            scraped_prices = (
                (result_item["data"] or {}).get("prices")
                if isinstance(result_item.get("data"), dict)
                else None
            ) or {}
            result_item["html_prices"] = {
                field: scraped_prices.get(field) for field in PRICE_FIELDS
            }
            update_doc["price_check"]["html_prices"] = result_item["html_prices"]

            if any(result_item["html_prices"].values()):
                price_status, matches = compare_prices(db_prices, result_item["html_prices"])
                result_item["price_matches"] = matches
                result_item["price_status"] = price_status
                update_doc["price_check"]["matches"] = matches
                update_doc["price_status"] = price_status
                update_doc["price_check"]["status"] = price_status
                decision_summary = summarize_price_decision(
                    db_prices,
                    result_item["html_prices"],
                    matches,
                )
                result_item["decision_summary"] = decision_summary

                if should_keep_product_published(
                    product_id,
                    db_prices,
                    result_item["html_prices"],
                    request.keep_published_on_better_price,
                ):
                    update_doc["prices"] = {
                        **db_prices,
                        **{
                            field: value
                            for field, value in result_item["html_prices"].items()
                            if value is not None
                        },
                    }
                    update_doc["status"] = "published"
                    update_doc["price_status"] = "vigente"
                    update_doc["price_check"]["status"] = "vigente"
                    result_item["price_status"] = "vigente"
                    result_item["archived"] = False
                    result_item["action"] = "updated_and_published"
                    logger.info(
                        "📈 Se mantiene publicado porque el descuento es >= 50 y el mejor precio actual mejora al de la DB | product_id=%s db_best=%s html_best=%s",
                        product_id,
                        get_best_price(db_prices),
                        get_best_price(result_item["html_prices"]),
                    )

                elif price_status == "expirado":
                    update_doc["status"] = "archived"
                    result_item["archived"] = True
                    logger.info(
                        "📦 Producto marcado como expirado porque hay diferencias relevantes en precios | product_id=%s motivos_expirado=%s motivos_vigente=%s",
                        product_id,
                        decision_summary["expired_reasons"],
                        decision_summary["vigente_reasons"],
                    )
                    create_price_expired_notification(product, result_item, now)
                elif price_status == "vigente":
                    logger.info(
                        "✅ Producto vigente, los precios comparables coinciden | product_id=%s motivos_vigente=%s",
                        product_id,
                        decision_summary["vigente_reasons"],
                    )
                else:
                    logger.info(
                        "ℹ️ Producto no-checkeado, faltan precios comparables | product_id=%s motivos=%s",
                        product_id,
                        decision_summary["unchecked_reasons"],
                    )
            else:
                strategy_used = (result_item["data"] or {}).get("_strategy", "unknown")
                data_error = (result_item["data"] or {}).get("_error")
                proxy_used = (result_item["data"] or {}).get("_proxy_used", "none")
                non_archive_reasons = [
                    "no se detectaron precios comparables en el HTML scrapeado",
                    f"strategy={strategy_used}",
                    f"proxy={proxy_used}",
                ]
                if data_error:
                    non_archive_reasons.append(f"scraper_error={data_error}")
                result_item["decision_summary"] = {
                    "expired_reasons": [],
                    "vigente_reasons": [],
                    "unchecked_reasons": non_archive_reasons,
                }
                logger.info(
                    "ℹ️ Producto no se archiva porque no hubo precios para comparar | product_id=%s razones=%s",
                    product_id,
                    non_archive_reasons,
                )

            elapsed = time.perf_counter() - product_started_at
            logger.info(
                "✅ Scraping completado | product_id=%s store=%s strategy=%s price_status=%s archived=%s elapsed=%.2fs",
                product_id,
                store_id,
                (result_item["data"] or {}).get("_strategy", "unknown"),
                result_item["price_status"],
                result_item["archived"],
                elapsed,
            )
        except Exception as exc:
            elapsed = time.perf_counter() - product_started_at
            result_item["error"] = str(exc)
            logger.warning(
                "⚠️ Scraping fallo | product_id=%s store=%s error=%s elapsed=%.2fs",
                product_id,
                store_id,
                exc,
                elapsed,
            )
        if mongo_id is not None:
            mongo_db.products.update_one({"_id": mongo_id}, {"$set": update_doc})

        results.append(result_item)

    summary = {
        "processed": len(results),
        "success": sum(1 for item in results if item.get("data")),
        "errors": sum(1 for item in results if item.get("error")),
        "vigente": sum(1 for item in results if item["price_status"] == "vigente"),
        "expirado": sum(1 for item in results if item["price_status"] == "expirado"),
        "no_checkeado": sum(1 for item in results if item["price_status"] == "no-checkeado"),
        "not_implemented": 0,
    }
    total_elapsed = time.perf_counter() - request_started_at
    logger.info(
        "🏁 Price-check request completado | processed=%s success=%s errors=%s elapsed=%.2fs",
        summary["processed"],
        summary["success"],
        summary["errors"],
        total_elapsed,
    )
    return {"summary": summary, "results": results}


@app.get("/health/check")
async def health_check() -> Dict[str, Any]:
    uptime_seconds = int(time.time() - APP_STARTED_AT)
    return {
        "success": True,
        "data": {
            "status": "healthy",
            "db": "connected" if mongo_db.client else "disconnected",
            "timestamp": datetime.now().timestamp(),
            "uptime_seconds": uptime_seconds,
        },
        "error": None,
        "message": "Health check",
    }


@app.post("/api/v1/products/price-check/run")
async def run_price_checker(
    request: ProductPriceCheckRequest,
    http_request: Request,
    x_internal_token: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_internal_token(x_internal_token)

    if request.async_mode and ENVIRONMENT == "prod":
        run_url = PRICE_CHECK_RUN_URL or str(http_request.url_for("run_price_checker_worker"))
        enqueue_payload = {
            "batch_size": request.batch_size,
            "store": request.store,
            "product_id": request.product_id,
            "keep_published_on_better_price": request.keep_published_on_better_price,
            "async_mode": False,
            "allow_sync_fallback": False,
        }
        queue_result = await enqueue_price_check_job(enqueue_payload, run_url)
        if queue_result.get("ok"):
            logger.info(
                "📬 Price-check encolado | run_url=%s message_id=%s",
                run_url,
                queue_result.get("message_id"),
            )
            return {
                "success": True,
                "message": "Price-check encolado",
                "data": {
                    "queued": True,
                    "queue": "qstash",
                    "message_id": queue_result.get("message_id"),
                    "run_url": run_url,
                },
            }

        logger.warning("⚠️ No se pudo encolar price-check: %s", queue_result.get("error"))
        if not request.allow_sync_fallback:
            raise HTTPException(
                status_code=503,
                detail=f"No se pudo encolar en QStash: {queue_result.get('error')}",
            )

    data = await run_price_checker_sync(request)
    return {
        "success": True,
        "message": "Price-check ejecutado",
        "data": data,
    }


@app.post("/api/v1/products/price-check/worker", name="run_price_checker_worker")
async def run_price_checker_worker(
    request: ProductPriceCheckRequest,
    x_internal_token: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    require_internal_token(x_internal_token)
    data = await run_price_checker_sync(request)
    return {
        "success": True,
        "message": "Price-check worker ejecutado",
        "data": data,
    }


@app.on_event("shutdown")
def shutdown_event() -> None:
    mongo_db.close()
