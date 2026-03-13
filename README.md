# DescuentameCrawlee

Servicio FastAPI para leer productos desde MongoDB y scrapear datos HTML por tienda.

Hoy el endpoint principal no compara precios. Por ahora devuelve, para cada producto encontrado en Mongo:

- `url`
- `title`
- `h1s`
- `h2s`
- `h3s`

La estrategia de scraping depende de la tienda:

- `falabella`: `BeautifulSoupCrawler`
- `paris`: `BeautifulSoupCrawler`
- `mercadolibre` / `meli`: Playwright directo
- `ripley`: primero `BeautifulSoupCrawler`, y si no obtiene data útil hace fallback a `PlaywrightCrawler`

El endpoint también puede correr en modo asíncrono para producción:

- responde rápido `200`
- encola el trabajo en QStash
- luego QStash invoca el worker interno con ejecución síncrona

## Lógica Final por Tienda

### `falabella`

- usa `BeautifulSoupCrawler`
- devuelve `url`, `title`, `h1s`, `h2s`, `h3s`
- si hay proxy configurado para BeautifulSoup, lo usa fuera de `ENVIRONMENT=dev`

### `paris`

- usa `BeautifulSoupCrawler`
- devuelve `url`, `title`, `h1s`, `h2s`, `h3s`
- si hay proxy configurado para BeautifulSoup, lo usa fuera de `ENVIRONMENT=dev`

### `mercadolibre` / `meli`

- usa Playwright directo (`async_playwright`)
- crea contexto con:
  - `locale=es-CL`
  - `timezone_id=America/Santiago`
  - `user_agent` custom
  - `viewport` desktop
  - `accept-language=es-CL,es;q=0.9,en;q=0.8`
- inyecta script para reducir señales obvias de automatización
- puede usar proxy vía `PLAYWRIGHT_PROXY_*`

### `ripley`

- intenta primero con `BeautifulSoupCrawler`
- si `BeautifulSoup` devuelve data útil, termina ahí
- si falla, no procesa respuesta o devuelve HTML sin contenido útil, cae a `PlaywrightCrawler`
- en la respuesta queda `_strategy` con:
  - `beautifulsoup`
  - `beautifulsoup->playwright-crawler`
- cuando BeautifulSoup resuelve, también queda `_proxy_used`

## Comportamiento por Entorno

### `ENVIRONMENT=dev`

- no usa proxy en ningún flujo
- `BeautifulSoupCrawler` usa el cliente HTTP por defecto de Crawlee
- esto se dejó así porque local funcionaba mejor sin `ImpitHttpClient(timeout=10)`

### Otros entornos

- `BeautifulSoupCrawler` puede usar `BEAUTIFULSOUP_PROXY_URLS`
- `BeautifulSoupCrawler` usa `ImpitHttpClient(timeout=10)` para cortar proxies lentos rápido
- Playwright directo puede usar `PLAYWRIGHT_PROXY_SERVER`, `PLAYWRIGHT_PROXY_USERNAME`, `PLAYWRIGHT_PROXY_PASSWORD`
- `PlaywrightCrawler` puede usar lista rotativa con `PLAYWRIGHT_CRAWLER_PROXY_URLS`

## Proxies

Variables disponibles:

```env
ENVIRONMENT=dev
BEAUTIFULSOUP_PROXY_URLS=http://user:pass@ip1:port,http://user:pass@ip2:port
PLAYWRIGHT_PROXY_URLS=http://user:pass@ip1:port,http://user:pass@ip2:port
PLAYWRIGHT_CRAWLER_PROXY_URLS=http://user:pass@ip1:port,http://user:pass@ip2:port
```

Notas:

- `BEAUTIFULSOUP_PROXY_URLS` es lista
- `PLAYWRIGHT_PROXY_URLS` es lista para Playwright directo
- `PLAYWRIGHT_CRAWLER_PROXY_URLS` es lista para `PlaywrightCrawler`
- si `ENVIRONMENT=dev`, los proxies quedan deshabilitados aunque estén definidos
- `ENVIRONMENT=dev` tiene prioridad sobre [scraping_config.json](/Users/dandrade/Github/DescuentameCrawlee/scraping_config.json)
- en `ENVIRONMENT=dev`, `BeautifulSoupCrawler` también deja de usar `ImpitHttpClient(timeout=10)` y vuelve al cliente HTTP por defecto de Crawlee

Compatibilidad:

- el código todavía acepta `PLAYWRIGHT_PROXY_SERVER`, `PLAYWRIGHT_PROXY_USERNAME`, `PLAYWRIGHT_PROXY_PASSWORD`
- pero si defines `PLAYWRIGHT_PROXY_URLS`, esa lista tiene prioridad para Playwright directo

## Prioridad de Configuración

El orden real de decisión es:

1. [scraping_config.json](/Users/dandrade/Github/DescuentameCrawlee/scraping_config.json) define la configuración base por estrategia y por tienda
2. las variables de entorno aportan los proxies concretos
3. `ENVIRONMENT=dev` actúa como override fuerte para desarrollo local

Eso significa que, aunque el JSON diga:

```json
"beautifulsoup": { "use_proxy": true }
```

si corres con:

```env
ENVIRONMENT=dev
```

entonces el código igualmente:

- desactiva proxies de `BeautifulSoupCrawler`
- desactiva proxies de Playwright directo
- desactiva proxies de `PlaywrightCrawler`
- desactiva `ImpitHttpClient(timeout=10)` para `BeautifulSoupCrawler`

El objetivo es que el entorno local conserve un comportamiento más cercano al que ya te funcionaba antes de endurecer los timeouts y la rotación de proxies.

## Endpoints

### `GET /health/check`

Health check básico del servicio y del estado de conexión a Mongo.

### `POST /api/v1/products/price-check/run`

Endpoint público. Busca productos en la colección `products` y scrapea su página de mercado.

Body:

```json
{
  "batch_size": 1,
  "store": "falabella",
  "product_id": "115663135"
}
```

Campos:

- `batch_size`: cantidad de productos a procesar
- `store`: filtro opcional por tienda
- `product_id`: filtro opcional por producto específico
- `keep_published_on_better_price`: si es `true`, evita archivar cuando el descuento es `>= 50%` y el mejor precio mejora la DB
- `async_mode`: si es `true` y `ENVIRONMENT=prod`, intenta encolar en QStash hacia el worker interno y responder rápido
- `allow_sync_fallback`: si QStash falla, permite ejecutar el check síncrono en la misma request

### `POST /api/v1/products/price-check/worker`

Endpoint interno para QStash. Ejecuta siempre el procesamiento síncrono.

- no está pensado para clientes públicos
- requiere el mismo `x-internal-token` cuando `PRICE_CHECKER_TOKEN` está configurado

Si `PRICE_CHECKER_TOKEN` está configurado, debes enviar:

```http
x-internal-token: TU_TOKEN
```

## Respuesta

Ejemplo:

```json
{
  "success": true,
  "message": "Price-check ejecutado",
  "data": {
    "summary": {
      "processed": 1,
      "success": 1,
      "errors": 0,
      "not_implemented": 0
    },
    "results": [
      {
        "product_id": "49361931",
        "store": "meli",
        "product_url": "https://www.mercadolibre.cl/...",
        "data": {
          "url": "https://www.mercadolibre.cl/...",
          "title": "Loción Desmaquillante...",
          "h1s": ["Loción Desmaquillante..."],
          "h2s": [],
          "h3s": []
        }
      }
    ]
  }
}
```

Si `async_mode=true` en producción, puede responder así:

```json
{
  "success": true,
  "message": "Price-check encolado",
  "data": {
    "queued": true,
    "queue": "qstash",
    "message_id": "msg_...",
    "run_url": "https://tu-servicio/api/v1/products/price-check/worker"
  }
}
```

## Variables de entorno

Obligatoria:

```env
MONGODB_URI=...
```

Opcional:

```env
PRICE_CHECKER_TOKEN=...
ENVIRONMENT=dev
BEAUTIFULSOUP_PROXY_URLS=...
PLAYWRIGHT_PROXY_SERVER=...
PLAYWRIGHT_PROXY_USERNAME=...
PLAYWRIGHT_PROXY_PASSWORD=...
PLAYWRIGHT_CRAWLER_PROXY_URLS=...
QSTASH_TOKEN=...
QSTASH_URL=https://qstash.upstash.io
QSTASH_TIMEOUT_SECONDS=1.5
QSTASH_RETRIES=2
PRICE_CHECK_RUN_URL=https://tu-servicio/api/v1/products/price-check/worker
```

## Desarrollo local

Instalar dependencias:

```bash
pip install -r requirements.txt
```

Levantar servidor:

```bash
uvicorn api:app --reload --port 8003
```

Ejemplo de request local:

```bash
curl -X POST 'http://localhost:8003/api/v1/products/price-check/run' \
  -H 'Content-Type: application/json' \
  -H 'x-internal-token: TU_PRICE_CHECKER_TOKEN' \
  -d '{
    "batch_size": 1,
    "store": "mercadolibre"
  }'
```

Si no usas `PRICE_CHECKER_TOKEN`, elimina ese header.

Ejemplo de request asíncrono para producción:

```bash
curl -X POST 'https://tu-servicio/api/v1/products/price-check/run' \
  -H 'Content-Type: application/json' \
  -H 'x-internal-token: TU_PRICE_CHECKER_TOKEN' \
  -d '{
    "batch_size": 1,
    "store": "falabella",
    "async_mode": true,
    "allow_sync_fallback": true,
    "keep_published_on_better_price": true
  }'
```

## Docker

Construcción:

```bash
docker build -t descuentame-crawlee .
```

Ejecución:

```bash
docker run --rm -p 10000:10000 \
  -e MONGODB_URI='...' \
  -e PRICE_CHECKER_TOKEN='...' \
  descuentame-crawlee
```

## Render

El proyecto ya incluye:

- [Dockerfile](/Users/dandrade/Github/DescuentameCrawlee/Dockerfile)
- [render.yaml](/Users/dandrade/Github/DescuentameCrawlee/render.yaml)

Variables necesarias en Render:

- `MONGODB_URI`
- `PRICE_CHECKER_TOKEN` opcional
- `ENVIRONMENT=prod`
- `QSTASH_TOKEN` si vas a usar encolado
- `PRICE_CHECK_RUN_URL` recomendado en producción y apuntando al worker interno

Health check configurado:

```txt
/health/check
```

## Logs

El servicio deja trazas para entender qué hizo en cada producto:

- tienda y URL procesada
- estrategia elegida
- duración del scraping
- duración total del request
- preview de 500 caracteres del HTML
- proxy usado cuando aplica
- cantidad de intentos de `BeautifulSoupCrawler`

Ejemplo:

```txt
🚀 Iniciando scraping | product_id=49361931 store=meli url=...
🔀 Estrategia seleccionada | store=meli normalized_store=meli strategy=playwright url=...
🎭 Estrategia Playwright | url=...
🎭 Playwright cargo pagina | url=... elapsed=4.87s
✅ Scraping completado | product_id=49361931 store=meli strategy=playwright elapsed=5.13s
🏁 Price-check request completado | processed=1 success=1 errors=0 elapsed=5.18s
```

Ejemplo para Ripley con proxy:

```txt
🕷️ Estrategia BeautifulSoupCrawler | url=...
🌐 BeautifulSoup proxies configurados | count=5
🕷️ BeautifulSoup procesando respuesta | url=... attempt=1 proxy=http://38.3.162.129:999
🕷️ BeautifulSoup html preview | url=... html_500=<!DOCTYPE html> ...
🕷️ Ripley resuelto con BeautifulSoup | url=... proxy=http://38.3.162.129:999
```
