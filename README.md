# DescuentameCrawlee

Servicio FastAPI para leer productos desde MongoDB y scrapear datos HTML por tienda.

Hoy el endpoint principal no compara precios. Por ahora devuelve, para cada producto encontrado en Mongo:

- `url`
- `title`
- `h1s`
- `h2s`
- `h3s`

La estrategia de scraping depende de la tienda:

- `falabella`, `paris`, `ripley`: `BeautifulSoupCrawler`
- `mercadolibre` / `meli`: `Playwright`

## Endpoints

### `GET /health/check`

Health check básico del servicio y del estado de conexión a Mongo.

### `POST /api/v1/products/price-check/run`

Busca productos en la colección `products` y scrapea su página de mercado.

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

## Variables de entorno

Obligatoria:

```env
MONGODB_URI=...
```

Opcional:

```env
PRICE_CHECKER_TOKEN=...
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

Health check configurado:

```txt
/health/check
```

## Logs

El servicio deja trazas para entender qué hizo en cada producto:

- tienda y URL procesada
- estrategia elegida (`beautifulsoup` o `playwright`)
- duración del scraping
- duración total del request

Ejemplo:

```txt
🚀 Iniciando scraping | product_id=49361931 store=meli url=...
🔀 Estrategia seleccionada | store=meli normalized_store=meli strategy=playwright url=...
🎭 Estrategia Playwright | url=...
🎭 Playwright cargo pagina | url=... elapsed=4.87s
✅ Scraping completado | product_id=49361931 store=meli strategy=playwright elapsed=5.13s
🏁 Price-check request completado | processed=1 success=1 errors=0 elapsed=5.18s
```
