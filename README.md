# DescuentameCrawlee

Proyecto FastAPI + Crawlee + Playwright listo para desplegar en Render usando Docker.

## Estructura

- `api.py`: API principal (copiada desde `descuentame-back`)
- `requirements.txt`: dependencias Python
- `Dockerfile`: imagen base Playwright con Chromium y deps del sistema
- `render.yaml`: blueprint para Render

## Deploy en Render

1. Sube este proyecto a un repositorio Git.
2. En Render, crea servicio usando `Blueprint` o conecta repo Docker.
3. Configura variables sensibles:
   - `BLOB_READ_WRITE_TOKEN`
   - `API_BASE_URL`
   - `AUTH_JWT_SECRET`
4. Deploy.

## ¿Funciona PlaywrightCrawler + njsparser en Render?

Sí, con Docker (como está armado) funciona porque la imagen base ya incluye dependencias del navegador y runtime de Playwright.
