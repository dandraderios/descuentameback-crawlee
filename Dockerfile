FROM mcr.microsoft.com/playwright/python:v1.51.0-jammy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY api.py ./

ENV PYTHONUNBUFFERED=1
ENV PORT=10000

EXPOSE 10000

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]
