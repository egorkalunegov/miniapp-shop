FROM node:20-slim AS webapp-build

WORKDIR /src/webapp

COPY webapp/package*.json ./
RUN npm install

COPY webapp/ ./
RUN npm run build


FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend/main.py /app/main.py
COPY --from=webapp-build /src/webapp/dist /app/webapp_dist

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
