FROM python:3.12-slim
ARG ADAPTER
RUN test -n "$ADAPTER" && useradd --system --uid 10001 governance
WORKDIR /app
ENV PYTHONPATH=/app
COPY services/producer_adapters/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/producer_adapters/common ./common
COPY services/producer_adapters/tests ./tests
COPY schemas/governance-event.schema.json ./governance-event.schema.json
COPY services/producer_adapters/${ADAPTER}/app.py ./app.py
USER 10001
EXPOSE 8080
CMD ["python", "app.py"]
