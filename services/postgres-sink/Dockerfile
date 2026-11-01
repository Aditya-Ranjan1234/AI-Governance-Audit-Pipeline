FROM python:3.12-slim
RUN useradd --system --uid 10001 governance
WORKDIR /app
COPY services/postgres-sink/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/postgres-sink/app.py .
USER 10001
CMD ["python", "app.py"]

