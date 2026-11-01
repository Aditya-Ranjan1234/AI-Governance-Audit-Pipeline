FROM python:3.12-slim
RUN useradd --system --uid 10001 governance
WORKDIR /app
ENV PYTHONPATH=/app
COPY services/risk-scorer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/risk-scorer/*.py ./
COPY services/risk-scorer/tests ./tests
USER 10001
CMD ["python", "app.py"]
