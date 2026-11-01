FROM python:3.12-slim
WORKDIR /evaluation
COPY evaluation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY evaluation/evaluator.py .
COPY schemas/governance-event.schema.json ./governance-event.schema.json
ENTRYPOINT ["python", "evaluator.py"]

