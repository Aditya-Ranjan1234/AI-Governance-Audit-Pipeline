FROM python:3.12-slim
RUN useradd --system --uid 10001 governance
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app/services/investigator-agent
COPY services/investigator-agent/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY services/investigator-agent/*.py ./
COPY config/rules.yml /app/config/rules.yml
COPY db/002_investigator.sql /app/db/002_investigator.sql
USER 10001
ENTRYPOINT ["python", "app.py"]
CMD ["worker"]
