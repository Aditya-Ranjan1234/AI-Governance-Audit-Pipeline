FROM scratch AS wheelhouse
COPY services/investigator-agent/requirements.txt /requirements.txt

FROM python:3.12-slim
RUN useradd --system --uid 10001 governance
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app/services/investigator-agent
COPY services/investigator-agent/requirements.txt ./
ARG PIP_NO_INDEX=0
RUN --mount=type=bind,from=wheelhouse,target=/wheels \
    pip install --no-cache-dir --find-links=/wheels -r requirements.txt
COPY services/investigator-agent/*.py ./
COPY config/rules.yml /app/config/rules.yml
COPY db/002_investigator.sql /app/db/002_investigator.sql
COPY db/003_reviewer.sql /app/db/003_reviewer.sql
USER 10001
ENTRYPOINT ["python", "app.py"]
CMD ["worker"]
