FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY scrapper /app/scrapper
COPY scrapper_cli /app/scrapper_cli
COPY plugins /app/plugins
COPY examples /app/examples

RUN pip install -U pip && pip install -e .

CMD ["scrape", "run", "--job", "examples/job_example.yaml", "--metrics-port", "9009"]

