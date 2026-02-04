# The Scrapper (flow-centric scraping framework)

This repository contains a **production-shaped, scalable web scraping system** in Python:

- **Flow-centric plugins**: add new sites/flows without touching core engine
- **Async-native engine**: efficient I/O; sync plugins run via bounded threadpool
- **Reliability policies**: rate limiting, retries, timeouts, and basic circuit breaking
- **Data quality**: schema validation (Pydantic), dedupe keys, quarantine for invalid items
- **Storage**: schema-agnostic SQLite sink by default
- **Observability**: JSON logs + lightweight metrics endpoint

## Quickstart

### 1) Install

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -U pip
pip install -e .
```

### 2) Run the example job

```bash
scrape run --job examples/job_example.yaml
```

Outputs are written to `./data/scraped.db`.

## Concepts

### Site vs Flow vs Schema
- **Site**: operational defaults (domains, headers/auth patterns)
- **Flow**: a workflow within a site (e.g., `search_results`, `product_detail`)
- **Schema**: versioned output contract (e.g., `ExampleArticle@v1`)

### Plugin model
Plugins live in `plugins/`. Each flow plugin yields:
- `Request` objects (what to fetch next), and/or
- `ExtractedItem` objects (raw extracted records + metadata)

Core engine handles fetching, retries, rate limiting, validation, dedupe, and sinking.

## Adding a new site/flow
1. Create a new flow plugin in `plugins/<your_site>/<flow>.py`
2. Create/choose a schema in `scrapper/schemas/` and reference it as `Name@vN`
3. Create a job YAML selecting `site`, `flow`, `schema`, and inputs

## Docker

```bash
docker build -t the-scrapper .
docker run --rm -v "$(pwd)/data:/app/data" the-scrapper scrape run --job examples/job_example.yaml
```

# Flow-centric Scraper Platform

A production-ready, plugin-based scraping framework:

- **Async-native engine** (sync flows via threadpool)
- **Site + Flow plugins** (Python) — add sites/flows without touching the core
- **Schema registry** — many outputs, versioned
- **Reliability** — rate limit, retries, timeouts, circuit breaker, backpressure
- **Data quality** — Pydantic validation, dedupe, quarantine
- **SQLite sink** — schema-agnostic records/quarantine/requests/runs

## Quick start (local)
1) Install: `pip install -e .`
2) Run example flow: `python -m scrapper_cli.main run --job jobs/example_product_detail.yaml`

## Layout
- `scrapper/` — engine, registry, policies, normalization, sinks, monitoring, logging, config
- `scrapper_cli/` — CLI entrypoint
- `plugins/` — example site + flows
- `jobs/` — sample job specs
- `scrapper/schemas/` — versioned Pydantic schemas

## Adding a new site/flow
1) Create a `SitePlugin` (defaults: domains, headers, policies).
2) Create a `FlowPlugin` (async or sync): implement `start_requests()` and `parse()`.
3) Declare `schema_id` for the flow; add/choose a schema in `scrapper/schemas/`.
4) Add a job config selecting `{site, flow, schema, input, policies, sink}`.

## Docker
Build and run: `docker build -t scrapper .` then `docker run scrapper scrape run --job jobs/example_product_detail.yaml`.

