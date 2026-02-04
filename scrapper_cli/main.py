from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from scrapper.config import load_job_config
from scrapper.engine import run_job


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="scrape")
    sub = p.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="Run a scrape job from YAML config")
    run.add_argument("--job", required=True, help="Path to job YAML")
    run.add_argument("--plugins-dir", default="plugins", help="Plugins directory (dev mode)")
    run.add_argument(
        "--metrics-port",
        type=int,
        default=9009,
        help="Expose /metrics on this port (0 to disable)",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()
    if args.cmd == "run":
        job_path = Path(args.job)
        job = load_job_config(job_path)
        asyncio.run(
            run_job(
                job=job,
                plugins_dir=args.plugins_dir,
                metrics_port=args.metrics_port,
            )
        )


if __name__ == "__main__":
    main()

