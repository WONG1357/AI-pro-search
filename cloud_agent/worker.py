"""A source fetch runs in a killable subprocess; no model calls occur here."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from cloud_agent.schema import SearchRequest
from pipeline.pipeline import run_pipeline


def run_source(request: SearchRequest, source: str) -> dict:
    if source not in request.sources:
        raise ValueError("Source is not included in this request")
    result = run_pipeline(
        keywords=request.keywords, years=request.years,
        selected_sources=[source], export_results=False,
        start_date=request.start_date, end_date=request.end_date,
        classifier_profile=request.classifier_profile,
        components=list(request.components), accident_terms=list(request.accident_terms),
        source_options={
            "_global": {"request_timeout": request.request_timeout, "max_pages": request.max_pages},
            source: {"max_records": request.max_records},
        },
    )
    return {
        name: json.loads(frame.to_json(orient="records", date_format="iso"))
        for name, frame in result.items() if isinstance(frame, pd.DataFrame)
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    request = SearchRequest.from_dict(json.loads(args.request.read_text(encoding="utf-8")))
    payload = run_source(request, args.source)
    temporary = args.result.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temporary.replace(args.result)


if __name__ == "__main__":
    main()
