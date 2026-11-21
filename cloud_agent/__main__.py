"""Cloud controller and the deterministic tools used inside the hosted sandbox."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from uuid import uuid4
from zoneinfo import ZoneInfo


def _json_file(path: Path) -> dict:
    if path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("Input JSON exceeds the 2 MiB limit")
    return json.loads(path.read_text(encoding="utf-8"))


def _instructions(repo: Path) -> str:
    text = (repo / "cloud_agent" / "instructions.md").read_text(encoding="utf-8")
    return text + "\nCurrent Hong Kong date: " + datetime.now(ZoneInfo("Asia/Hong_Kong")).date().isoformat() + ".\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AI Pro Search: OpenAI-hosted surveillance agent")
    sub = parser.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start", help="Start a new task in OpenAI cloud")
    task = start.add_mutually_exclusive_group(required=True)
    task.add_argument("--task")
    task.add_argument("--task-file", type=Path)
    start.add_argument("--state", type=Path)
    start.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    start.add_argument("--model", default="gpt-6-astra")
    start.add_argument("--agent-id")
    start.add_argument("--detach", action="store_true", help="Return after session creation; cloud execution continues")
    for name in ("status", "resume", "cancel"):
        command = sub.add_parser(name)
        command.add_argument("--state", type=Path, required=True)
    download = sub.add_parser("download")
    download.add_argument("--state", type=Path, required=True)
    download.add_argument("--output", type=Path, default=Path("agent-results"))
    cleanup = sub.add_parser("cleanup", help="Delete an idle cloud session after preserving artifacts")
    cleanup.add_argument("--state", type=Path, required=True)
    search = sub.add_parser("search", help="Deterministic retrieval tool, normally executed inside the cloud sandbox")
    search.add_argument("--request", type=Path, required=True)
    search.add_argument("--output", type=Path, default=Path("/workspace/outputs"))
    verify = sub.add_parser("verify")
    verify.add_argument("--run-dir", type=Path, required=True)
    finalize = sub.add_parser("finalize")
    finalize.add_argument("--run-dir", type=Path, required=True)
    finalize.add_argument("--assessment", type=Path, required=True)
    supplement = sub.add_parser("supplement", help="Preserve explicitly discovered official documents separately from database counts")
    supplement.add_argument("--run-dir", type=Path, required=True)
    supplement.add_argument("--documents", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "search":
            from cloud_agent.schema import SearchRequest
            from cloud_agent.collector import collect
            from cloud_agent.evidence import write_report, verify_report
            request = SearchRequest.from_dict(_json_file(args.request))
            result = collect(request)
            folder = write_report(request, result, args.output)
            print(json.dumps({"run_directory": str(folder), **verify_report(folder)}, indent=2))
            # Partial/failed source coverage is a valid report, never an unhandled tool crash.
            return 0
        if args.command == "verify":
            from cloud_agent.integrity import verify_report
            print(json.dumps(verify_report(args.run_dir), indent=2))
            return 0
        if args.command == "finalize":
            from cloud_agent.evidence import add_assessment
            print(json.dumps({"assessment": str(add_assessment(args.run_dir, _json_file(args.assessment)))}))
            return 0
        if args.command == "supplement":
            from cloud_agent.evidence import add_supplements
            result = add_supplements(args.run_dir, _json_file(args.documents))
            print(json.dumps(result, indent=2))
            return 0 if result["supplemental_documents"] else 2

        from cloud_agent import hosted
        if args.command == "start":
            task_text = args.task if args.task is not None else args.task_file.read_text(encoding="utf-8")
            if not task_text.strip() or len(task_text) > 12000:
                raise ValueError("Task must be nonempty text of at most 12,000 characters")
            state = args.state or Path(".agent-state") / (uuid4().hex + ".json")
            result = hosted.start_task(task_text, args.repo, state, model=args.model, instructions=_instructions(args.repo), agent_id=args.agent_id, wait=not args.detach)
            print(json.dumps({"state_file": str(state), **result}, ensure_ascii=False, indent=2))
        elif args.command == "status":
            result = hosted.get_status(args.state)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.command == "resume":
            result = hosted.resume_task(args.state)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.command == "cancel":
            result = hosted.cancel_task(args.state)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.command == "download":
            files = hosted.collect_artifacts(args.state, args.output)
            from cloud_agent.integrity import verify_downloads
            reports = verify_downloads(files)
            print(json.dumps({"files": [str(path) for path in files], "verified_reports": reports}, indent=2))
            return 2 if all(report["status"] == "failed" for report in reports) else 0
        elif args.command == "cleanup":
            result = hosted.delete_session(args.state)
            print(json.dumps(result, indent=2))
        return 1 if result.get("status") in {"failed", "cancelled", "environment_failed"} else 0
    except (ValueError, OSError, RuntimeError) as exc:
        # Do not print SDK request headers, raw input payloads, or API keys.
        import re
        message = re.sub(r"sk-[A-Za-z0-9_-]+", "[REDACTED]", str(exc))
        print(json.dumps({"error": message[:1000]}), file=sys.stderr)
        return 1
    except Exception as exc:
        # API exceptions can carry submitted content. Return safe metadata only.
        detail = {"error": type(exc).__name__}
        status = getattr(exc, "status_code", None)
        if isinstance(status, int):
            detail["http_status"] = status
        print(json.dumps(detail), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
