from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


def _canonical_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_text(s: str) -> str:
    h = hashlib.sha256()
    h.update(s.encode("utf-8"))
    return h.hexdigest()


@dataclass
class ArtifactPaths:
    run_dir: Path
    spec_json: Path
    manifest_json: Path
    replay_token_json: Path
    audit_jsonl: Path
    fills_jsonl: Path
    events_jsonl: Path
    metrics_json: Path
    markout_csv: Path


def make_run_dir(root: Path, run_id: str, timestamp_utc: str) -> ArtifactPaths:
    run_dir = (root / f"{run_id}_{timestamp_utc}").resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    return ArtifactPaths(
        run_dir=run_dir,
        spec_json=run_dir / "spec.json",
        manifest_json=run_dir / "manifest.json",
        replay_token_json=run_dir / "replay_token.json",
        audit_jsonl=run_dir / "audit.jsonl",
        fills_jsonl=run_dir / "fills.jsonl",
        events_jsonl=run_dir / "events.jsonl",
        metrics_json=run_dir / "metrics.json",
        markout_csv=run_dir / "markout.csv",
    )


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(_canonical_dumps(obj) + "\n", encoding="utf-8")


def append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(_canonical_dumps(r) + "\n")


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    import csv

    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def file_sha256(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
