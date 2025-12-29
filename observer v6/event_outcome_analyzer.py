# FILE: event_outcome_analyzer.py
# FILE: event_outcome_analyzer.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
event_outcome_analyzer.py

Purpose:
- Produce a mean-reversion scorecard for DISLOCATION events.
- ALWAYS write alerts/mr_candidates_report.jsonl (even if empty), so ops never wonder if it's a "bug".

Key fixes vs older analyzer:
- Accept baseline embedded in events (event["baseline"]) and metrics (baseline_spread_mean).
- Robustly locate snapshots: prefer event["snapshot_file"] and event["confirm_snapshot_file"]; fallback to data_path scan.
- Handle old events with baseline:null: mark as dropped with reason, do NOT crash.
- Output:
    state/event_scorecard.csv
    alerts/mr_candidates_report.jsonl

This analyzer is now "truthy":
- It only scores events it can actually link to snapshots and baseline.
- It writes drop reasons for everything else.

NOTE:
- This is still MR-centric. Your carry engine is in execution_bot.py.
"""

from __future__ import annotations

import csv
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import Config

cfg = Config()

TS_FMT = "%Y-%m-%d_%H-%M-%S"


# -------------------- IO helpers --------------------

def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(float(x))
    except Exception:
        return None


def safe_json_load(path: Path) -> Any:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def safe_jsonl_iter(path: Path):
    if not path.exists() or path.stat().st_size == 0:
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def safe_jsonl_append(path: Path, obj: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# -------------------- Snapshot access --------------------

def parse_ts(ts_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(ts_str, TS_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def market_key(chain_id: Any, address: Any) -> str:
    try:
        cid = int(chain_id)
    except Exception:
        cid = 0
    addr = str(address or "").lower().strip()
    return f"{cid}:{addr}" if addr else f"{cid}:unknown"


def load_snapshot(path: Path) -> Optional[Dict[str, Any]]:
    obj = safe_json_load(path)
    if not isinstance(obj, dict):
        return None
    return obj


def build_snapshot_index(data_dir: Path) -> Dict[str, Dict[str, Path]]:
    """
    Build mapping:
      idx[market_key][timestamp_str] = file_path
    based on snapshots_v3 JSON files.
    """
    idx: Dict[str, Dict[str, Path]] = {}
    if not data_dir.exists():
        return idx
    for fp in data_dir.glob("*.json"):
        obj = safe_json_load(fp)
        if not isinstance(obj, dict):
            continue
        ts = obj.get("timestamp")
        cid = obj.get("chain_id")
        addr = obj.get("address")
        if not isinstance(ts, str) or not ts:
            continue
        k = market_key(cid, addr)
        idx.setdefault(k, {})[ts] = fp
    return idx


# -------------------- MR scoring --------------------

@dataclass
class MRScore:
    market_key: str
    market: str
    chain_id: int
    address: str
    snapshot_ts: str
    dist0: float
    dist_15m: Optional[float]
    dist_60m: Optional[float]
    dist_3h: Optional[float]
    dist_6h: Optional[float]
    revert_15m: int
    revert_60m: int
    revert_3h: int
    revert_6h: int
    drop_reason: str


def dist_to_baseline(spread: float, baseline_mean: float) -> float:
    return abs(spread - baseline_mean)


def get_baseline_mean(event: Dict[str, Any]) -> Optional[float]:
    # Prefer metrics baseline mean
    metrics = event.get("metrics")
    if isinstance(metrics, dict):
        v = safe_float(metrics.get("baseline_spread_mean"))
        if v is not None:
            return v
    # Fall back to embedded baseline
    b = event.get("baseline")
    if isinstance(b, dict):
        v = safe_float(b.get("spread_mean"))
        if v is not None:
            return v
    return None


def pick_snapshot_file(event: Dict[str, Any], idx: Dict[str, Dict[str, Path]]) -> Tuple[Optional[Path], Optional[Path], str]:
    """
    Returns:
      (base_snapshot_file, confirm_snapshot_file, reason_if_fail)
    """
    # First: explicit files from event
    sf = event.get("snapshot_file")
    cf = event.get("confirm_snapshot_file")
    if isinstance(sf, str) and sf and Path(sf).exists():
        base_fp = Path(sf)
    else:
        base_fp = None
    if isinstance(cf, str) and cf and Path(cf).exists():
        conf_fp = Path(cf)
    else:
        conf_fp = None

    # If base not found, use index by market_key + snapshot_ts
    mkey = event.get("market_key")
    st = event.get("snapshot_ts")
    if base_fp is None and isinstance(mkey, str) and isinstance(st, str):
        base_fp = idx.get(mkey, {}).get(st)
    if conf_fp is None and isinstance(mkey, str) and isinstance(event.get("confirm_snapshot_ts"), str):
        conf_fp = idx.get(mkey, {}).get(event.get("confirm_snapshot_ts"))

    if base_fp is None:
        return None, conf_fp, "missing_base_snapshot"
    return base_fp, conf_fp, "ok"


def find_future_snapshot(mkey: str, base_ts: str, minutes_ahead: int, idx: Dict[str, Dict[str, Path]]) -> Optional[Path]:
    """
    Find snapshot at approximately base_ts + minutes_ahead (nearest within +/- 10 minutes).
    """
    base_dt = parse_ts(base_ts)
    if base_dt is None:
        return None
    target = base_dt + timedelta(minutes=minutes_ahead)
    mp = idx.get(mkey, {})
    if not mp:
        return None

    # brute small search: timestamps are 10-min grid; check nearby offsets
    candidates = []
    for delta in (-20, -10, 0, 10, 20):
        dt = target + timedelta(minutes=delta)
        ts = dt.strftime(TS_FMT)
        fp = mp.get(ts)
        if fp:
            candidates.append((abs(delta), fp))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def compute_revert(dist0: float, dist1: Optional[float]) -> int:
    if dist1 is None:
        return 0
    return 1 if dist1 <= 0.5 * dist0 else 0


def main():
    base = Path(__file__).resolve().parent
    data_dir = Path(getattr(cfg, "data_path", base / "collector" / "logs" / "snapshots_v3"))
    alerts_path = base / getattr(cfg, "alerts_path", "alerts/regime_events.jsonl")

    state_dir = base / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    out_csv = state_dir / "event_scorecard.csv"

    # Always create report file (even empty)
    mr_report_path = base / getattr(cfg, "mr_candidates_report_path", "alerts/mr_candidates_report.jsonl")
    mr_report_path.parent.mkdir(parents=True, exist_ok=True)
    mr_report_path.write_text("", encoding="utf-8")

    idx = build_snapshot_index(data_dir)

    rows: List[MRScore] = []
    scored = 0
    dropped = 0

    for event in safe_jsonl_iter(alerts_path):
        if not isinstance(event, dict):
            continue
        if event.get("regime") != "DISLOCATION":
            continue

        mkey = event.get("market_key")
        market = str(event.get("market") or "")
        chain_id = safe_int(event.get("chain_id")) or 0
        address = str(event.get("address") or "").lower().strip()
        snap_ts = str(event.get("snapshot_ts") or "")

        if not isinstance(mkey, str) or not mkey:
            dropped += 1
            continue

        baseline_mean = get_baseline_mean(event)
        if baseline_mean is None:
            rows.append(
                MRScore(
                    market_key=mkey,
                    market=market,
                    chain_id=chain_id,
                    address=address,
                    snapshot_ts=snap_ts,
                    dist0=0.0,
                    dist_15m=None,
                    dist_60m=None,
                    dist_3h=None,
                    dist_6h=None,
                    revert_15m=0,
                    revert_60m=0,
                    revert_3h=0,
                    revert_6h=0,
                    drop_reason="missing_baseline",
                )
            )
            dropped += 1
            continue

        base_fp, conf_fp, reason = pick_snapshot_file(event, idx)
        if base_fp is None:
            rows.append(
                MRScore(
                    market_key=mkey,
                    market=market,
                    chain_id=chain_id,
                    address=address,
                    snapshot_ts=snap_ts,
                    dist0=0.0,
                    dist_15m=None,
                    dist_60m=None,
                    dist_3h=None,
                    dist_6h=None,
                    revert_15m=0,
                    revert_60m=0,
                    revert_3h=0,
                    revert_6h=0,
                    drop_reason=reason,
                )
            )
            dropped += 1
            continue

        base_snap = load_snapshot(base_fp)
        if base_snap is None:
            dropped += 1
            continue
        spread0 = safe_float(base_snap.get("spread"))
        if spread0 is None:
            rows.append(
                MRScore(
                    market_key=mkey,
                    market=market,
                    chain_id=chain_id,
                    address=address,
                    snapshot_ts=snap_ts,
                    dist0=0.0,
                    dist_15m=None,
                    dist_60m=None,
                    dist_3h=None,
                    dist_6h=None,
                    revert_15m=0,
                    revert_60m=0,
                    revert_3h=0,
                    revert_6h=0,
                    drop_reason="missing_spread0",
                )
            )
            dropped += 1
            continue

        dist0 = dist_to_baseline(float(spread0), float(baseline_mean))

        # horizons
        fp_15 = find_future_snapshot(mkey, base_snap["timestamp"], 15, idx)
        fp_60 = find_future_snapshot(mkey, base_snap["timestamp"], 60, idx)
        fp_3h = find_future_snapshot(mkey, base_snap["timestamp"], 180, idx)
        fp_6h = find_future_snapshot(mkey, base_snap["timestamp"], 360, idx)

        def d(fp: Optional[Path]) -> Optional[float]:
            if fp is None:
                return None
            s = load_snapshot(fp)
            if s is None:
                return None
            sp = safe_float(s.get("spread"))
            if sp is None:
                return None
            return dist_to_baseline(float(sp), float(baseline_mean))

        dist_15 = d(fp_15)
        dist_60 = d(fp_60)
        dist_3 = d(fp_3h)
        dist_6 = d(fp_6h)

        r15 = compute_revert(dist0, dist_15)
        r60 = compute_revert(dist0, dist_60)
        r3 = compute_revert(dist0, dist_3)
        r6 = compute_revert(dist0, dist_6)

        rows.append(
            MRScore(
                market_key=mkey,
                market=market,
                chain_id=chain_id,
                address=address,
                snapshot_ts=base_snap["timestamp"],
                dist0=dist0,
                dist_15m=dist_15,
                dist_60m=dist_60,
                dist_3h=dist_3,
                dist_6h=dist_6,
                revert_15m=r15,
                revert_60m=r60,
                revert_3h=r3,
                revert_6h=r6,
                drop_reason="ok",
            )
        )
        scored += 1

        # Emit MR candidate report line (even if later you choose not to trade it)
        safe_jsonl_append(
            mr_report_path,
            {
                "ts": utc_iso(),
                "market_key": mkey,
                "market": market,
                "snapshot_ts": base_snap["timestamp"],
                "dist0": dist0,
                "revert_15m": r15,
                "revert_60m": r60,
                "revert_3h": r3,
                "revert_6h": r6,
            },
        )

    # Write CSV scorecard
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "market_key", "market", "chain_id", "address", "snapshot_ts",
            "dist0", "dist_15m", "dist_60m", "dist_3h", "dist_6h",
            "revert_15m", "revert_60m", "revert_3h", "revert_6h",
            "drop_reason",
        ])
        for r in rows:
            w.writerow([
                r.market_key, r.market, r.chain_id, r.address, r.snapshot_ts,
                r.dist0, r.dist_15m, r.dist_60m, r.dist_3h, r.dist_6h,
                r.revert_15m, r.revert_60m, r.revert_3h, r.revert_6h,
                r.drop_reason,
            ])

    print(f"[analyzer] events_total={len(rows)} scored={scored} dropped={dropped}")
    print(f"[analyzer] wrote: {out_csv}")
    print(f"[analyzer] wrote: {mr_report_path} (always exists, may be empty)")


if __name__ == "__main__":
    main()
