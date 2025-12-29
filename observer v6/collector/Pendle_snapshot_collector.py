# FILE: collector/Pendle_snapshot_collector.py
# FILE: collector/Pendle_snapshot_collector.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pendle snapshot collector (v3)

Tämä skripti hakee Pendle V2 API:sta kaikki markkinat, suodattaa ne
ja kirjoittaa snapshotit:
- JSON: ./collector/logs/snapshots_v3/*.json
- CSV : ./collector/logs/snapshots_v3.csv

Se kirjoittaa jokaiselle hyväksytylle markkinalle yhden rivin per poll (default 10 min).

Lisäksi:
- Tallentaa kentät, joita observer / analyysi tarvitsee (yt_apy, spread, apy_divergence jne).
- Kirjoittaa "skip reasons" TOP-N jokaisella kierroksella, jotta näet heti miksi markkinoita tiputetaan.

Huom: Tämä on tarkoituksella "tylsä ja luotettava".
"""

from __future__ import annotations

import csv
import json
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


LOGS = Path(__file__).resolve().parent / "logs"
SNAPS_DIR = LOGS / "snapshots_v3"
CSV_FILE = LOGS / "snapshots_v3.csv"

TS_FMT = "%Y-%m-%d_%H-%M-%S"


def _to_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _first_present(d: Dict[str, Any], paths: Tuple[str, ...]) -> Any:
    for p in paths:
        cur: Any = d
        ok = True
        for part in p.split("."):
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur[part]
        if ok:
            return cur
    return None


def _parse_chain_id(m: Dict[str, Any]) -> Optional[int]:
    v = _first_present(m, ("chainId", "chain_id", "chain.id"))
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _parse_chain_name(m: Dict[str, Any]) -> str:
    v = _first_present(m, ("chain", "chain.name", "chainName"))
    if isinstance(v, str) and v:
        return v.lower().strip()
    cid = _parse_chain_id(m)
    if cid == 1:
        return "ethereum"
    if cid == 42161:
        return "arbitrum"
    if cid == 8453:
        return "base"
    return "unknown"


def _parse_address(m: Dict[str, Any]) -> Optional[str]:
    v = _first_present(m, ("address", "marketAddress", "market.address"))
    if isinstance(v, str) and v.startswith("0x") and len(v) >= 10:
        return v.lower().strip()
    return None


def _parse_market_symbol(m: Dict[str, Any]) -> Optional[str]:
    v = _first_present(m, ("name", "symbol", "market", "market.name", "market.symbol"))
    if isinstance(v, str) and v:
        return v.strip()
    return None


def _parse_expiry_dt(m: Dict[str, Any]) -> Optional[datetime]:
    v = _first_present(m, ("expiry", "expiryTime", "expiryTimestamp", "maturity", "maturityTime"))
    if v is None:
        return None
    try:
        # seconds
        if isinstance(v, (int, float)) and v > 10_000_000:
            return datetime.fromtimestamp(int(v), tz=timezone.utc)
        # iso
        if isinstance(v, str) and v:
            # allow "Z"
            return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None
    return None


def _parse_apy_fields(m: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    # long_yt_apy equivalents
    yt_paths = (
        "ytApy",
        "longYtApy",
        "yt_apy",
        "long_yt_apy",
        "yt.apy",
        "ytApy.apy",
    )
    imp_paths = (
        "impliedApy",
        "implied_apy",
        "implied.apy",
        "impliedApy.apy",
    )

    yt = _to_float_or_none(_first_present(m, yt_paths))
    imp = _to_float_or_none(_first_present(m, imp_paths))
    return yt, imp


def _parse_liquidity(m: Dict[str, Any]) -> Optional[float]:
    liq_paths = (
        "liquidity",
        "liquidityUsd",
        "liquidityUSD",
        "totalLiquidity",
        "totalValueLocked",
        "tvl",
    )
    return _to_float_or_none(_first_present(m, liq_paths))


@dataclass
class Config:
    base_url: str = "https://api-v2.pendle.finance"
    markets_endpoint: str = "/core/v1/markets/all"
    poll_interval_sec: int = 600  # 10 min
    timeout_sec: int = 20

    allowed_chains: Tuple[int, ...] = (1, 42161, 8453)  # ethereum, arbitrum, base
    min_liquidity_usd: float = 50_000.0
    min_days_to_maturity: float = 2.0
    max_days_to_maturity: float = 365.0

    verbose: bool = True
    print_skip_reasons_topn: int = 10


CFG = Config()


# ------------------------------ small utils ------------------------------

def _jget(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        if part not in cur:
            return default
        cur = cur[part]
    return cur


# ------------------------------ IO ------------------------------

CSV_FIELDS = [
    "timestamp", "ts_iso", "ts_unix",
    "market", "chain", "chain_id", "address",
    "days_to_maturity",
    "long_yt_apy", "yt_apy", "implied_apy",
    "spread", "apy_divergence",
    "liquidity",
]


def _ensure_csv_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """Hard-guarantee that critical CSV fields exist and are sane.

    This prevents downstream drift/calm logic from silently breaking if a snapshot schema
    ever changes or a single field comes through as None/empty.
    """
    # timestamps
    if not row.get("ts_unix"):
        # try derive from ts_iso if present
        ts_iso = row.get("ts_iso")
        if isinstance(ts_iso, str) and ts_iso:
            try:
                dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                row["ts_unix"] = int(dt.timestamp())
            except Exception:
                row["ts_unix"] = int(datetime.now(timezone.utc).timestamp())

    # normalize yt_apy
    if row.get("yt_apy") in (None, ""):
        if row.get("long_yt_apy") not in (None, ""):
            row["yt_apy"] = row.get("long_yt_apy")

    # spread & apy_divergence
    if row.get("spread") in (None, ""):
        try:
            imp = float(row.get("implied_apy"))
            yt = float(row.get("yt_apy") if row.get("yt_apy") is not None else row.get("long_yt_apy"))
            row["spread"] = imp - yt
        except Exception:
            pass

    if row.get("apy_divergence") in (None, ""):
        try:
            imp = float(row.get("implied_apy"))
            yt = float(row.get("yt_apy") if row.get("yt_apy") is not None else row.get("long_yt_apy"))
            row["apy_divergence"] = abs(imp - yt)
        except Exception:
            pass

    return row


def _write_csv_row(path: Path, row: Dict[str, Any]) -> None:
    row = _ensure_csv_fields(dict(row))
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in CSV_FIELDS})


def _write_snapshot_json(dir_path: Path, snap: Dict[str, Any]) -> None:
    ts = snap["timestamp"]
    sym = snap["market"] or "unknown"
    fp = dir_path / f"{ts}_{sym}.json"
    fp.write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")


# ------------------------------ API ------------------------------

def fetch_markets() -> List[Dict[str, Any]]:
    url = CFG.base_url.rstrip("/") + CFG.markets_endpoint
    r = requests.get(url, timeout=CFG.timeout_sec)
    r.raise_for_status()
    j = r.json()
    if isinstance(j, dict):
        for k in ("data", "markets", "results"):
            if k in j and isinstance(j[k], list):
                return j[k]
    if isinstance(j, list):
        return j
    return []


# ------------------------------ Snapshot build ------------------------------

def _make_snapshot(m: Dict[str, Any], now: datetime) -> Tuple[Optional[Dict[str, Any]], str]:
    addr = _parse_address(m)
    if not addr:
        return None, "missing_address"

    expiry_dt = _parse_expiry_dt(m)
    if expiry_dt is None:
        return None, "missing_expiry"

    delta_days = (expiry_dt - now).total_seconds() / 86400.0
    if delta_days <= 0:
        return None, "expired"
    if not (CFG.min_days_to_maturity <= delta_days <= CFG.max_days_to_maturity):
        return None, "dtm_out_of_range"

    chain_id = _parse_chain_id(m)
    chain_name = _parse_chain_name(m)

    # if chain_id exists enforce allowed chains
    if chain_id is not None and chain_id not in CFG.allowed_chains:
        return None, "chain_not_allowed"

    long_yt_apy, implied_apy = _parse_apy_fields(m)
    liquidity = _parse_liquidity(m)

    if long_yt_apy is None or implied_apy is None or liquidity is None:
        return None, "missing_key_fields"

    if liquidity < CFG.min_liquidity_usd:
        return None, "liq_too_low"

    spread = implied_apy - long_yt_apy

    ts_iso = now.isoformat().replace("+00:00", "Z")
    snap = {
        "timestamp": now.strftime(TS_FMT),
        "ts_iso": ts_iso,
        "ts_unix": int(now.timestamp()),
        "market": _parse_market_symbol(m) or "unknown",
        "chain": chain_name,
        "chain_id": chain_id,
        "address": addr,
        "days_to_maturity": float(delta_days),
        "liquidity": float(liquidity),

        "long_yt_apy": float(long_yt_apy),
        # schema-normalized:
        "yt_apy": float(long_yt_apy),
        "implied_apy": float(implied_apy),

        "spread": float(spread),
        "apy_divergence": float(abs(implied_apy - long_yt_apy)),
    }
    return snap, "ok"


# ------------------------------ Main loop ------------------------------

def main() -> None:
    SNAPS_DIR.mkdir(parents=True, exist_ok=True)
    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    while True:
        now = datetime.now(timezone.utc)

        try:
            markets = fetch_markets()
        except Exception as e:
            print(f"[collector] fetch failed: {e}")
            time.sleep(CFG.poll_interval_sec)
            continue

        reasons = Counter()
        written = 0

        for m in markets:
            snap, reason = _make_snapshot(m, now)
            reasons[reason] += 1
            if snap is None:
                continue
            try:
                _write_snapshot_json(SNAPS_DIR, snap)
                _write_csv_row(CSV_FILE, snap)
                written += 1
            except Exception as e:
                reasons["write_error"] += 1
                if CFG.verbose:
                    print(f"[collector] write failed: {e}")

        total += written
        print(f"[collector] cycle={now.strftime(TS_FMT)} written={written} total={total} markets={len(markets)}")

        # Print skip reasons TOP-N (THIS IS THE KEY DEBUG TOOL)
        if CFG.verbose:
            topn = reasons.most_common(CFG.print_skip_reasons_topn)
            # show only meaningful ones
            topn = [(k, v) for k, v in topn if k != "ok"]
            if topn:
                print("[collector] Skip reasons (top): " + ", ".join([f"{k}:{v}" for k, v in topn]))
            else:
                print("[collector] No skips. All markets written.")

        time.sleep(CFG.poll_interval_sec)


if __name__ == "__main__":
    main()
