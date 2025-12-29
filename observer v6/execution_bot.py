# FILE: execution_bot.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
execution_bot.py

Overpriced YT Carry Candidate Emitter (NO TX)

This is NOT a mean-reversion bot.

What it does:
- Reads collector/logs/snapshots_v3.csv (append-only snapshot log)
- Focuses ONLY on configured market_keys (stk-ePendle + mPENDLE by default)
- Emits a carry candidate when:
    * spread = implied_apy - yt_apy is sufficiently NEGATIVE  (YT overpriced)
    * liquidity is sufficient
    * days_to_maturity within [min,max]
    * market is calm right now (spread drift over 10m / 30m below thresholds)
    * observer has NOT flagged repricing risk recently (carry_risk_flags.jsonl)
    * cooldown per market_key is satisfied
- Writes JSONL to alerts/tradeable_candidates.jsonl (name kept for compatibility)

Important:
- This tool does NOT execute trades. It only logs candidates deterministically.
"""

from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import Config

cfg = Config()


# -------------------- Basics --------------------

def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        return int(float(x))
    except Exception:
        return None


def ensure_dirs() -> None:
    Path(cfg.exec_state_path).parent.mkdir(parents=True, exist_ok=True)
    Path(cfg.tradeable_candidates_path).parent.mkdir(parents=True, exist_ok=True)


# -------------------- State --------------------

@dataclass
class ExecState:
    last_emitted_snapshot_ts: Dict[str, str]
    last_emitted_unix: Dict[str, int]


def load_state() -> ExecState:
    if not os.path.exists(cfg.exec_state_path):
        return ExecState(last_emitted_snapshot_ts={}, last_emitted_unix={})
    try:
        with open(cfg.exec_state_path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return ExecState(
            last_emitted_snapshot_ts=dict(d.get("last_emitted_snapshot_ts", {}) or {}),
            last_emitted_unix={k: int(v) for k, v in (d.get("last_emitted_unix", {}) or {}).items()},
        )
    except Exception:
        return ExecState(last_emitted_snapshot_ts={}, last_emitted_unix={})


def save_state(st: ExecState) -> None:
    tmp = cfg.exec_state_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            {
                "ts": utc_iso(),
                "last_emitted_snapshot_ts": st.last_emitted_snapshot_ts,
                "last_emitted_unix": st.last_emitted_unix,
            },
            f,
            indent=2,
        )
    os.replace(tmp, cfg.exec_state_path)


def append_candidate(obj: Dict[str, Any]) -> None:
    Path(cfg.tradeable_candidates_path).parent.mkdir(parents=True, exist_ok=True)
    with open(cfg.tradeable_candidates_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# -------------------- Data load --------------------

def mk_market_key(chain_id: int, address: str) -> str:
    return f"{int(chain_id)}:{(address or '').lower().strip()}"


def load_latest_rows_by_market_key(csv_path: str, target_keys: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Return last ~8 rows per market_key (ordered by ts_unix)."""
    p = Path(csv_path)
    if not p.exists():
        return {}

    buf: Dict[str, List[Dict[str, Any]]] = {k: [] for k in target_keys}

    with p.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            cid = safe_int(row.get("chain_id"))
            addr = (row.get("address") or "").lower().strip()
            if cid is None or not addr:
                continue
            k = mk_market_key(cid, addr)
            if k not in buf:
                continue
            buf[k].append(row)
            if len(buf[k]) > 8:
                buf[k] = buf[k][-8:]

    for k in list(buf.keys()):
        buf[k] = sorted(buf[k], key=lambda x: safe_int(x.get("ts_unix") or 0) or 0)
        if not buf[k]:
            buf.pop(k, None)
    return buf


def load_recent_carry_risk_flags(now_unix: int) -> Dict[str, Dict[str, Any]]:
    """Return last risk flag per market_key within the configured window."""
    path = Path(getattr(cfg, "carry_risk_flags_path", "./alerts/carry_risk_flags.jsonl"))
    if not path.exists() or path.stat().st_size == 0:
        return {}

    window_sec = int(getattr(cfg, "carry_block_if_risk_flag_within_minutes", 60)) * 60
    out: Dict[str, Dict[str, Any]] = {}

    # Read full file (small). Keep newest per market_key.
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                mkey = obj.get("market_key")
                if not isinstance(mkey, str) or not mkey:
                    continue
                ts = obj.get("ts")
                if not isinstance(ts, str) or not ts:
                    continue
                # parse ISO -> unix
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    t_unix = int(dt.timestamp())
                except Exception:
                    continue
                if now_unix - t_unix > window_sec:
                    continue
                prev = out.get(mkey)
                if prev is None or int(prev.get("_t_unix", 0)) < t_unix:
                    obj["_t_unix"] = t_unix
                    out[mkey] = obj
    except Exception:
        return {}

    return out


# -------------------- Signal logic --------------------

def within_dtm(dtm: Optional[float]) -> bool:
    if dtm is None:
        return False
    return (dtm >= cfg.carry_min_days_to_maturity) and (dtm <= cfg.carry_max_days_to_maturity)


def liquidity_ok(mkey: str, liq: Optional[float]) -> bool:
    if liq is None:
        return False
    minliq = float(cfg.carry_min_liquidity_usd.get(mkey, 0.0))
    return liq >= minliq


def overpricing_ok(mkey: str, spread: Optional[float]) -> bool:
    if spread is None:
        return False
    min_abs = float(cfg.carry_min_overpricing_abs.get(mkey, 0.0))
    return spread <= -min_abs


def calc_drifts(rows: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    """Return (drift_10m, drift_30m) using last 2 and last 4 samples."""
    if len(rows) < 2:
        return None, None
    s_last = safe_float(rows[-1].get("spread"))
    s_prev = safe_float(rows[-2].get("spread"))
    if s_last is None or s_prev is None:
        return None, None
    drift_10m = abs(s_last - s_prev)

    drift_30m = None
    if len(rows) >= 4:
        s_30 = safe_float(rows[-4].get("spread"))
        if s_30 is not None:
            drift_30m = abs(s_last - s_30)
    return drift_10m, drift_30m


def calm_ok(drift_10m: Optional[float], drift_30m: Optional[float]) -> bool:
    if drift_10m is None:
        return False
    if drift_10m > cfg.carry_max_abs_spread_drift_10m:
        return False
    if drift_30m is not None and drift_30m > cfg.carry_max_abs_spread_drift_30m:
        return False
    return True


def cooldown_ok(st: ExecState, mkey: str) -> bool:
    last_unix = st.last_emitted_unix.get(mkey)
    if last_unix is None:
        return True
    return (int(time.time()) - int(last_unix)) >= int(cfg.carry_min_minutes_between_candidates) * 60


def risk_flag_ok(risk_flags: Dict[str, Dict[str, Any]], mkey: str) -> bool:
    return mkey not in risk_flags


def build_candidate(
    mkey: str,
    rows: List[Dict[str, Any]],
    drift_10m: Optional[float],
    drift_30m: Optional[float],
    risk_block: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    last = rows[-1]
    cand = {
        "ts": utc_iso(),
        "signal": "CARRY_YT_OVERPRICED",
        "action_hint": "SHORT_YT_LONG_PT",
        "market": last.get("market"),
        "market_key": mkey,
        "chain_id": safe_int(last.get("chain_id")),
        "address": (last.get("address") or "").lower().strip(),
        "snapshot_ts": last.get("timestamp"),
        "ts_unix": safe_int(last.get("ts_unix")),
        "days_to_maturity": safe_float(last.get("days_to_maturity")),
        "yt_apy": safe_float(last.get("yt_apy") or last.get("long_yt_apy")),
        "implied_apy": safe_float(last.get("implied_apy")),
        "spread": safe_float(last.get("spread")),
        "apy_divergence": safe_float(last.get("apy_divergence")),
        "liquidity": safe_float(last.get("liquidity")),
        "drift_abs_10m": drift_10m,
        "drift_abs_30m": drift_30m,
        "blocked_by_recent_risk_flag": bool(risk_block),
        "risk_flag": ({"ts": risk_block.get("ts"), "risk": risk_block.get("risk"), "reasons": risk_block.get("reasons")} if risk_block else None),
        "thresholds": {
            "min_overpricing_abs": cfg.carry_min_overpricing_abs.get(mkey),
            "min_liquidity_usd": cfg.carry_min_liquidity_usd.get(mkey),
            "dtm_min": cfg.carry_min_days_to_maturity,
            "dtm_max": cfg.carry_max_days_to_maturity,
            "max_abs_spread_drift_10m": cfg.carry_max_abs_spread_drift_10m,
            "max_abs_spread_drift_30m": cfg.carry_max_abs_spread_drift_30m,
            "cooldown_min": cfg.carry_min_minutes_between_candidates,
            "block_if_risk_flag_within_min": getattr(cfg, "carry_block_if_risk_flag_within_minutes", 60),
        },
    }
    return cand


def evaluate_market(
    mkey: str,
    rows: List[Dict[str, Any]],
    st: ExecState,
    risk_flags: Dict[str, Dict[str, Any]],
) -> Tuple[bool, List[str], Optional[Dict[str, Any]]]:
    reasons: List[str] = []
    if not rows:
        return False, ["FAIL:no_rows"], None

    last = rows[-1]
    snap_ts = last.get("timestamp")
    if not isinstance(snap_ts, str) or not snap_ts:
        return False, ["FAIL:missing_snapshot_ts"], None

    # avoid re-processing same snapshot endlessly
    if st.last_emitted_snapshot_ts.get(mkey) == snap_ts:
        return False, ["SKIP:already_emitted_for_snapshot"], None

    spread = safe_float(last.get("spread"))
    liq = safe_float(last.get("liquidity"))
    dtm = safe_float(last.get("days_to_maturity"))

    if within_dtm(dtm):
        reasons.append("PASS:dtm")
    else:
        reasons.append("FAIL:dtm")
        return False, reasons, None

    if liquidity_ok(mkey, liq):
        reasons.append("PASS:liq")
    else:
        reasons.append("FAIL:liq")
        return False, reasons, None

    if overpricing_ok(mkey, spread):
        reasons.append("PASS:overpriced")
    else:
        reasons.append("FAIL:overpriced")
        return False, reasons, None

    d10, d30 = calc_drifts(rows)
    if calm_ok(d10, d30):
        reasons.append("PASS:calm")
    else:
        reasons.append("FAIL:calm")
        return False, reasons, None

    # observer risk flags gate
    rb = risk_flags.get(mkey)
    if risk_flag_ok(risk_flags, mkey):
        reasons.append("PASS:risk_flag")
    else:
        reasons.append("FAIL:risk_flag")
        return False, reasons, None

    if cooldown_ok(st, mkey):
        reasons.append("PASS:cooldown")
    else:
        reasons.append("FAIL:cooldown")
        return False, reasons, None

    cand = build_candidate(mkey, rows, d10, d30, rb)
    cand["reasons"] = reasons
    return True, reasons, cand


# -------------------- Main --------------------

def main() -> None:
    ensure_dirs()
    st = load_state()

    buffers = load_latest_rows_by_market_key(cfg.snapshots_csv_path, cfg.carry_target_market_keys)
    now_unix = int(time.time())
    risk_flags = load_recent_carry_risk_flags(now_unix=now_unix)

    if cfg.verbose:
        print(f"[exec] loaded buffers for {len(buffers)}/{len(cfg.carry_target_market_keys)} target markets")
        if risk_flags:
            print(f"[exec] active risk flags (window={getattr(cfg, 'carry_block_if_risk_flag_within_minutes', 60)}m): {list(risk_flags.keys())}")

    emitted = 0
    for mkey, rows in buffers.items():
        ok, reasons, cand = evaluate_market(mkey, rows, st, risk_flags)
        if not ok:
            if cfg.verbose:
                print(f"[exec] {mkey} -> {reasons[-1] if reasons else 'SKIP'}")
            continue

        assert cand is not None
        append_candidate(cand)
        st.last_emitted_snapshot_ts[mkey] = str(cand.get("snapshot_ts") or "")
        st.last_emitted_unix[mkey] = int(time.time())
        emitted += 1

        if cfg.verbose:
            print(f"[exec] EMIT {mkey} market={cand.get('market')} spread={cand.get('spread')} liq={cand.get('liquidity')} dtm={cand.get('days_to_maturity')} reasons={cand.get('reasons')}")

    save_state(st)
    if cfg.verbose:
        print(f"[exec] done. emitted={emitted}. out={cfg.tradeable_candidates_path}")


if __name__ == "__main__":
    main()
