# FILE: observer_v2_complete.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
observer_v2_complete.py

Pendle Observer (v5) — professional, deterministic.

Core jobs:
1) Maintain baselines per market_key (chain_id:address).
2) Emit DISLOCATION events with embedded baseline anchors (for diagnostics / MR analysis).
3) Emit carry-risk flags for the Overpriced YT Carry strategy:
   - If repricing is active (velocity/accel high, liquidity drop, APY divergence), we log a risk flag.
   - execution_bot blocks carry entries if a recent risk flag exists.

Outputs:
- alerts/regime_events.jsonl
- alerts/carry_risk_flags.jsonl
- state/baselines.json
- state/market_state.json
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from config import Config

cfg = Config()

TS_FMT = "%Y-%m-%d_%H-%M-%S"


# -------------------- Thresholds (dislocation detection) --------------------

@dataclass
class Thresholds:
    velocity: float = 0.0008
    acceleration: float = 0.0018
    liq_shock: float = 0.05
    apy_div: float = 0.03

    # emission control
    min_minutes_between_events_same_market: int = 30
    max_events_per_hour_global: int = 25

    # confirmation
    confirm_max_minutes: int = 45

    # baseline
    baseline_window_minutes: int = 120
    baseline_min_samples: int = 10
    level_dislocation_min_dist: float = 0.01  # 100 bps


DEFAULT_THRESHOLDS = Thresholds()


# -------------------- Types --------------------

@dataclass
class Baseline:
    spread_mean: float = 0.0
    spread_sigma: float = 0.0
    liquidity_median: float = 0.0
    yt_apy_median: float = 0.0
    implied_apy_median: float = 0.0


@dataclass
class MarketRuntimeState:
    last_spread: Optional[float] = None
    last_velocity: Optional[float] = None
    last_ts: Optional[str] = None
    last_event_ts: Optional[str] = None
    last_carry_risk_ts: Optional[str] = None


@dataclass
class RuntimeState:
    markets: Dict[str, MarketRuntimeState] = field(default_factory=dict)
    recent_event_ts: List[str] = field(default_factory=list)


# -------------------- Paths --------------------

ROOT = Path(__file__).resolve().parent

DATA_DIR = Path(getattr(cfg, "data_path", ROOT / "collector" / "logs" / "snapshots_v3"))
ALERTS_PATH = ROOT / getattr(cfg, "alerts_path", "alerts/regime_events.jsonl")
CARRY_RISK_PATH = ROOT / getattr(cfg, "carry_risk_flags_path", "alerts/carry_risk_flags.jsonl")

BASELINES_PATH = ROOT / getattr(cfg, "baselines_path", "state/baselines.json")
MARKET_STATE_PATH = ROOT / getattr(cfg, "market_state_path", "state/market_state.json")


def _ensure_dirs() -> None:
    (ROOT / "alerts").mkdir(parents=True, exist_ok=True)
    (ROOT / "state").mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


# -------------------- Helpers --------------------

def _iso(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_ts(ts_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(ts_str, TS_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _as_float(x: Any, default: float = math.nan) -> float:
    try:
        if x is None or x == "":
            return default
        v = float(x)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def _safe_read_json(path: Path) -> Any:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _safe_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _append_jsonl(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def market_key(chain_id: int, address: str) -> str:
    addr = (address or "").lower().strip()
    return f"{int(chain_id)}:{addr}" if addr else f"{int(chain_id)}:unknown"


def _normalize_yt_apy(snap: Dict[str, Any]) -> float:
    v = snap.get("yt_apy")
    if v is None:
        v = snap.get("long_yt_apy")
    return _as_float(v, 0.0)


# -------------------- Baselines --------------------

def load_baselines() -> Dict[str, Baseline]:
    d = _safe_read_json(BASELINES_PATH)
    mk = d.get("markets", {}) if isinstance(d, dict) else {}
    out: Dict[str, Baseline] = {}
    if isinstance(mk, dict):
        for k, bd in mk.items():
            if not isinstance(bd, dict):
                continue
            out[k] = Baseline(
                spread_mean=_as_float(bd.get("spread_mean"), 0.0),
                spread_sigma=_as_float(bd.get("spread_sigma"), 0.0),
                liquidity_median=_as_float(bd.get("liquidity_median"), 0.0),
                yt_apy_median=_as_float(bd.get("yt_apy_median"), 0.0),
                implied_apy_median=_as_float(bd.get("implied_apy_median"), 0.0),
            )
    return out


def save_baselines(baselines: Dict[str, Baseline]) -> None:
    _safe_write_json(BASELINES_PATH, {"ts": _iso(), "markets": {k: asdict(v) for k, v in baselines.items()}})


def _robust_sigma(values: List[float], center: float) -> float:
    absdev = [abs(x - center) for x in values]
    if not absdev:
        return 0.0
    mad = median(absdev)
    return float(1.4826 * mad)


def compute_local_baseline(
    series: List[Tuple[datetime, Path]],
    end_dt: datetime,
    chain_id: int,
    address: str,
    window_minutes: int,
    min_samples: int,
) -> Optional[Baseline]:
    if not series:
        return None
    start_dt = end_dt - timedelta(minutes=window_minutes)
    addr_l = (address or "").lower().strip()

    spreads: List[float] = []
    liqs: List[float] = []
    yts: List[float] = []
    imps: List[float] = []

    for dt, fp in reversed(series):
        if dt >= end_dt:
            continue
        if dt < start_dt:
            break
        snap = _safe_read_json(fp)
        if not isinstance(snap, dict):
            continue
        if _as_int(snap.get("chain_id") or 0, 0) != int(chain_id):
            continue
        saddr = str(snap.get("address") or "").lower().strip()
        if addr_l and saddr and saddr != addr_l:
            continue

        sp = _as_float(snap.get("spread"), math.nan)
        liq = _as_float(snap.get("liquidity"), math.nan)
        if math.isnan(sp) or math.isnan(liq):
            continue
        spreads.append(float(sp))
        liqs.append(float(liq))

        yt = _normalize_yt_apy(snap)
        imp = _as_float(snap.get("implied_apy"), math.nan)
        if not math.isnan(yt):
            yts.append(float(yt))
        if not math.isnan(imp):
            imps.append(float(imp))

    if len(spreads) < max(2, min_samples):
        return None

    m = float(median(spreads))
    sig = _robust_sigma(spreads, m)
    return Baseline(
        spread_mean=m,
        spread_sigma=sig,
        liquidity_median=float(median(liqs)) if liqs else 0.0,
        yt_apy_median=float(median(yts)) if yts else 0.0,
        implied_apy_median=float(median(imps)) if imps else 0.0,
    )


# -------------------- Runtime state --------------------

def load_runtime_state() -> RuntimeState:
    d = _safe_read_json(MARKET_STATE_PATH)
    mk = d.get("markets", {}) if isinstance(d, dict) else {}
    st = RuntimeState()
    if isinstance(mk, dict):
        for k, v in mk.items():
            if not isinstance(v, dict):
                continue
            st.markets[k] = MarketRuntimeState(
                last_spread=v.get("last_spread"),
                last_velocity=v.get("last_velocity"),
                last_ts=v.get("last_ts"),
                last_event_ts=v.get("last_event_ts"),
                last_carry_risk_ts=v.get("last_carry_risk_ts"),
            )
    st.recent_event_ts = d.get("recent_event_ts", []) if isinstance(d, dict) else []
    if not isinstance(st.recent_event_ts, list):
        st.recent_event_ts = []
    return st


def save_runtime_state(st: RuntimeState) -> None:
    _safe_write_json(
        MARKET_STATE_PATH,
        {"ts": _iso(), "markets": {k: asdict(v) for k, v in st.markets.items()}, "recent_event_ts": st.recent_event_ts},
    )


# -------------------- Snapshot indexing --------------------

def index_snapshots_by_market() -> Dict[str, List[Tuple[datetime, Path]]]:
    out: Dict[str, List[Tuple[datetime, Path]]] = {}
    for fp in DATA_DIR.glob("*.json"):
        stem = fp.stem
        if "_" not in stem:
            continue
        try:
            ts_str, market = stem.rsplit("_", 1)
            dt = _parse_ts(ts_str)
            if dt is None:
                continue
        except Exception:
            continue
        out.setdefault(market, []).append((dt, fp))
    for m in out:
        out[m].sort(key=lambda x: x[0])
    return out


# -------------------- Emission guards --------------------

def _cleanup_recent_events(st: RuntimeState) -> None:
    keep: List[str] = []
    now = datetime.now(timezone.utc)
    for t in st.recent_event_ts:
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if (now - dt).total_seconds() <= 3600:
                keep.append(t)
        except Exception:
            continue
    st.recent_event_ts = keep


def _minutes_between(a_iso: str, b_iso: str) -> Optional[float]:
    try:
        da = datetime.fromisoformat(a_iso.replace("Z", "+00:00"))
        db = datetime.fromisoformat(b_iso.replace("Z", "+00:00"))
        if da.tzinfo is None:
            da = da.replace(tzinfo=timezone.utc)
        if db.tzinfo is None:
            db = db.replace(tzinfo=timezone.utc)
        return abs((db - da).total_seconds()) / 60.0
    except Exception:
        return None


def should_emit(mkey: str, thresholds: Thresholds, st: RuntimeState) -> bool:
    _cleanup_recent_events(st)
    if len(st.recent_event_ts) >= thresholds.max_events_per_hour_global:
        return False
    rt = st.markets.get(mkey)
    if rt and rt.last_event_ts:
        mins = _minutes_between(rt.last_event_ts, _iso())
        if mins is not None and mins < thresholds.min_minutes_between_events_same_market:
            return False
    return True


# -------------------- Core computation --------------------

def compute_flags_and_metrics(
    snap: Dict[str, Any],
    baseline: Baseline,
    thresholds: Thresholds,
    rt: MarketRuntimeState,
) -> Tuple[str, List[str], Dict[str, Any], Dict[str, float]]:
    spread = _as_float(snap.get("spread"), 0.0)
    liq = _as_float(snap.get("liquidity"), 0.0)
    yt = _normalize_yt_apy(snap)
    imp = _as_float(snap.get("implied_apy"), 0.0)
    dtm = _as_float(snap.get("days_to_maturity"), 0.0)

    apy_div = abs(yt - imp)

    v = abs(spread - baseline.spread_mean)
    last_v = rt.last_velocity if rt.last_velocity is not None else v
    a = abs(v - last_v)

    liq_med = baseline.liquidity_median if baseline.liquidity_median > 0 else max(liq, 1.0)
    liq_shock = (liq - liq_med) / liq_med if liq_med else 0.0

    flags: List[str] = []
    if v >= thresholds.velocity:
        flags.append("velocity")
    if a >= thresholds.acceleration:
        flags.append("acceleration")
    if liq_shock >= thresholds.liq_shock:
        flags.append("liq_shock")
    elif liq_shock <= -thresholds.liq_shock:
        flags.append("liq_drop")
    if apy_div >= thresholds.apy_div:
        flags.append("apy_div")

    regime = "NORMAL"
    if len(flags) >= 2:
        if "liq_drop" in flags and ("velocity" not in flags) and ("apy_div" not in flags):
            regime = "NORMAL"
        else:
            regime = "DISLOCATION"

    dist0 = abs(spread - baseline.spread_mean)
    is_level = dist0 >= thresholds.level_dislocation_min_dist
    is_derivative = ("acceleration" in flags) and (not is_level)

    metrics: Dict[str, Any] = {
        "spread": float(spread),
        "spread_velocity": float(v),
        "spread_acceleration": float(a),
        "liquidity": float(liq),
        "liquidity_shock": float(liq_shock),
        "yt_apy": float(yt),
        "implied_apy": float(imp),
        "apy_divergence": float(apy_div),
        "days_to_maturity": float(dtm),
        "baseline_spread_mean": float(baseline.spread_mean),
        "dist_to_baseline": float(dist0),
        "is_level_dislocation": bool(is_level),
        "is_derivative_event": bool(is_derivative),
    }
    thr_used = {
        "velocity": float(thresholds.velocity),
        "acceleration": float(thresholds.acceleration),
        "liq_shock": float(thresholds.liq_shock),
        "apy_div": float(thresholds.apy_div),
    }
    return regime, flags, metrics, thr_used


def maybe_emit_carry_risk_flag(
    mkey: str,
    market: str,
    chain_id: int,
    address: str,
    snapshot_ts: str,
    metrics: Dict[str, Any],
    rt: MarketRuntimeState,
) -> Optional[Dict[str, Any]]:
    # Only for configured carry targets
    targets = set(getattr(cfg, "carry_target_market_keys", []) or [])
    if mkey not in targets:
        return None

    v = float(metrics.get("spread_velocity") or 0.0)
    a = float(metrics.get("spread_acceleration") or 0.0)
    liq_shock = float(metrics.get("liquidity_shock") or 0.0)
    apy_div = float(metrics.get("apy_divergence") or 0.0)

    reasons: List[str] = []
    if v >= float(getattr(cfg, "carry_risk_max_abs_velocity", 0.02)):
        reasons.append("velocity_high")
    if a >= float(getattr(cfg, "carry_risk_max_abs_acceleration", 0.003)):
        reasons.append("acceleration_high")
    if liq_shock <= -float(getattr(cfg, "carry_risk_liq_drop_abs", 0.08)):
        reasons.append("liquidity_drop")
    if apy_div >= float(getattr(cfg, "carry_risk_apy_divergence", 0.06)):
        reasons.append("apy_div_high")

    if not reasons:
        return None

    now_iso = _iso()
    # cooldown: don't spam same market risk flags too often
    if rt.last_carry_risk_ts:
        mins = _minutes_between(rt.last_carry_risk_ts, now_iso)
        if mins is not None and mins < 10:
            return None

    rt.last_carry_risk_ts = now_iso
    return {
        "ts": now_iso,
        "market": market,
        "market_key": mkey,
        "chain_id": chain_id,
        "address": (address or "").lower().strip(),
        "snapshot_ts": snapshot_ts,
        "risk": "REPRICE_ACTIVE",
        "reasons": reasons,
        "metrics": {
            "spread_velocity": v,
            "spread_acceleration": a,
            "liquidity_shock": liq_shock,
            "apy_divergence": apy_div,
        },
    }


# -------------------- Run one cycle --------------------

def observe_once(profile: str = "default") -> Dict[str, Any]:
    _ensure_dirs()
    thresholds = DEFAULT_THRESHOLDS  # profile kept for CLI compatibility
    baselines = load_baselines()
    st = load_runtime_state()
    idx = index_snapshots_by_market()

    events_emitted = 0
    carry_risk_emitted = 0
    markets_checked = 0
    now_iso = _iso()

    for market, series in idx.items():
        if len(series) < 2:
            continue

        # evaluate t-1 snapshot and require t within confirm window
        dt0, fp0 = series[-2]
        dt1, fp1 = series[-1]
        if (dt1 - dt0) > timedelta(minutes=thresholds.confirm_max_minutes):
            continue

        snap0 = _safe_read_json(fp0)
        if not isinstance(snap0, dict):
            continue

        chain_id = _as_int(snap0.get("chain_id") or 0, 0)
        address = str(snap0.get("address") or "")
        mkey = market_key(chain_id, address)

        # compute / refresh local baseline from series
        b_local = compute_local_baseline(
            series=series,
            end_dt=dt0,
            chain_id=chain_id,
            address=address,
            window_minutes=thresholds.baseline_window_minutes,
            min_samples=thresholds.baseline_min_samples,
        )

        if b_local is None:
            b_local = baselines.get(mkey)

        if b_local is None:
            # hard fallback: baseline = current snapshot
            b_local = Baseline(
                spread_mean=_as_float(snap0.get("spread"), 0.0),
                spread_sigma=0.0,
                liquidity_median=_as_float(snap0.get("liquidity"), 0.0),
                yt_apy_median=_normalize_yt_apy(snap0),
                implied_apy_median=_as_float(snap0.get("implied_apy"), 0.0),
            )

        baselines[mkey] = b_local  # always non-null

        rt = st.markets.get(mkey) or MarketRuntimeState()
        regime, flags, metrics, thr_used = compute_flags_and_metrics(snap0, b_local, thresholds, rt)

        # carry risk flags (separate stream)
        risk_evt = maybe_emit_carry_risk_flag(
            mkey=mkey,
            market=market,
            chain_id=chain_id,
            address=address,
            snapshot_ts=dt0.strftime(TS_FMT),
            metrics=metrics,
            rt=rt,
        )
        if risk_evt is not None:
            _append_jsonl(CARRY_RISK_PATH, risk_evt)
            carry_risk_emitted += 1

        # update runtime state
        rt.last_velocity = float(metrics["spread_velocity"])
        rt.last_spread = float(metrics["spread"])
        rt.last_ts = now_iso
        st.markets[mkey] = rt

        markets_checked += 1

        if regime != "DISLOCATION":
            continue

        if not should_emit(mkey, thresholds, st):
            continue

        event = {
            "ts": now_iso,
            "market": market,
            "market_key": mkey,
            "chain_id": chain_id,
            "address": address,
            "regime": "DISLOCATION",
            "flags": ",".join(flags),
            "snapshot_ts": dt0.strftime(TS_FMT),
            "confirm_snapshot_ts": dt1.strftime(TS_FMT),
            "confirm_snapshot_file": str(fp1),
            "metrics": metrics,
            "thresholds": thr_used,
            "baseline": asdict(b_local),
            "snapshot_file": str(fp0),
        }

        _append_jsonl(ALERTS_PATH, event)
        st.recent_event_ts.append(now_iso)
        rt.last_event_ts = now_iso
        st.markets[mkey] = rt
        events_emitted += 1

    save_runtime_state(st)
    save_baselines(baselines)

    return {
        "ts": now_iso,
        "markets_checked": markets_checked,
        "events_emitted": events_emitted,
        "carry_risk_emitted": carry_risk_emitted,
        "data_dir": str(DATA_DIR),
    }


def main() -> None:
    import sys
    profile = "default"
    if len(sys.argv) > 1:
        profile = sys.argv[1].strip().lower()
    res = observe_once(profile=profile)
    print("[observer] DONE", res)


if __name__ == "__main__":
    main()
