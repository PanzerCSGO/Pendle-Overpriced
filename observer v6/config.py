# FILE: config.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Config:
    # --------------------
    # Paths
    # --------------------
    data_path: str = r"./collector/logs/snapshots_v3"
    snapshots_csv_path: str = r"./collector/logs/snapshots_v3.csv"

    baselines_path: str = r"./state/baselines.json"
    market_state_path: str = r"./state/market_state.json"
    exec_state_path: str = r"./state/exec_state.json"

    alerts_path: str = r"./alerts/regime_events.jsonl"
    tradeable_candidates_path: str = r"./alerts/tradeable_candidates.jsonl"
    mr_candidates_report_path: str = r"./alerts/mr_candidates_report.jsonl"

    # Carry-risk flags emitted by observer (used to avoid entering while repricing is active)
    carry_risk_flags_path: str = r"./alerts/carry_risk_flags.jsonl"

    # --------------------
    # Overpriced YT Carry (NO TX)
    # --------------------
    # Primary identity: "<chain_id>:<address_lower>"
    carry_target_market_keys: List[str] = field(default_factory=lambda: [
        "42161:0x44688047c0a8bab55bf0b19f9d47d567b2c3319f",  # stk-ePendle (Arbitrum)
        "42161:0xc24ffebd4fdba787df94f5089f0fabc252116ab7",  # mPENDLE (Arbitrum)
    ])

    # spread = implied_apy - yt_apy
    # YT overpriced => spread negative. Condition: spread <= -min_overpricing_abs
    carry_min_overpricing_abs: Dict[str, float] = field(default_factory=lambda: {
        "42161:0x44688047c0a8bab55bf0b19f9d47d567b2c3319f": 0.05,
        "42161:0xc24ffebd4fdba787df94f5089f0fabc252116ab7": 0.01,
    })

    # Liquidity filter (USD)
    carry_min_liquidity_usd: Dict[str, float] = field(default_factory=lambda: {
        "42161:0x44688047c0a8bab55bf0b19f9d47d567b2c3319f": 1_500_000.0,
        "42161:0xc24ffebd4fdba787df94f5089f0fabc252116ab7": 2_000_000.0,
    })

    carry_min_days_to_maturity: float = 30.0
    carry_max_days_to_maturity: float = 240.0

    # Calm market filters based on spread drift from snapshots CSV
    carry_max_abs_spread_drift_10m: float = 0.010
    carry_max_abs_spread_drift_30m: float = 0.020

    # Cooldown per market for candidate emission
    carry_min_minutes_between_candidates: int = 60

    # Risk flags: execution_bot blocks if observer flagged repricing recently
    carry_block_if_risk_flag_within_minutes: int = 60
    carry_risk_max_abs_velocity: float = 0.02       # spread_velocity beyond this => repricing active
    carry_risk_max_abs_acceleration: float = 0.003  # spread_acceleration beyond this => repricing active
    carry_risk_liq_drop_abs: float = 0.08           # liquidity_shock <= -0.08 => avoid
    carry_risk_apy_divergence: float = 0.06         # apy_divergence >= 0.06 => avoid

    verbose: bool = True
