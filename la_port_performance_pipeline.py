"""
🚢 Los Angeles Port Performance Monitor
GitHub-safe pipeline script (no credentials in code)

What it does
- Extracts weekly operational KPIs from Port Optimizer / Signal (Los Angeles views)
- Builds clean KPI tables (volume pressure, terminal congestion, outgate stress, berth flag, health summary)
- Writes outputs to CSV (default)
- Optionally loads outputs to Azure SQL (if env vars are provided)

Security
- DO NOT hardcode SQL credentials or session cookies in this file.
- Provide secrets via environment variables (local .env not committed, or GitHub Actions secrets).
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
import requests

# Optional SQL load (only needed if you set LOAD_TO_SQL=true)
try:
    import sqlalchemy as sa
    import urllib.parse
except Exception:
    sa = None


# -------------------------
# CONFIG
# -------------------------

@dataclass
class Config:
    # Endpoints (Signal / Port Optimizer)
    URL_WEEKLY: str = os.getenv(
        "PO_URL_WEEKLY",
        "https://signal.portoptimizer.com/api/v1/signal/_search?_c=WeeklyVolumesComparison",
    )
    URL_TERMINAL: str = os.getenv(
        "PO_URL_TERMINAL",
        "https://signal.portoptimizer.com/api/v1/signal/signalLoadEmpty/_search?_c=ContainersAtTerminalData",
    )
    URL_OUTGATED: str = os.getenv(
        "PO_URL_OUTGATED",
        "https://signal.portoptimizer.com/api/v1/signal/signalDaysAfterDischarge/multi/_search?_c=FetchOutgatedContainerMetricsData",
    )
    URL_BERTH: str = os.getenv(
        "PO_URL_BERTH",
        "https://signal.portoptimizer.com/api/v1/signal/signalDashboardStd/_search?_c=FetchQuickviewDashboardBerthData",
    )

    # Output
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "outputs")
    PREFIX: str = os.getenv("OUTPUT_PREFIX", "la")  # la_*

    # KPI thresholds (tune as needed)
    VOLUME_HIGH: float = float(os.getenv("VOLUME_HIGH", "1.15"))
    VOLUME_LOW: float = float(os.getenv("VOLUME_LOW", "0.90"))

    TERMINAL_THRESH_LOADED: float = float(os.getenv("TERMINAL_THRESH_LOADED", "0.25"))
    TERMINAL_THRESH_EMPTY: float = float(os.getenv("TERMINAL_THRESH_EMPTY", "0.50"))
    TERMINAL_CONGESTED_BUCKETS: Tuple[str, ...] = ("9-12 Days", "13+ Days")  # proxy for "9+"

    OUTGATE_SLOW_BUCKETS: Tuple[str, ...] = ("5-8 Days", "9-12 Days", "13+ Days")
    OUTGATE_STRESS_THRESHOLD: float = float(os.getenv("OUTGATE_STRESS_THRESHOLD", "0.40"))

    BERTH_HIGH_DAYS: float = float(os.getenv("BERTH_HIGH_DAYS", "2.0"))

    # Request behavior
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "40"))
    RETRIES: int = int(os.getenv("REQUEST_RETRIES", "3"))
    BACKOFF: float = float(os.getenv("REQUEST_BACKOFF", "1.4"))
    SLEEP_BETWEEN_CALLS_SEC: float = float(os.getenv("SLEEP_BETWEEN_CALLS_SEC", "0.2"))

    # SQL (optional)
    LOAD_TO_SQL: bool = os.getenv("LOAD_TO_SQL", "false").lower() == "true"
    AZ_SQL_SERVER: str = os.getenv("AZ_SQL_SERVER", "")
    AZ_SQL_DB: str = os.getenv("AZ_SQL_DB", "")
    AZ_SQL_USER: str = os.getenv("AZ_SQL_USER", "")
    AZ_SQL_PASSWORD: str = os.getenv("AZ_SQL_PASSWORD", "")
    AZ_SQL_DRIVER: str = os.getenv("AZ_SQL_DRIVER", "ODBC Driver 17 for SQL Server")


CFG = Config()

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://signal.portoptimizer.com",
    "Referer": "https://signal.portoptimizer.com/",
    "User-Agent": "Mozilla/5.0",
}


# -------------------------
# Secrets handling (cookies)
# -------------------------

def load_cookies_from_env() -> Dict[str, str]:
    """
    Signal / Port Optimizer access may rely on session cookies.
    Provide cookies via either:
      1) PO_COOKIES_JSON='{"cookie_name":"value",...}'
      2) Individual vars: PO_COOKIE_VISID, PO_COOKIE_NLBI, PO_COOKIE_INCAP

    NOTE: Do NOT commit cookies to GitHub.
    """
    cj = os.getenv("PO_COOKIES_JSON", "").strip()
    if cj:
        try:
            parsed = json.loads(cj)
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items()}
        except Exception:
            raise ValueError("PO_COOKIES_JSON is set but not valid JSON.")
    # fallback: common cookie names used by Incapsula protections
    visid = os.getenv("PO_COOKIE_VISID", "").strip()
    nlbi = os.getenv("PO_COOKIE_NLBI", "").strip()
    incap = os.getenv("PO_COOKIE_INCAP", "").strip()
    cookies = {}
    if visid:
        cookies["visid_incap_2599010"] = visid
    if nlbi:
        cookies["nlbi_2599010"] = nlbi
    if incap:
        cookies["incap_ses_1705_2599010"] = incap
    return cookies


# -------------------------
# Utility
# -------------------------

def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz=timezone.utc)

def get_path(d: dict, path: list, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur

def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def post_json(url: str, payload: dict, name: str, cookies: Dict[str, str]) -> dict:
    last_err = None
    with requests.Session() as s:
        for i in range(CFG.RETRIES):
            try:
                r = s.post(url, headers=HEADERS, cookies=cookies, json=payload, timeout=CFG.REQUEST_TIMEOUT)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"{name} transient HTTP {r.status_code}: {r.text[:200]}")
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_err = e
                time.sleep((CFG.BACKOFF ** i))
    raise RuntimeError(f"{name} failed after {CFG.RETRIES} retries: {last_err}")


# -------------------------
# Extractors
# -------------------------

def extract_weekly_volumes(cookies: Dict[str, str], days_back: int = 90) -> pd.DataFrame:
    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=days_back)

    payload = {
        "fromDate": from_dt.strftime("%Y-%m-%dT00:00:00"),
        "toDate": to_dt.strftime("%Y-%m-%dT23:59:59"),
        "searchParameters": [
            {
                "key": "byDateHistogram",
                "children": [{"key": "bySumInboundFullContainers", "children": []}],
                "histogramInterval": "1w",
                "timeZone": "America/Los_Angeles",
                "offset": "-1d",
                "filterByMinCount": 0,
            }
        ],
    }

    data = post_json(CFG.URL_WEEKLY, payload, "WEEKLY", cookies)

    buckets = get_path(data, ["response", "aggregations", "byDateHistogram", "buckets"], default=[]) or []
    rows = []
    for b in buckets:
        rows.append(
            {
                "week_start_local": b.get("key_as_string"),
                "week_start_epoch_ms": b.get("key"),
                "inbound_full_containers": get_path(b, ["bySumInboundFullContainers", "value"]),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["week_start_utc"] = pd.to_datetime(df["week_start_epoch_ms"], unit="ms", utc=True, errors="coerce")
        df["week_start_date"] = df["week_start_utc"].dt.date

    df["extraction_ts_utc"] = now_utc()
    df["source"] = "signal.portoptimizer.com"
    return df


def normalize_load_empty(x: str) -> str:
    s = str(x).lower()
    if "empty" in s:
        return "EMPTY"
    if "load" in s:
        return "LOADED"
    return str(x).upper()


def extract_terminal_aging(cookies: Dict[str, str], days_back: int = 90) -> pd.DataFrame:
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days_back)

    payload = {
        "fromDate": from_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "toDate": to_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "searchParameters": [
            {
                "key": "byLoadEmpty",
                "children": [{"key": "byDischargedAndNotOutgated", "children": []}],
                "filters": [{"filterKey": "shipmentStatusCd", "filterValues": ["UV", "I", "AR", "UR"]}],
            }
        ],
    }

    data = post_json(CFG.URL_TERMINAL, payload, "TERMINAL", cookies)

    buckets = get_path(data, ["response", "aggregations", "byLoadEmpty", "buckets"], default=[]) or []
    rows = []
    for lb in buckets:
        load_key = lb.get("key_as_string") or lb.get("key")
        inner = get_path(lb, ["byDischargedAndNotOutgated", "buckets"], default=[]) or []
        for x in inner:
            rows.append(
                {
                    "load_empty": normalize_load_empty(load_key),
                    "age_bucket": x.get("key_as_string") or x.get("key"),
                    "containers": x.get("doc_count", 0),
                }
            )

    df = pd.DataFrame(rows)
    df["fromDate"] = payload["fromDate"]
    df["toDate"] = payload["toDate"]
    df["extraction_ts_utc"] = now_utc()
    df["source"] = "signal.portoptimizer.com"
    return df


def extract_outgated_status_only(cookies: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      df_outgated_age_status: status_code + status_label + age_bucket + containers (local+rail summed)
      df_outgated_dwell: avg dwell days by status (30D/60D/90D)
    """
    to_dt = datetime.now()

    payload = {
        "searchRequests": [
            {
                "fromDate": (to_dt - timedelta(days=30)).strftime("%Y-%m-%d"),
                "toDate": to_dt.strftime("%Y-%m-%dT23:59:59"),
                "searchParameters": [
                    {
                        "key": "byStatus",
                        "children": [
                            {"key": "byDischargedAndOutgatedByLocal", "children": []},
                            {"key": "byDischargedAndOutgatedByRail", "children": []},
                        ],
                    }
                ],
            },
            {
                "fromDate": (to_dt - timedelta(days=30)).strftime("%Y-%m-%d"),
                "toDate": to_dt.strftime("%Y-%m-%dT23:59:59"),
                "searchParameters": [{"key": "byStatus", "children": [{"key": "byAverageDischargedAndOutgated", "children": []}]}],
            },
            {
                "fromDate": (to_dt - timedelta(days=60)).strftime("%Y-%m-%d"),
                "toDate": to_dt.strftime("%Y-%m-%dT23:59:59"),
                "searchParameters": [{"key": "byStatus", "children": [{"key": "byAverageDischargedAndOutgated", "children": []}]}],
            },
            {
                "fromDate": (to_dt - timedelta(days=90)).strftime("%Y-%m-%d"),
                "toDate": to_dt.strftime("%Y-%m-%dT23:59:59"),
                "searchParameters": [{"key": "byStatus", "children": [{"key": "byAverageDischargedAndOutgated", "children": []}]}],
            },
        ]
    }

    data = post_json(CFG.URL_OUTGATED, payload, "OUTGATED", cookies)

    responses = data if isinstance(data, list) else (data.get("responses") or data.get("searchResponses") or data.get("results"))
    if not isinstance(responses, list):
        raise ValueError("Outgated multi response did not contain a list of responses.")

    lookup = get_path(responses[0], ["lookuphelp", "byStatus"], default={}) or {}

    def status_label(code: str) -> str:
        return lookup.get(code, code)

    age_aggs = get_path(responses[0], ["response", "aggregations"], default={}) or get_path(responses[0], ["aggregations"], default={}) or {}
    status_buckets = get_path(age_aggs, ["byStatus", "buckets"], default=[]) or []

    rows = []
    for sb in status_buckets:
        code = sb.get("key_as_string") or sb.get("key") or "UNKNOWN"
        local_b = get_path(sb, ["byDischargedAndOutgatedByLocal", "buckets"], default=[]) or []
        rail_b = get_path(sb, ["byDischargedAndOutgatedByRail", "buckets"], default=[]) or []

        bucket_map: Dict[str, int] = {}
        for x in local_b:
            k = x.get("key_as_string") or x.get("key")
            bucket_map[k] = bucket_map.get(k, 0) + int(x.get("doc_count", 0))
        for x in rail_b:
            k = x.get("key_as_string") or x.get("key")
            bucket_map[k] = bucket_map.get(k, 0) + int(x.get("doc_count", 0))

        for k, v in bucket_map.items():
            rows.append({"status_code": code, "status_label": status_label(code), "age_bucket": k, "containers": v})

    df_age_status = pd.DataFrame(rows)
    df_age_status["fromDate"] = payload["searchRequests"][0]["fromDate"]
    df_age_status["toDate"] = payload["searchRequests"][0]["toDate"]
    df_age_status["extraction_ts_utc"] = now_utc()
    df_age_status["source"] = "signal.portoptimizer.com"

    rows_dwell = []
    for idx, win in zip([1, 2, 3], ["30D", "60D", "90D"]):
        aggs = get_path(responses[idx], ["response", "aggregations"], default={}) or get_path(responses[idx], ["aggregations"], default={}) or {}
        sbuckets = get_path(aggs, ["byStatus", "buckets"], default=[]) or []
        for sb in sbuckets:
            code = sb.get("key_as_string") or sb.get("key") or "UNKNOWN"
            avg_val = get_path(sb, ["byAverageDischargedAndOutgated", "value"])
            rows_dwell.append(
                {
                    "status_code": code,
                    "status_label": status_label(code),
                    "window": win,
                    "fromDate": payload["searchRequests"][idx]["fromDate"],
                    "toDate": payload["searchRequests"][idx]["toDate"],
                    "avg_dwell_days": avg_val,
                }
            )

    df_dwell = pd.DataFrame(rows_dwell)
    df_dwell["extraction_ts_utc"] = now_utc()
    df_dwell["source"] = "signal.portoptimizer.com"

    return df_age_status, df_dwell


def extract_berth(cookies: Dict[str, str], days_back: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(days=days_back)

    payload = {
        "fromDate": from_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "toDate": to_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "searchParameters": [
            {"key": "byVesselsAtBerth", "children": [], "size": 500, "filterByMinCount": 50},
            {"key": "byAverageTimeAtBerth", "children": []},
        ],
    }

    data = post_json(CFG.URL_BERTH, payload, "BERTH", cookies)
    aggs = get_path(data, ["response", "aggregations"], default={}) or {}

    rows = []
    for b in get_path(aggs, ["byVesselsAtBerth", "buckets"], default=[]) or []:
        rows.append(
            {
                "vessel_key": b.get("key_as_string") or b.get("key"),
                "doc_count": b.get("doc_count", 0),
                "fromDate": payload["fromDate"],
                "toDate": payload["toDate"],
            }
        )

    df_vessels = pd.DataFrame(rows)
    if not df_vessels.empty:
        df_vessels = df_vessels.sort_values("doc_count", ascending=False).reset_index(drop=True)
    df_vessels["extraction_ts_utc"] = now_utc()
    df_vessels["source"] = "signal.portoptimizer.com"

    avg_val = get_path(aggs, ["byAverageTimeAtBerth", "value"])
    df_avg = pd.DataFrame(
        [
            {
                "avg_time_at_berth_days": avg_val,
                "fromDate": payload["fromDate"],
                "toDate": payload["toDate"],
                "extraction_ts_utc": now_utc(),
                "source": "signal.portoptimizer.com",
            }
        ]
    )

    return df_vessels, df_avg


# -------------------------
# KPI tables
# -------------------------

def kpi_volume_pressure(df_weekly: pd.DataFrame) -> pd.DataFrame:
    df = df_weekly.copy()
    if df.empty:
        return df
    df = df.sort_values("week_start_utc")
    df["rolling_4w_avg_teu"] = df["inbound_full_containers"].rolling(window=4, min_periods=1).mean()
    df["volume_pressure_index"] = df["inbound_full_containers"] / df["rolling_4w_avg_teu"]
    df["volume_pressure_flag"] = np.where(
        df["volume_pressure_index"] >= CFG.VOLUME_HIGH,
        "HIGH",
        np.where(df["volume_pressure_index"] <= CFG.VOLUME_LOW, "LOW", "NORMAL"),
    )
    return df[
        [
            "week_start_date",
            "week_start_utc",
            "inbound_full_containers",
            "rolling_4w_avg_teu",
            "volume_pressure_index",
            "volume_pressure_flag",
            "extraction_ts_utc",
            "source",
        ]
    ]


def kpi_terminal_congestion(df_terminal_age: pd.DataFrame) -> pd.DataFrame:
    if df_terminal_age.empty:
        return df_terminal_age

    df = df_terminal_age.copy()
    df["containers"] = pd.to_numeric(df["containers"], errors="coerce").fillna(0)
    df["is_congested"] = df["age_bucket"].isin(CFG.TERMINAL_CONGESTED_BUCKETS)

    total = df.groupby("load_empty", as_index=False)["containers"].sum().rename(columns={"containers": "total_containers"})
    congested = (
        df.loc[df["is_congested"]]
        .groupby("load_empty", as_index=False)["containers"]
        .sum()
        .rename(columns={"containers": "congested_containers"})
    )

    out = total.merge(congested, on="load_empty", how="left")
    out["congested_containers"] = out["congested_containers"].fillna(0)
    out["congestion_pct"] = np.where(
        out["total_containers"] > 0, out["congested_containers"] / out["total_containers"], np.nan
    )

    out["threshold"] = np.where(out["load_empty"] == "EMPTY", CFG.TERMINAL_THRESH_EMPTY, CFG.TERMINAL_THRESH_LOADED)
    out["congestion_flag"] = np.where(out["congestion_pct"] >= out["threshold"], "HIGH", "NORMAL")

    out["congested_buckets"] = ", ".join(CFG.TERMINAL_CONGESTED_BUCKETS)
    out["extraction_ts_utc"] = now_utc()
    out["source"] = "signal.portoptimizer.com"
    return out


def kpi_outgate_stress_by_status(df_outgated_age_status: pd.DataFrame) -> pd.DataFrame:
    """STATUS-only KPI with correct denominators per status."""
    if df_outgated_age_status.empty:
        return df_outgated_age_status

    df = df_outgated_age_status.copy()
    df["containers"] = pd.to_numeric(df["containers"], errors="coerce").fillna(0)
    df["is_slow"] = df["age_bucket"].isin(CFG.OUTGATE_SLOW_BUCKETS)

    total = (
        df.groupby(["status_code", "status_label"], as_index=False)["containers"]
        .sum()
        .rename(columns={"containers": "total_containers"})
    )
    slow = (
        df.loc[df["is_slow"]]
        .groupby(["status_code", "status_label"], as_index=False)["containers"]
        .sum()
        .rename(columns={"containers": "slow_containers"})
    )

    out = total.merge(slow, on=["status_code", "status_label"], how="left")
    out["slow_containers"] = out["slow_containers"].fillna(0)
    out["slow_pct"] = np.where(out["total_containers"] > 0, out["slow_containers"] / out["total_containers"], np.nan)
    out["stress_flag"] = np.where(out["slow_pct"] >= CFG.OUTGATE_STRESS_THRESHOLD, "HIGH", "NORMAL")

    out["slow_buckets"] = ", ".join(CFG.OUTGATE_SLOW_BUCKETS)
    out["threshold"] = CFG.OUTGATE_STRESS_THRESHOLD
    out["extraction_ts_utc"] = now_utc()
    out["source"] = "signal.portoptimizer.com"
    return out.sort_values("slow_pct", ascending=False)


def kpi_berth_flag(df_avg_berth: pd.DataFrame) -> pd.DataFrame:
    if df_avg_berth.empty:
        return df_avg_berth
    df = df_avg_berth.copy()
    df["berth_flag"] = np.where(df["avg_time_at_berth_days"] >= CFG.BERTH_HIGH_DAYS, "HIGH", "NORMAL")
    df["threshold_days"] = CFG.BERTH_HIGH_DAYS
    return df


def kpi_health_summary(
    df_kpi_volume: pd.DataFrame,
    df_kpi_terminal: pd.DataFrame,
    df_kpi_outgate: pd.DataFrame,
    df_kpi_berth: pd.DataFrame,
) -> pd.DataFrame:
    vol_flag = None
    if df_kpi_volume is not None and not df_kpi_volume.empty:
        vol_flag = df_kpi_volume.sort_values("week_start_utc").iloc[-1]["volume_pressure_flag"]

    loaded_flag = None
    empty_flag = None
    if df_kpi_terminal is not None and not df_kpi_terminal.empty:
        row_loaded = df_kpi_terminal.loc[df_kpi_terminal["load_empty"] == "LOADED"]
        row_empty = df_kpi_terminal.loc[df_kpi_terminal["load_empty"] == "EMPTY"]
        loaded_flag = row_loaded.iloc[0]["congestion_flag"] if not row_loaded.empty else None
        empty_flag = row_empty.iloc[0]["congestion_flag"] if not row_empty.empty else None

    out_worst = None
    if df_kpi_outgate is not None and not df_kpi_outgate.empty:
        out_worst = "HIGH" if (df_kpi_outgate["stress_flag"] == "HIGH").any() else "NORMAL"

    berth_flag = None
    if df_kpi_berth is not None and not df_kpi_berth.empty:
        berth_flag = df_kpi_berth.iloc[0]["berth_flag"]

    return pd.DataFrame(
        [
            {
                "volume_pressure_flag_latest": vol_flag,
                "terminal_loaded_flag": loaded_flag,
                "terminal_empty_flag": empty_flag,
                "outgate_stress_flag_worst": out_worst,
                "berth_flag": berth_flag,
                "extraction_ts_utc": now_utc(),
                "source": "signal.portoptimizer.com",
            }
        ]
    )


# -------------------------
# Output helpers
# -------------------------

def table_name(name: str) -> str:
    """Prefix tables consistently for GitHub/public use."""
    return f"{CFG.PREFIX}_{name}"

def save_csv(df: pd.DataFrame, name: str) -> str:
    ensure_output_dir(CFG.OUTPUT_DIR)
    path = os.path.join(CFG.OUTPUT_DIR, f"{table_name(name)}.csv")
    df.to_csv(path, index=False)
    return path

def build_engine() -> "sa.Engine":
    if sa is None:
        raise RuntimeError("SQLAlchemy not installed. Install sqlalchemy + pyodbc to enable SQL load.")
    missing = [k for k in ("AZ_SQL_SERVER", "AZ_SQL_DB", "AZ_SQL_USER", "AZ_SQL_PASSWORD") if not getattr(CFG, k)]
    if missing:
        raise ValueError(f"Missing SQL env vars: {missing}")

    odbc = (
        f"Driver={{{CFG.AZ_SQL_DRIVER}}};"
        f"Server=tcp:{CFG.AZ_SQL_SERVER},1433;"
        f"Database={CFG.AZ_SQL_DB};"
        f"Uid={CFG.AZ_SQL_USER};"
        f"Pwd={CFG.AZ_SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return sa.create_engine("mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc), fast_executemany=True)

def load_table_sql(df: pd.DataFrame, name: str, engine: "sa.Engine", schema: str = "dbo") -> None:
    # Replace is fine for a demo project; you can switch to append+merge later.
    df.to_sql(name=table_name(name), con=engine, schema=schema, if_exists="replace", index=False)


# -------------------------
# Pipeline runner
# -------------------------

def run_pipeline() -> Dict[str, pd.DataFrame]:
    cookies = load_cookies_from_env()
    if not cookies:
        print("⚠️ No cookies provided. Set PO_COOKIES_JSON or PO_COOKIE_* env vars to enable extraction.")
        print("   (See README / .env.example).")
    # Extract
    df_weekly_raw = extract_weekly_volumes(cookies)
    time.sleep(CFG.SLEEP_BETWEEN_CALLS_SEC)

    df_terminal_age = extract_terminal_aging(cookies)
    time.sleep(CFG.SLEEP_BETWEEN_CALLS_SEC)

    df_outgated_age_status, df_outgated_dwell = extract_outgated_status_only(cookies)
    time.sleep(CFG.SLEEP_BETWEEN_CALLS_SEC)

    df_vessels_at_berth, df_avg_berth = extract_berth(cookies)

    # KPIs
    df_kpi_volume = kpi_volume_pressure(df_weekly_raw)
    df_kpi_terminal = kpi_terminal_congestion(df_terminal_age)
    df_kpi_outgate = kpi_outgate_stress_by_status(df_outgated_age_status)
    df_kpi_berth = kpi_berth_flag(df_avg_berth)
    df_kpi_health = kpi_health_summary(df_kpi_volume, df_kpi_terminal, df_kpi_outgate, df_kpi_berth)

    return {
        "kpi_volume_pressure_weekly": df_kpi_volume,
        "kpi_terminal_congestion": df_kpi_terminal,
        "kpi_slow_containers_stress": df_kpi_outgate,
        "kpi_dwell_trend_windows": df_outgated_dwell,
        "kpi_berth_activity": df_kpi_berth,
        "kpi_health_summary": df_kpi_health,
        "raw_weekly_volumes": df_weekly_raw,
        "raw_terminal_aging": df_terminal_age,
        "raw_outgated_age_status": df_outgated_age_status,
        "raw_vessels_at_berth": df_vessels_at_berth,
        "raw_avg_time_at_berth": df_avg_berth,
    }

def main() -> int:
    print("🚢 Running Los Angeles Port Performance Monitor...")
    tables = run_pipeline()

    # Save CSVs
    for name, df in tables.items():
        if df is None or df.empty:
            continue
        path = save_csv(df, name)
        print(f"✅ Saved: {path} ({len(df)} rows)")

    # Optional SQL load
    if CFG.LOAD_TO_SQL:
        engine = build_engine()
        with engine.begin() as conn:
            conn.execute(sa.text("SELECT 1"))
        for name, df in tables.items():
            if df is None or df.empty:
                continue
            load_table_sql(df, name, engine)
            print(f"☁️ Loaded to Azure SQL: {table_name(name)}")

    print("✅ Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
