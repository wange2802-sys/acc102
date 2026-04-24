from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


COLUMN_CANDIDATES = {
    "date": ["date", "caldt", "datadate", "tradedate"],
    "ticker": ["ticker", "tic", "symbol"],
    "identifier": ["permno", "permco", "gvkey", "cusip", "ncusip"],
    "price": ["prc", "price", "close", "altprc"],
    "return": ["ret", "retx", "return", "daily_return"],
    "volume": ["vol", "volume"],
    "shares": ["shrout", "shares_outstanding", "csho"],
    "market_cap": ["market_cap", "mktcap", "me"],
}


def _clean_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            return str(value)
    return value


def preview_records(df: pd.DataFrame, rows: int = 10) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in df.head(rows).to_dict(orient="records"):
        records.append({key: _clean_scalar(value) for key, value in row.items()})
    return records


def export_summary(summary: dict[str, Any], output_path: str | Path) -> Path:
    path = Path(output_path).expanduser().resolve()
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def export_dataframe(df: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path).expanduser().resolve()
    df.to_csv(path, index=False)
    return path


def detect_stock_columns(df: pd.DataFrame) -> dict[str, str | None]:
    lower_map = {column.lower(): column for column in df.columns}
    detected: dict[str, str | None] = {}
    for role, names in COLUMN_CANDIDATES.items():
        detected[role] = next((lower_map[name] for name in names if name in lower_map), None)
    return detected


def prepare_stock_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str | None]]:
    prepared = df.copy()
    columns = detect_stock_columns(prepared)

    date_col = columns["date"]
    if date_col:
        prepared[date_col] = pd.to_datetime(prepared[date_col], errors="coerce")

    for role in ["price", "return", "volume", "shares", "market_cap"]:
        column = columns[role]
        if column:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    price_col = columns["price"]
    shares_col = columns["shares"]
    market_cap_col = columns["market_cap"]
    if not market_cap_col and price_col and shares_col:
        prepared["derived_market_cap"] = prepared[price_col].abs() * prepared[shares_col]
        columns["market_cap"] = "derived_market_cap"

    if price_col:
        prepared[price_col] = prepared[price_col].abs()

    if date_col:
        sort_keys = [date_col]
        ticker_col = columns["ticker"] or columns["identifier"]
        if ticker_col:
            sort_keys.insert(0, ticker_col)
        prepared = prepared.sort_values(sort_keys).reset_index(drop=True)

    return prepared, columns


def summarize_stock_data(df: pd.DataFrame) -> dict[str, Any]:
    prepared, columns = prepare_stock_frame(df)
    ticker_col = columns["ticker"] or columns["identifier"]
    date_col = columns["date"]
    return_col = columns["return"]
    volume_col = columns["volume"]

    summary: dict[str, Any] = {
        "shape": prepared.shape,
        "columns": prepared.columns.tolist(),
        "detected_columns": columns,
        "missing_total": int(prepared.isna().sum().sum()),
        "duplicate_rows": int(prepared.duplicated().sum()),
    }

    if ticker_col:
        summary["ticker_count"] = int(prepared[ticker_col].dropna().nunique())

    if date_col:
        valid_dates = prepared[date_col].dropna()
        summary["date_range"] = {
            "start": valid_dates.min().strftime("%Y-%m-%d") if not valid_dates.empty else None,
            "end": valid_dates.max().strftime("%Y-%m-%d") if not valid_dates.empty else None,
        }

    if return_col:
        valid_returns = prepared[return_col].dropna()
        if not valid_returns.empty:
            summary["returns"] = {
                "avg_daily_return_pct": round(float(valid_returns.mean() * 100), 4),
                "volatility_pct": round(float(valid_returns.std() * 100), 4),
                "best_day_pct": round(float(valid_returns.max() * 100), 4),
                "worst_day_pct": round(float(valid_returns.min() * 100), 4),
                "positive_days_ratio": round(float((valid_returns > 0).mean()), 4),
            }

    if volume_col:
        valid_volume = prepared[volume_col].dropna()
        if not valid_volume.empty:
            summary["volume"] = {
                "avg_volume": round(float(valid_volume.mean()), 2),
                "median_volume": round(float(valid_volume.median()), 2),
                "max_volume": round(float(valid_volume.max()), 2),
            }

    return summary


def build_ticker_summary(df: pd.DataFrame) -> pd.DataFrame:
    prepared, columns = prepare_stock_frame(df)
    ticker_col = columns["ticker"] or columns["identifier"]
    date_col = columns["date"]
    price_col = columns["price"]
    return_col = columns["return"]
    volume_col = columns["volume"]
    market_cap_col = columns["market_cap"]

    if not ticker_col:
        raise ValueError("No ticker-like column detected in query result.")

    grouped_rows: list[dict[str, Any]] = []
    for ticker, group in prepared.groupby(ticker_col):
        row: dict[str, Any] = {"ticker": ticker, "observations": int(len(group))}
        ordered = group.sort_values(date_col) if date_col else group

        if date_col and not ordered[date_col].dropna().empty:
            row["start_date"] = ordered[date_col].dropna().iloc[0].strftime("%Y-%m-%d")
            row["end_date"] = ordered[date_col].dropna().iloc[-1].strftime("%Y-%m-%d")

        if price_col and not ordered[price_col].dropna().empty:
            row["latest_price"] = round(float(ordered[price_col].dropna().iloc[-1]), 4)

        if return_col:
            valid_returns = pd.to_numeric(ordered[return_col], errors="coerce").dropna()
            if not valid_returns.empty:
                row["avg_return_pct"] = round(float(valid_returns.mean() * 100), 4)
                row["volatility_pct"] = round(float(valid_returns.std() * 100), 4)
                row["cumulative_return_pct"] = round(float(((1 + valid_returns).prod() - 1) * 100), 4)

        if volume_col and not ordered[volume_col].dropna().empty:
            row["avg_volume"] = round(float(ordered[volume_col].dropna().mean()), 2)

        if market_cap_col and not ordered[market_cap_col].dropna().empty:
            row["latest_market_cap"] = round(float(ordered[market_cap_col].dropna().iloc[-1]), 2)

        grouped_rows.append(row)

    result = pd.DataFrame(grouped_rows)
    if "cumulative_return_pct" in result.columns:
        result = result.sort_values("cumulative_return_pct", ascending=False)
    return result.reset_index(drop=True)


def build_time_series(df: pd.DataFrame, focus_tickers: list[str] | None = None) -> dict[str, pd.DataFrame]:
    prepared, columns = prepare_stock_frame(df)
    ticker_col = columns["ticker"] or columns["identifier"]
    date_col = columns["date"]
    price_col = columns["price"]
    return_col = columns["return"]
    volume_col = columns["volume"]

    if not date_col or not ticker_col:
        raise ValueError("Time-series analysis requires both date and ticker-like columns.")

    working = prepared.copy()
    if focus_tickers:
        working = working[working[ticker_col].astype(str).isin(focus_tickers)]

    result: dict[str, pd.DataFrame] = {}
    for label, column in [("price", price_col), ("return", return_col), ("volume", volume_col)]:
        if not column:
            continue
        chart_df = (
            working[[date_col, ticker_col, column]]
            .dropna()
            .pivot_table(index=date_col, columns=ticker_col, values=column, aggfunc="last")
            .sort_index()
        )
        if not chart_df.empty:
            result[label] = chart_df
    return result


def filter_stock_dataset(
    df: pd.DataFrame,
    *,
    focus_tickers: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    prepared, columns = prepare_stock_frame(df)
    ticker_col = columns["ticker"] or columns["identifier"]
    date_col = columns["date"]

    if focus_tickers and ticker_col:
        prepared = prepared[prepared[ticker_col].astype(str).isin(focus_tickers)]

    if date_col and start_date:
        prepared = prepared[prepared[date_col] >= pd.to_datetime(start_date)]
    if date_col and end_date:
        prepared = prepared[prepared[date_col] <= pd.to_datetime(end_date)]

    return prepared.reset_index(drop=True)


def build_normalized_price_series(df: pd.DataFrame, focus_tickers: list[str] | None = None) -> pd.DataFrame:
    series = build_time_series(df, focus_tickers=focus_tickers).get("price")
    if series is None or series.empty:
        return pd.DataFrame()

    normalized = series.copy()
    for column in normalized.columns:
        first_valid = normalized[column].dropna()
        if first_valid.empty or float(first_valid.iloc[0]) == 0:
            normalized[column] = pd.NA
            continue
        normalized[column] = normalized[column] / float(first_valid.iloc[0]) * 100
    return normalized.dropna(how="all")


def build_cumulative_return_series(df: pd.DataFrame, focus_tickers: list[str] | None = None) -> pd.DataFrame:
    series = build_time_series(df, focus_tickers=focus_tickers).get("return")
    if series is None or series.empty:
        return pd.DataFrame()

    cumulative = (1 + series.fillna(0)).cumprod() - 1
    return (cumulative * 100).dropna(how="all")


def build_drawdown_series(df: pd.DataFrame, focus_tickers: list[str] | None = None) -> pd.DataFrame:
    normalized = build_normalized_price_series(df, focus_tickers=focus_tickers)
    if normalized.empty:
        return pd.DataFrame()

    running_peak = normalized.cummax()
    drawdown = normalized.divide(running_peak).subtract(1) * 100
    return drawdown.dropna(how="all")


def build_drawdown_summary(df: pd.DataFrame, focus_tickers: list[str] | None = None) -> pd.DataFrame:
    drawdown = build_drawdown_series(df, focus_tickers=focus_tickers)
    cumulative = build_cumulative_return_series(df, focus_tickers=focus_tickers)
    if drawdown.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for ticker in drawdown.columns:
        row: dict[str, Any] = {
            "ticker": ticker,
            "max_drawdown_pct": round(float(drawdown[ticker].min()), 4),
        }
        if not cumulative.empty and ticker in cumulative.columns:
            row["ending_cumulative_return_pct"] = round(float(cumulative[ticker].dropna().iloc[-1]), 4)
        rows.append(row)

    return pd.DataFrame(rows).sort_values("max_drawdown_pct").reset_index(drop=True)
