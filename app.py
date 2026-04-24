from __future__ import annotations

import argparse
import json

from analysis import build_ticker_summary, preview_records, summarize_stock_data
from data_loader import get_wrds_connection, run_wrds_sql


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WRDS SQL stock analysis tool.")
    parser.add_argument("--username", help="WRDS username.")
    parser.add_argument("--password", help="WRDS password. Optional if .pgpass is configured.")
    parser.add_argument("--sql", required=True, help="SQL query to run on WRDS.")
    parser.add_argument("--preview", type=int, default=10, help="Number of rows to preview.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    connection = get_wrds_connection(username=args.username, password=args.password)
    try:
        df = run_wrds_sql(connection, args.sql)
    finally:
        connection.close()

    payload = {
        "preview": preview_records(df, rows=args.preview),
        "summary": summarize_stock_data(df),
    }

    try:
        payload["ticker_summary"] = preview_records(build_ticker_summary(df), rows=20)
    except ValueError:
        payload["ticker_summary"] = []

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
