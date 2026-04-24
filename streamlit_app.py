from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from analysis import (
    build_cumulative_return_series,
    build_drawdown_series,
    build_drawdown_summary,
    build_ticker_summary,
    build_time_series,
    detect_stock_columns,
    filter_stock_dataset,
    build_normalized_price_series,
    export_summary,
    preview_records,
    summarize_stock_data,
)
from data_loader import get_wrds_connection, run_wrds_sql


st.set_page_config(
    page_title="WRDS Stock Lab",
    layout="wide",
    initial_sidebar_state="expanded",
)


DEFAULT_SQL = """select
    a.date,
    b.ticker,
    a.permno,
    abs(a.prc) as prc,
    a.ret,
    a.vol,
    a.shrout
from crsp.dsf as a
join crsp.stocknames as b
  on a.permno = b.permno
 and a.date between b.namedt and b.nameenddt
where b.ticker in ('AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA')
  and a.date between '2024-01-01' and '2024-12-31'
order by a.date, b.ticker
limit 5000
"""


MONTHLY_SQL = """select
    a.date,
    b.ticker,
    a.permno,
    abs(a.prc) as prc,
    a.ret,
    a.vol,
    a.shrout
from crsp.msf as a
join crsp.stocknames as b
  on a.permno = b.permno
 and a.date between b.namedt and b.nameenddt
where b.ticker in ('AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA')
  and a.date between '2020-01-01' and '2024-12-31'
order by a.date, b.ticker
limit 5000
"""


SQL_TEMPLATES = {
    "CRSP Daily Stocks": DEFAULT_SQL,
    "CRSP Monthly Stocks": MONTHLY_SQL,
    "Custom SQL": "select *\nfrom crsp.dsf\nlimit 1000\n",
}

DEFAULT_WATCHLIST = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]


@st.cache_resource(show_spinner=False)
def _get_connection(username: str, password: str):
    return get_wrds_connection(username=username or None, password=password or None)


@st.cache_data(show_spinner=False, ttl=600)
def _run_query(username: str, password: str, sql: str) -> pd.DataFrame:
    connection = _get_connection(username, password)
    return run_wrds_sql(connection, sql)


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            color: #203229;
            background:
                radial-gradient(circle at top left, rgba(210, 240, 232, 0.9), transparent 32%),
                radial-gradient(circle at bottom right, rgba(240, 210, 184, 0.65), transparent 28%),
                linear-gradient(135deg, #f6f1e8 0%, #eef3ec 45%, #e8efe9 100%);
        }
        .stApp, p, li, label, div, span, h1, h2, h3, h4, h5, h6 {
            color: #203229 !important;
        }
        [data-testid="stAppViewContainer"],
        [data-testid="stHeader"],
        [data-testid="stSidebar"],
        [data-testid="stSidebar"] * {
            color: #203229 !important;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(250, 246, 238, 0.96) 0%, rgba(236, 244, 238, 0.96) 100%);
            border-right: 1px solid rgba(32, 50, 41, 0.08);
        }
        .hero-card, .panel-card {
            background: rgba(255, 252, 247, 0.82);
            border: 1px solid rgba(41, 61, 51, 0.1);
            border-radius: 24px;
            padding: 1.5rem 1.6rem;
            box-shadow: 0 20px 50px rgba(41, 61, 51, 0.08);
            backdrop-filter: blur(10px);
        }
        .hero-title {
            font-size: 3rem;
            line-height: 1;
            font-weight: 700;
            letter-spacing: -0.03em;
            color: #203229;
            margin-bottom: 0.7rem;
        }
        .hero-copy {
            color: #486356;
            font-size: 1.02rem;
            line-height: 1.6;
            margin-bottom: 1rem;
        }
        .hero-kicker {
            display: inline-block;
            font-size: 0.8rem;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            color: #7c5a41;
            margin-bottom: 0.8rem;
        }
        .bullet {
            color: #294232;
            font-size: 0.95rem;
            margin: 0.45rem 0;
        }
        .section-title {
            font-size: 1.4rem;
            font-weight: 700;
            color: #203229;
            margin-bottom: 0.5rem;
        }
        .stButton > button, .stDownloadButton > button, div[data-testid="stFormSubmitButton"] > button {
            background: linear-gradient(135deg, #274536 0%, #1f3429 100%);
            color: #f7f2ea !important;
            border: 1px solid rgba(31, 52, 41, 0.9);
            border-radius: 12px;
            font-weight: 600;
        }
        .stButton > button *, .stDownloadButton > button *, div[data-testid="stFormSubmitButton"] > button * {
            color: #f7f2ea !important;
            fill: #f7f2ea !important;
        }
        .stButton > button:hover, .stDownloadButton > button:hover, div[data-testid="stFormSubmitButton"] > button:hover {
            color: #fffaf2 !important;
            border-color: #1f3429;
        }
        .stButton > button:hover *, .stDownloadButton > button:hover *, div[data-testid="stFormSubmitButton"] > button:hover * {
            color: #fffaf2 !important;
            fill: #fffaf2 !important;
        }
        .stTextInput input, .stTextArea textarea, div[data-baseweb="select"] > div, .stDateInput input {
            background: rgba(255, 252, 247, 0.96) !important;
            color: #203229 !important;
            caret-color: #203229 !important;
        }
        textarea, input {
            color: #203229 !important;
        }
        .stCodeBlock, pre, code {
            color: #183127 !important;
            background: rgba(255, 251, 244, 0.94) !important;
        }
        pre code, code span {
            color: #183127 !important;
            background: transparent !important;
        }
        div[data-testid="stMetricValue"], div[data-testid="stMetricLabel"] {
            color: #203229 !important;
        }
        .stTabs [data-baseweb="tab-list"] button {
            color: #203229 !important;
        }
        .stTabs [aria-selected="true"] {
            background: rgba(39, 69, 54, 0.10) !important;
            border-radius: 10px 10px 0 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_login() -> None:
    _inject_styles()
    left, right = st.columns([1.15, 0.85], gap="large")

    with left:
        st.markdown(
            """
            <div class="hero-card">
              <div class="hero-kicker">WRDS SQL Workspace</div>
              <div class="hero-title">Stock analysis, narrowed to the job.</div>
              <div class="hero-copy">
                This app is intentionally constrained: log into WRDS, run SQL, and get stock-specific analysis.
                No file uploads, no broad data exploration surface, no extra data source modes.
              </div>
              <div class="bullet">SQL templates for CRSP daily and monthly stock pulls.</div>
              <div class="bullet">Automatic stock column detection for price, return, volume, and market cap.</div>
              <div class="bullet">Ticker leaderboard, return diagnostics, and time-series charts after each query.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right:
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Sign In to WRDS</div>', unsafe_allow_html=True)
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("WRDS Username", value=st.session_state.get("wrds_username", ""))
            password = st.text_input("WRDS Password", type="password", value=st.session_state.get("wrds_password", ""))
            submitted = st.form_submit_button("Enter Workspace", use_container_width=True)

        if submitted:
            try:
                _get_connection(username, password)
                st.session_state["wrds_username"] = username
                st.session_state["wrds_password"] = password
                st.session_state["authenticated"] = True
                st.rerun()
            except Exception as exc:
                st.error(f"WRDS login failed: {exc}")
        st.markdown("</div>", unsafe_allow_html=True)


def _build_template_sql(template_name: str, watchlist: list[str]) -> str:
    if template_name == "Custom SQL":
        return SQL_TEMPLATES[template_name]

    quoted_tickers = ", ".join(f"'{ticker}'" for ticker in watchlist) or "'AAPL'"
    source_table = "crsp.dsf" if template_name == "CRSP Daily Stocks" else "crsp.msf"
    start_date = "2024-01-01" if template_name == "CRSP Daily Stocks" else "2020-01-01"

    return f"""select
    a.date,
    b.ticker,
    a.permno,
    abs(a.prc) as prc,
    a.ret,
    a.vol,
    a.shrout
from {source_table} as a
join crsp.stocknames as b
  on a.permno = b.permno
 and a.date between b.namedt and b.nameenddt
where b.ticker in ({quoted_tickers})
  and a.date between '{start_date}' and '2024-12-31'
order by a.date, b.ticker
limit 5000
"""


def _sidebar_controls() -> tuple[str, list[str], tuple[pd.Timestamp | None, pd.Timestamp | None], str]:
    st.sidebar.header("WRDS Stock Lab")
    if st.sidebar.button("Log Out", use_container_width=True):
        st.session_state.clear()
        st.rerun()

    template_name = st.sidebar.selectbox("SQL Template", options=list(SQL_TEMPLATES))
    watchlist_text = st.sidebar.text_input(
        "Watchlist",
        value=st.session_state.get("watchlist_text", ",".join(DEFAULT_WATCHLIST)),
        help="Comma-separated tickers used to build the stock SQL templates.",
    )
    watchlist = [item.strip().upper() for item in watchlist_text.split(",") if item.strip()]
    st.session_state["watchlist_text"] = ",".join(watchlist)

    if st.sidebar.button("Load Template", use_container_width=True):
        st.session_state["sql_text"] = _build_template_sql(template_name, watchlist)

    sql_text = st.sidebar.text_area(
        "SQL Editor",
        value=st.session_state.get("sql_text", _build_template_sql(template_name, watchlist)),
        height=320,
    )
    st.session_state["sql_text"] = sql_text

    selected_tickers = st.sidebar.multiselect(
        "Focus Tickers",
        options=st.session_state.get("available_tickers", []),
        default=st.session_state.get("available_tickers", [])[:4],
        help="Used only for chart focus after the SQL result is loaded.",
    )

    date_bounds = st.session_state.get("available_dates")
    if date_bounds:
        start_value, end_value = date_bounds
        selected_dates = st.sidebar.date_input(
            "Date Range",
            value=(start_value.date(), end_value.date()),
            min_value=start_value.date(),
            max_value=end_value.date(),
        )
        if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
            return template_name, selected_tickers, (
                pd.Timestamp(selected_dates[0]),
                pd.Timestamp(selected_dates[1]),
            ), sql_text

    return template_name, selected_tickers, (None, None), sql_text


def _render_summary(summary: dict[str, object]) -> None:
    cols = st.columns(5)
    rows, columns = summary["shape"]
    cols[0].metric("Rows", rows)
    cols[1].metric("Columns", columns)
    cols[2].metric("Missing Values", summary["missing_total"])
    cols[3].metric("Duplicate Rows", summary["duplicate_rows"])
    cols[4].metric("Tickers", summary.get("ticker_count", 0))

    returns = summary.get("returns", {})
    if returns:
        subcols = st.columns(4)
        subcols[0].metric("Avg Daily Return", f"{returns['avg_daily_return_pct']:.2f}%")
        subcols[1].metric("Volatility", f"{returns['volatility_pct']:.2f}%")
        subcols[2].metric("Best Day", f"{returns['best_day_pct']:.2f}%")
        subcols[3].metric("Worst Day", f"{returns['worst_day_pct']:.2f}%")


def _render_overview(df: pd.DataFrame, summary: dict[str, object]) -> None:
    _render_summary(summary)
    st.subheader("Query Preview")
    st.dataframe(df, use_container_width=True, height=380)

    with st.expander("Detected Stock Columns"):
        st.json(summary["detected_columns"])

    with st.expander("Preview JSON"):
        st.json(preview_records(df, rows=min(20, len(df))))


def _render_leaderboard(df: pd.DataFrame) -> None:
    st.subheader("Ticker Leaderboard")
    ticker_summary = build_ticker_summary(df)
    st.dataframe(ticker_summary, use_container_width=True, height=420)
    metric_cols = [column for column in ["cumulative_return_pct", "volatility_pct", "avg_volume"] if column in ticker_summary.columns]
    if metric_cols:
        st.caption("Ticker comparison")
        st.bar_chart(ticker_summary.set_index("ticker")[metric_cols], height=320)
    st.download_button(
        "Download ticker summary",
        data=ticker_summary.to_csv(index=False).encode("utf-8"),
        file_name="ticker_summary.csv",
        mime="text/csv",
    )


def _render_charts(df: pd.DataFrame, selected_tickers: list[str]) -> None:
    st.subheader("Time Series")
    chart_data = build_time_series(df, focus_tickers=selected_tickers or None)
    if not chart_data:
        st.info("The SQL result needs ticker/date plus at least one of price, return, or volume to draw charts.")
        return

    normalized_price = build_normalized_price_series(df, focus_tickers=selected_tickers or None)
    cumulative_return = build_cumulative_return_series(df, focus_tickers=selected_tickers or None)
    drawdown_series = build_drawdown_series(df, focus_tickers=selected_tickers or None)
    drawdown_summary = build_drawdown_summary(df, focus_tickers=selected_tickers or None)
    ticker_summary = build_ticker_summary(df)
    ticker_options = ticker_summary["ticker"].astype(str).tolist()
    single_ticker = None
    if ticker_options:
        single_ticker = st.selectbox("Single stock detail", options=ticker_options, index=0)

    if "price" in chart_data:
        st.caption("Price comparison")
        st.line_chart(chart_data["price"], height=320)
    if not normalized_price.empty:
        st.caption("Normalized price comparison (base = 100)")
        st.line_chart(normalized_price, height=320)
    if not cumulative_return.empty:
        st.caption("Cumulative return comparison (%)")
        st.line_chart(cumulative_return, height=320)
    if "return" in chart_data:
        st.caption("Return comparison")
        st.line_chart(chart_data["return"], height=280)
    if "volume" in chart_data:
        st.caption("Volume comparison")
        st.area_chart(chart_data["volume"], height=260)
    if not drawdown_series.empty:
        st.caption("Drawdown comparison (%)")
        st.line_chart(drawdown_series, height=300)
    if not drawdown_summary.empty:
        st.caption("Max drawdown vs ending cumulative return")
        st.dataframe(drawdown_summary, use_container_width=True, height=220)
        if {"max_drawdown_pct", "ending_cumulative_return_pct"}.issubset(drawdown_summary.columns):
            st.bar_chart(
                drawdown_summary.set_index("ticker")[["max_drawdown_pct", "ending_cumulative_return_pct"]],
                height=280,
            )

    if single_ticker:
        detail_cols = st.columns(3)
        for idx, metric_name in enumerate(["price", "return", "volume"]):
            if metric_name not in chart_data:
                continue
            series = chart_data[metric_name]
            if single_ticker not in series.columns:
                continue
            detail_cols[idx].caption(f"{single_ticker} {metric_name.title()} Trend")
            detail_cols[idx].line_chart(series[[single_ticker]], height=220)
        extra_cols = st.columns(2)
        if not cumulative_return.empty and single_ticker in cumulative_return.columns:
            extra_cols[0].caption(f"{single_ticker} Cumulative Return")
            extra_cols[0].line_chart(cumulative_return[[single_ticker]], height=220)
        if not drawdown_series.empty and single_ticker in drawdown_series.columns:
            extra_cols[1].caption(f"{single_ticker} Drawdown")
            extra_cols[1].line_chart(drawdown_series[[single_ticker]], height=220)


def _render_export(summary: dict[str, object], df: pd.DataFrame) -> None:
    st.subheader("Export")
    st.download_button(
        "Download query result CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="wrds_stock_query.csv",
        mime="text/csv",
    )
    st.download_button(
        "Download stock summary JSON",
        data=json.dumps(summary, indent=2, ensure_ascii=False).encode("utf-8"),
        file_name="stock_summary.json",
        mime="application/json",
    )
    if st.button("Save summary in project directory"):
        saved_path = export_summary(summary, Path.cwd() / "stock_summary.json")
        st.success(f"Saved to {saved_path}")


def main() -> None:
    if not st.session_state.get("authenticated"):
        _render_login()
        return

    _inject_styles()
    template_name, selected_tickers, selected_dates, sql_text = _sidebar_controls()
    st.title("WRDS Stock Lab")
    st.caption("SQL only. WRDS only. Tuned for stock analysis.")

    query_col, info_col = st.columns([1.35, 0.65], gap="large")
    with query_col:
        st.markdown("#### Query")
        st.code(sql_text, language="sql")
    with info_col:
        st.markdown("#### Active Template")
        st.markdown(f"**{template_name}**")
        st.markdown("Run the SQL below to refresh the analytics panels.")

    if st.button("Run Stock Query", type="primary", use_container_width=True):
        username = st.session_state.get("wrds_username", "")
        password = st.session_state.get("wrds_password", "")
        try:
            with st.spinner("Running WRDS query..."):
                result_df = _run_query(username, password, sql_text)
            st.session_state["result_df"] = result_df
            detected = detect_stock_columns(result_df)
            ticker_col = detected["ticker"] or detected["identifier"]
            date_col = detected["date"]
            if ticker_col:
                tickers = sorted(result_df[ticker_col].dropna().astype(str).unique().tolist())
                st.session_state["available_tickers"] = tickers
            else:
                st.session_state["available_tickers"] = []
            if date_col:
                date_series = pd.to_datetime(result_df[date_col], errors="coerce").dropna()
                if not date_series.empty:
                    st.session_state["available_dates"] = (date_series.min(), date_series.max())
                else:
                    st.session_state["available_dates"] = None
            else:
                st.session_state["available_dates"] = None
        except Exception as exc:
            st.error(f"Query failed: {exc}")
            return

    result_df = st.session_state.get("result_df")
    if result_df is None:
        st.info("Load a template or edit the SQL, then run the query.")
        return

    filtered_df = filter_stock_dataset(
        result_df,
        focus_tickers=selected_tickers or None,
        start_date=selected_dates[0].strftime("%Y-%m-%d") if selected_dates[0] is not None else None,
        end_date=selected_dates[1].strftime("%Y-%m-%d") if selected_dates[1] is not None else None,
    )
    if filtered_df.empty:
        st.warning("No rows remain after the current ticker/date filters.")
        return

    summary = summarize_stock_data(filtered_df)
    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Leaderboard", "Charts", "Export"])

    with tab1:
        _render_overview(filtered_df, summary)
    with tab2:
        try:
            _render_leaderboard(filtered_df)
        except ValueError as exc:
            st.info(str(exc))
    with tab3:
        try:
            _render_charts(filtered_df, selected_tickers)
        except ValueError as exc:
            st.info(str(exc))
    with tab4:
        _render_export(summary, filtered_df)


if __name__ == "__main__":
    main()
