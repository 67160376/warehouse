"""
Python Data Pipeline Engineering Lab
ETL Pipeline: Omnichannel Retail Sales -> Star Schema (SQLite)

Design goals
------------
- Idempotent: re-running the same batch never inserts duplicate facts.
- Incremental: only rows that are new, or whose updated_at is newer than the
  last successfully processed value for that order_id, are (re)loaded.
- Availability over strictness: a bad row is quarantined with a reason_code;
  it never stops the whole batch or a healthy pipeline run.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional

import pandas as pd

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


# --------------------------------------------------------------------------
# Task 1 - Pipeline Configuration
# --------------------------------------------------------------------------
@dataclass
class PipelineConfig:
    input_path: Path                       # directory containing source CSV/XLSX files
    output_database: Path                  # path to the SQLite database file
    batch_list: list[str] = field(default_factory=lambda: ["orders_batch_1"])
    error_mode: str = "quarantine"         # "quarantine" (default) or "fail_fast"
    quarantine_csv: Path = Path("quarantine.csv")
    run_log_csv: Path = Path("pipeline_run_log.csv")

    def __post_init__(self):
        self.input_path = Path(self.input_path)
        self.output_database = Path(self.output_database)
        self.quarantine_csv = Path(self.quarantine_csv)
        self.run_log_csv = Path(self.run_log_csv)
        if self.error_mode not in ("quarantine", "fail_fast"):
            raise ValueError("error_mode must be 'quarantine' or 'fail_fast'")


# --------------------------------------------------------------------------
# Task 1 - Extract
# --------------------------------------------------------------------------
def extract_customers(config: PipelineConfig) -> pd.DataFrame:
    path = config.input_path / "customers.csv"
    df = pd.read_csv(path, dtype=str)
    log.info("EXTRACT customers | rows=%d | file=%s", len(df), path.name)
    return df


def extract_products(config: PipelineConfig) -> pd.DataFrame:
    path = config.input_path / "products.csv"
    df = pd.read_csv(path, dtype=str)
    log.info("EXTRACT products | rows=%d | file=%s", len(df), path.name)
    return df


def extract_orders(config: PipelineConfig, batch_name: str) -> pd.DataFrame:
    path = config.input_path / f"{batch_name}.csv"
    start = time.time()
    try:
        df = pd.read_csv(path, dtype=str)
        elapsed = time.time() - start
        log.info(
            "EXTRACT %s | rows=%d | elapsed=%.3fs | file=%s",
            batch_name, len(df), elapsed, path.name,
        )
        return df
    except Exception as exc:  # noqa: BLE001 - a batch that can't be read must not kill the run
        log.error("EXTRACT %s FAILED | %s", batch_name, exc)
        raise


# --------------------------------------------------------------------------
# Task 2 - Transform & Data Quality
# --------------------------------------------------------------------------
PAYMENT_METHOD_MAP = {
    "cash": "Cash",
    "credit card": "Credit Card",
    "promptpay": "PromptPay",
    "bank transfer": "Bank Transfer",
}

SALES_CHANNEL_MAP = {
    "store": "Store",
    "online": "Online",
    "e-commerce": "Online",   # same channel, inconsistent spelling in source
    "marketplace": "Marketplace",
}


def _normalize_payment_method(series: pd.Series) -> pd.Series:
    return series.str.strip().str.lower().map(PAYMENT_METHOD_MAP)


def _normalize_sales_channel(series: pd.Series) -> pd.Series:
    return series.str.strip().str.lower().map(SALES_CHANNEL_MAP)


def transform_orders(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    batch_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean_df, quarantine_df). Never raises on a bad row."""
    df = orders.copy()
    df["reason_code"] = ""

    def flag(mask: pd.Series, code: str) -> None:
        empty = df["reason_code"] == ""
        hit = mask & empty
        df.loc[hit, "reason_code"] = code

    # --- safe type coercion -------------------------------------------------
    df["order_datetime_parsed"] = pd.to_datetime(df["order_datetime"], errors="coerce")
    df["updated_at_parsed"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price_num"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct_num"] = pd.to_numeric(df["discount_pct"], errors="coerce")

    # --- normalize categoricals ---------------------------------------------
    df["payment_method_norm"] = _normalize_payment_method(df["payment_method"])
    df["sales_channel_norm"] = _normalize_sales_channel(df["sales_channel"])

    # --- quality checks (first failing rule wins, in this priority order) --
    flag(df["order_id"].isna() | (df["order_id"].str.strip() == ""), "MISSING_ORDER_ID")
    flag(df["order_datetime_parsed"].isna(), "INVALID_DATETIME")
    flag(df["updated_at_parsed"].isna(), "INVALID_UPDATED_AT")
    flag(df["customer_id"].isna(), "MISSING_CUSTOMER_ID")
    flag(~df["customer_id"].isin(customers["customer_id"]), "CUSTOMER_NOT_FOUND")
    flag(~df["product_id"].isin(products["product_id"]), "PRODUCT_NOT_FOUND")
    flag(df["quantity_num"].isna(), "QUANTITY_NOT_NUMERIC")
    flag(df["quantity_num"].notna() & ((df["quantity_num"] <= 0) | (df["quantity_num"] > 20)), "QUANTITY_OUT_OF_RANGE")
    flag(df["unit_price_num"].isna(), "PRICE_NOT_NUMERIC")
    flag(df["unit_price_num"].notna() & (df["unit_price_num"] <= 0), "PRICE_NOT_POSITIVE")
    flag(df["discount_pct_num"].isna(), "DISCOUNT_NOT_NUMERIC")
    flag(
        df["discount_pct_num"].notna() & ((df["discount_pct_num"] < 0) | (df["discount_pct_num"] > 100)),
        "DISCOUNT_OUT_OF_RANGE",
    )
    flag(df["payment_method_norm"].isna(), "UNKNOWN_PAYMENT_METHOD")
    flag(df["sales_channel_norm"].isna(), "UNKNOWN_SALES_CHANNEL")

    quarantine_mask = df["reason_code"] != ""
    quarantine = df.loc[quarantine_mask].copy()
    clean = df.loc[~quarantine_mask].copy()

    # --- deduplicate clean rows by order_id, keep latest updated_at --------
    before = len(clean)
    clean = clean.sort_values("updated_at_parsed").drop_duplicates(subset="order_id", keep="last")
    deduped = before - len(clean)
    if deduped:
        log.info("TRANSFORM %s | deduplicated %d duplicate order_id rows (kept latest updated_at)", batch_name, deduped)

    # --- derived measures -----------------------------------------------------
    clean["gross_amount"] = (clean["quantity_num"] * clean["unit_price_num"]).round(2)
    clean["net_amount"] = (clean["gross_amount"] * (1 - clean["discount_pct_num"] / 100)).round(2)

    clean = clean.rename(columns={
        "order_datetime_parsed": "order_datetime_clean",
        "updated_at_parsed": "updated_at_clean",
        "quantity_num": "quantity_clean",
        "unit_price_num": "unit_price_clean",
        "discount_pct_num": "discount_pct_clean",
        "payment_method_norm": "payment_method_clean",
        "sales_channel_norm": "sales_channel_clean",
    })

    quarantine = quarantine.assign(source_batch=batch_name)
    quarantine_cols = [
        "order_id", "order_datetime", "customer_id", "product_id", "quantity",
        "unit_price", "discount_pct", "payment_method", "sales_channel",
        "updated_at", "reason_code", "source_batch",
    ]
    quarantine = quarantine[quarantine_cols]

    log.info(
        "TRANSFORM %s | read=%d valid=%d rejected=%d (dup=%d)",
        batch_name, len(df), len(clean), len(quarantine), deduped,
    )
    return clean.reset_index(drop=True), quarantine.reset_index(drop=True)


# --------------------------------------------------------------------------
# Task 3 - Star Schema DDL
# --------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id     TEXT UNIQUE NOT NULL,
    customer_name   TEXT,
    province        TEXT,
    segment         TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id      TEXT UNIQUE NOT NULL,
    product_name    TEXT,
    category        TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date       TEXT UNIQUE NOT NULL,
    day             INTEGER,
    month           INTEGER,
    quarter         INTEGER,
    year            INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_key        INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT UNIQUE NOT NULL,
    date_key        INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key     INTEGER NOT NULL REFERENCES dim_product(product_key),
    quantity        INTEGER NOT NULL,
    unit_price      REAL NOT NULL,
    discount_pct    REAL NOT NULL,
    gross_amount    REAL NOT NULL,
    net_amount      REAL NOT NULL,
    payment_method  TEXT,
    sales_channel   TEXT,
    updated_at      TEXT NOT NULL,
    source_batch    TEXT NOT NULL,
    loaded_at       TEXT NOT NULL,
    CHECK (quantity > 0),
    CHECK (unit_price > 0),
    CHECK (net_amount >= 0)
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch           TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    ended_at        TEXT,
    rows_read       INTEGER,
    rows_valid      INTEGER,
    rows_rejected   INTEGER,
    rows_loaded     INTEGER,
    status          TEXT NOT NULL
);

-- watermark: last successfully-loaded updated_at per order_id, used for
-- incremental / idempotent loading.
CREATE TABLE IF NOT EXISTS load_watermark (
    order_id        TEXT PRIMARY KEY,
    updated_at      TEXT NOT NULL
);
"""


def get_connection(config: PipelineConfig) -> sqlite3.Connection:
    conn = sqlite3.connect(config.output_database)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(DDL)
    conn.commit()
    return conn


# --------------------------------------------------------------------------
# Task 3 - Load (dimensions + fact, with upsert / insert-or-ignore)
# --------------------------------------------------------------------------
def load_dim_customer(conn: sqlite3.Connection, customers: pd.DataFrame) -> None:
    rows = customers[["customer_id", "customer_name", "province", "segment"]].values.tolist()
    conn.executemany(
        """
        INSERT INTO dim_customer (customer_id, customer_name, province, segment)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(customer_id) DO UPDATE SET
            customer_name = excluded.customer_name,
            province = excluded.province,
            segment = excluded.segment;
        """,
        rows,
    )
    conn.commit()


def load_dim_product(conn: sqlite3.Connection, products: pd.DataFrame) -> None:
    rows = products[["product_id", "product_name", "category"]].values.tolist()
    conn.executemany(
        """
        INSERT INTO dim_product (product_id, product_name, category)
        VALUES (?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
            product_name = excluded.product_name,
            category = excluded.category;
        """,
        rows,
    )
    conn.commit()


def load_dim_date(conn: sqlite3.Connection, dates: pd.Series) -> None:
    unique_dates = pd.Series(dates.dropna().unique())
    rows = []
    for d in unique_dates:
        d = pd.Timestamp(d).normalize()
        date_key = int(d.strftime("%Y%m%d"))
        quarter = (d.month - 1) // 3 + 1
        rows.append((date_key, d.strftime("%Y-%m-%d"), d.day, d.month, quarter, d.year))
    conn.executemany(
        "INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, quarter, year) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def load_fact_sales(conn: sqlite3.Connection, clean: pd.DataFrame, batch_name: str) -> int:
    """Idempotent + incremental upsert of fact_sales.

    - New order_id -> inserted.
    - Existing order_id whose watermark updated_at is older -> updated ("late arriving" fact).
    - Existing order_id whose watermark updated_at is same/newer -> skipped (no-op),
      which is exactly what keeps re-running the same batch idempotent.
    """
    cur = conn.cursor()
    watermark = pd.read_sql("SELECT order_id, updated_at FROM load_watermark", conn)
    watermark_map = dict(zip(watermark["order_id"], watermark["updated_at"]))

    cust_map = dict(pd.read_sql("SELECT customer_id, customer_key FROM dim_customer", conn).values)
    prod_map = dict(pd.read_sql("SELECT product_id, product_key FROM dim_product", conn).values)

    loaded = 0
    now = datetime.now(UTC).isoformat()
    for row in clean.itertuples(index=False):
        order_id = row.order_id
        updated_at = row.updated_at_clean.isoformat()

        prior = watermark_map.get(order_id)
        if prior is not None and prior >= updated_at:
            continue  # already loaded this or a newer version -> idempotent no-op

        date_key = int(row.order_datetime_clean.strftime("%Y%m%d"))
        cur.execute(
            """
            INSERT INTO fact_sales (
                order_id, date_key, customer_key, product_key, quantity, unit_price,
                discount_pct, gross_amount, net_amount, payment_method, sales_channel,
                updated_at, source_batch, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                date_key = excluded.date_key,
                customer_key = excluded.customer_key,
                product_key = excluded.product_key,
                quantity = excluded.quantity,
                unit_price = excluded.unit_price,
                discount_pct = excluded.discount_pct,
                gross_amount = excluded.gross_amount,
                net_amount = excluded.net_amount,
                payment_method = excluded.payment_method,
                sales_channel = excluded.sales_channel,
                updated_at = excluded.updated_at,
                source_batch = excluded.source_batch,
                loaded_at = excluded.loaded_at;
            """,
            (
                order_id, date_key, cust_map[row.customer_id], prod_map[row.product_id],
                int(row.quantity_clean), float(row.unit_price_clean), float(row.discount_pct_clean),
                float(row.gross_amount), float(row.net_amount),
                row.payment_method_clean, row.sales_channel_clean,
                updated_at, batch_name, now,
            ),
        )
        cur.execute(
            "INSERT INTO load_watermark (order_id, updated_at) VALUES (?, ?) "
            "ON CONFLICT(order_id) DO UPDATE SET updated_at = excluded.updated_at;",
            (order_id, updated_at),
        )
        loaded += 1

    conn.commit()
    return loaded


def write_quarantine(config: PipelineConfig, quarantine: pd.DataFrame) -> None:
    if quarantine.empty:
        return
    header = not config.quarantine_csv.exists()
    quarantine.to_csv(config.quarantine_csv, mode="a", header=header, index=False)


def write_run_log_csv(config: PipelineConfig, conn: sqlite3.Connection) -> None:
    df = pd.read_sql("SELECT * FROM pipeline_run_log ORDER BY run_id", conn)
    df.to_csv(config.run_log_csv, index=False)


# --------------------------------------------------------------------------
# Task 5 - Orchestration
# --------------------------------------------------------------------------
def run_batch(conn: sqlite3.Connection, config: PipelineConfig, batch_name: str,
              customers: pd.DataFrame, products: pd.DataFrame) -> dict:
    started_at = datetime.now(UTC).isoformat()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO pipeline_run_log (batch, started_at, status) VALUES (?, ?, 'running')",
        (batch_name, started_at),
    )
    conn.commit()
    run_id = cur.lastrowid

    try:
        orders = extract_orders(config, batch_name)
    except Exception as exc:  # noqa: BLE001
        # Extract failed entirely (e.g. missing/corrupt file): record failure,
        # do NOT touch data already loaded from other batches.
        ended_at = datetime.now(UTC).isoformat()
        cur.execute(
            "UPDATE pipeline_run_log SET ended_at=?, status=?, rows_read=0, rows_valid=0, "
            "rows_rejected=0, rows_loaded=0 WHERE run_id=?",
            (ended_at, f"failed: {exc}", run_id),
        )
        conn.commit()
        log.error("BATCH %s marked FAILED, pipeline continues.", batch_name)
        return {"batch": batch_name, "status": "failed", "rows_loaded": 0}

    clean, quarantine = transform_orders(orders, customers, products, batch_name)

    if config.error_mode == "fail_fast" and not quarantine.empty:
        ended_at = datetime.now(UTC).isoformat()
        cur.execute(
            "UPDATE pipeline_run_log SET ended_at=?, status='failed_fast', rows_read=?, "
            "rows_valid=?, rows_rejected=? WHERE run_id=?",
            (ended_at, len(orders), len(clean), len(quarantine), run_id),
        )
        conn.commit()
        raise ValueError(f"{batch_name}: {len(quarantine)} rows failed validation (fail_fast mode)")

    write_quarantine(config, quarantine)

    load_dim_customer(conn, customers)
    load_dim_product(conn, products)
    if not clean.empty:
        load_dim_date(conn, clean["order_datetime_clean"])
    rows_loaded = load_fact_sales(conn, clean, batch_name) if not clean.empty else 0

    ended_at = datetime.now(UTC).isoformat()
    cur.execute(
        "UPDATE pipeline_run_log SET ended_at=?, status='success', rows_read=?, rows_valid=?, "
        "rows_rejected=?, rows_loaded=? WHERE run_id=?",
        (ended_at, len(orders), len(clean), len(quarantine), rows_loaded, run_id),
    )
    conn.commit()

    log.info(
        "LOAD %s | valid=%d rejected=%d newly_loaded_or_updated=%d (skipped_as_already_current=%d)",
        batch_name, len(clean), len(quarantine), rows_loaded, len(clean) - rows_loaded,
    )
    return {
        "batch": batch_name, "status": "success", "rows_read": len(orders),
        "rows_valid": len(clean), "rows_rejected": len(quarantine), "rows_loaded": rows_loaded,
    }


def run_pipeline(config: PipelineConfig) -> list[dict]:
    """extract -> transform -> validate -> load, batch by batch.

    A failure in one batch is recorded and skipped; it never rolls back or
    blocks batches that already succeeded, and never crashes the process.
    """
    conn = get_connection(config)
    customers = extract_customers(config)
    products = extract_products(config)

    results = []
    for batch_name in config.batch_list:
        try:
            results.append(run_batch(conn, config, batch_name, customers, products))
        except Exception as exc:  # noqa: BLE001 - fail_fast mode raises; caller decides
            log.error("Pipeline stopped on %s: %s", batch_name, exc)
            raise
    write_run_log_csv(config, conn)
    conn.close()
    return results


def print_kpi_summary(config: PipelineConfig) -> None:
    conn = sqlite3.connect(config.output_database)
    fact = pd.read_sql("SELECT * FROM fact_sales", conn)
    log_df = pd.read_sql("SELECT * FROM pipeline_run_log", conn)
    conn.close()

    total_read = log_df["rows_read"].sum()
    total_valid = log_df["rows_valid"].sum()
    total_rejected = log_df["rows_rejected"].sum()
    total_loaded_events = log_df["rows_loaded"].sum()
    net_sales = fact["net_amount"].sum()

    print("\n" + "=" * 60)
    print("KPI SUMMARY (cumulative across all pipeline_run_log rows)")
    print("=" * 60)
    print(f"rows read      : {total_read}")
    print(f"rows valid     : {total_valid}")
    print(f"rows rejected  : {total_rejected}")
    print(f"fact rows now  : {len(fact)} (order_id is unique by construction)")
    print(f"load events    : {total_loaded_events} (insert/update ops across all runs)")
    print(f"fact_sales rows: {len(fact)} (final, deduplicated by order_id)")
    print(f"net sales total: {net_sales:,.2f}")
    print("=" * 60)


# --------------------------------------------------------------------------
# Demonstration entry point
# --------------------------------------------------------------------------
if __name__ == "__main__":
    base = Path(__file__).parent
    config = PipelineConfig(
        input_path=base / "source_data",
        output_database=base / "output" / "retail_dw.db",
        batch_list=[],  # set per-call below to show 4 explicit runs
        error_mode="quarantine",
        quarantine_csv=base / "output" / "quarantine.csv",
        run_log_csv=base / "output" / "pipeline_run_log.csv",
    )
    config.output_database.parent.mkdir(parents=True, exist_ok=True)
    for stale in (config.output_database, config.quarantine_csv, config.run_log_csv):
        stale.unlink(missing_ok=True)

    # --- Task 4 evidence: 4 explicit runs ---------------------------------
    print("\n### RUN 1: batch_1 (first load) ###")
    config.batch_list = ["orders_batch_1"]
    run_pipeline(config)
    conn = sqlite3.connect(config.output_database)
    fact_count_1 = pd.read_sql("SELECT COUNT(*) c FROM fact_sales", conn)["c"][0]
    conn.close()
    print(f"fact_sales row count after RUN 1: {fact_count_1}")

    print("\n### RUN 2: batch_1 again (must be idempotent) ###")
    config.batch_list = ["orders_batch_1"]
    run_pipeline(config)
    conn = sqlite3.connect(config.output_database)
    fact_count_2 = pd.read_sql("SELECT COUNT(*) c FROM fact_sales", conn)["c"][0]
    conn.close()
    print(f"fact_sales row count after RUN 2: {fact_count_2} (expected == {fact_count_1})")
    assert fact_count_1 == fact_count_2, "Idempotency check FAILED"
    print("Idempotency check PASSED: repeated batch_1 did not add rows.")

    print("\n### RUN 3: batch_2 (incremental) ###")
    config.batch_list = ["orders_batch_2"]
    run_pipeline(config)

    print("\n### RUN 4: batch_3 (incremental) ###")
    config.batch_list = ["orders_batch_3"]
    run_pipeline(config)

    print_kpi_summary(config)
