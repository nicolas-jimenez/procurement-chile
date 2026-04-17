"""
06_did_fe.py
DiD regressions with firm FEs: post × log(n_sim) interaction.

Usage:
    python3 06_did_fe.py entry        # entry regressions, all + by size
    python3 06_did_fe.py bid          # bid-level regressions, all + by size
    python3 06_did_fe.py all
"""

import sys, gc, warnings
import numpy as np
import pandas as pd
import pyfixest as pfx
from pathlib import Path

warnings.filterwarnings("ignore")

# ── paths ─────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import OUTPUT_ROOT  # noqa: E402

BID_SIM  = OUTPUT_ROOT / "simultaneousbids" / "bid_level_simult.parquet"
BID_ANAL = OUTPUT_ROOT / "bids" / "bid_analysis_sample.parquet"
TDIR     = OUTPUT_ROOT / "simultaneousbids" / "tables"
TDIR.mkdir(parents=True, exist_ok=True)

SIZE_ORDER = ["micro", "small", "medium", "large"]

# ── pyfixest helper ───────────────────────────────────────────────────────────
def run_fe(formula, data, label):
    print(f"  FE  {label} (n={len(data):,})")
    try:
        res  = pfx.feols(formula, data=data, vcov="HC1")
        coef = res.coef(); se = res.se(); pval = res.pvalue()
        row  = {"sample": label, "n_obs": int(res._N),
                "r2_within": round(float(res._r2_within), 4)}
        for k in coef.index:
            row[k]          = round(float(coef[k]), 6)
            row[k + "_se"]  = round(float(se[k]),   6)
            row[k + "_p"]   = round(float(pval[k]),  4)
            row[k + "_s"]   = ("***" if pval[k] < 0.01 else
                               "**"  if pval[k] < 0.05 else
                               "*"   if pval[k] < 0.10 else "")
        return row
    except Exception as e:
        return {"sample": label, "error": str(e)}

# ── pretty-print ──────────────────────────────────────────────────────────────
def show(df, cols):
    """Print key columns of results table."""
    sub = df[["sample","n_obs","r2_within"] +
             [c for c in cols if c in df.columns]].copy()
    pd.set_option("display.max_colwidth", 40)
    pd.set_option("display.width", 200)
    print(sub.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════════════
# ENTRY
# ══════════════════════════════════════════════════════════════════════════════
def run_entry():
    print("\n" + "="*60)
    print("[ENTRY DiD] first_bid_in_region ~ n_sim + post:n_sim + firm FE")
    print("="*60)

    ent = pd.read_parquet(BID_SIM, columns=[
        "rut_bidder", "ym", "tender_id", "monto_estimado",
        "first_bid_in_region", "n_sim_lag1", "n_sim_nl_lag1",
        "post", "size_group"
    ])
    ent = ent.dropna(subset=["first_bid_in_region","n_sim_lag1","n_sim_nl_lag1",
                              "monto_estimado"])
    ent = ent[ent["monto_estimado"] > 0]
    ent["log_n_sim"]    = np.log1p(ent["n_sim_lag1"])
    ent["log_n_sim_nl"] = np.log1p(ent["n_sim_nl_lag1"])
    ent["log_monto"]    = np.log(ent["monto_estimado"])
    ent["rut_str"]      = ent["rut_bidder"].astype(str)
    ent["post"]         = ent["post"].astype(int)
    print(f"  Loaded: {len(ent):,} rows, {ent['rut_str'].nunique():,} firms")

    formula = ("first_bid_in_region ~ log_n_sim + post:log_n_sim "
               "+ log_n_sim_nl + post:log_n_sim_nl "
               "+ log_monto | rut_str")

    rows = []

    # All firms
    rows.append(run_fe(formula, ent, "All"))
    gc.collect()

    # By size
    for sg in SIZE_ORDER:
        sub = ent[ent["size_group"] == sg].copy()
        rows.append(run_fe(formula, sub, sg))
        del sub; gc.collect()

    del ent; gc.collect()

    df = pd.DataFrame(rows)
    df.to_csv(TDIR / "t20_did_entry_fe.csv", index=False)

    key_cols = [
        "log_n_sim", "log_n_sim_se", "log_n_sim_s",
        "post:log_n_sim", "post:log_n_sim_se", "post:log_n_sim_s",
        "log_n_sim_nl", "log_n_sim_nl_se", "log_n_sim_nl_s",
        "post:log_n_sim_nl", "post:log_n_sim_nl_se", "post:log_n_sim_nl_s",
    ]
    print("\n[T20] Entry DiD with firm FE:")
    show(df, key_cols)

# ══════════════════════════════════════════════════════════════════════════════
# BID LEVEL
# ══════════════════════════════════════════════════════════════════════════════
def run_bid():
    print("\n" + "="*60)
    print("[BID DiD] log_sub_price_ratio ~ n_sim + post:n_sim + firm FE")
    print("="*60)

    import duckdb
    # Use DuckDB entirely for the join - it streams parquet files without
    # materialising 15.8M rows in Python memory
    print("  Joining via DuckDB (streaming) …")
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=2")

    # bidder_id format: "76956121-8" → split on '-', take first part
    sql = f"""
    SELECT
        TRY_CAST(split_part(b.bidder_id, '-', 1) AS BIGINT) AS rut_bidder,
        b.log_sub_price_ratio,
        ln(b.monto_utm)                    AS log_monto,
        CAST(b.post AS INT)                AS post,
        ln(1 + f.n_sim)                    AS log_n_sim,
        ln(1 + f.n_sim_nonlocal)           AS log_n_sim_nl,
        f.size_group
    FROM read_parquet('{BID_ANAL}') b
    INNER JOIN read_parquet('{BID_SIM}') f
        ON  b.tender_id = f.tender_id
        AND TRY_CAST(split_part(b.bidder_id, '-', 1) AS BIGINT) = f.rut_bidder
    WHERE b.log_sub_price_ratio IS NOT NULL
      AND b.monto_utm > 0
      AND f.size_group IN ('micro','small','medium','large')
    """
    bid = con.execute(sql).df()
    con.close(); gc.collect()

    bid["rut_str"] = bid["rut_bidder"].astype(str)
    print(f"  Merged: {len(bid):,} rows, {bid['rut_str'].nunique():,} firms")

    formula = ("log_sub_price_ratio ~ log_n_sim + post:log_n_sim "
               "+ log_n_sim_nl + post:log_n_sim_nl "
               "+ log_monto | rut_str")

    rows = []

    rows.append(run_fe(formula, bid, "All"))
    gc.collect()

    for sg in SIZE_ORDER:
        sub = bid[bid["size_group"] == sg].copy()
        rows.append(run_fe(formula, sub, sg))
        del sub; gc.collect()

    del bid; gc.collect()

    df = pd.DataFrame(rows)
    df.to_csv(TDIR / "t21_did_bid_fe.csv", index=False)

    key_cols = [
        "log_n_sim", "log_n_sim_se", "log_n_sim_s",
        "post:log_n_sim", "post:log_n_sim_se", "post:log_n_sim_s",
        "log_n_sim_nl", "log_n_sim_nl_se", "log_n_sim_nl_s",
        "post:log_n_sim_nl", "post:log_n_sim_nl_se", "post:log_n_sim_nl_s",
    ]
    print("\n[T21] Bid-level DiD with firm FE:")
    show(df, key_cols)

# ── dispatch ──────────────────────────────────────────────────────────────────
task = sys.argv[1] if len(sys.argv) > 1 else "all"
if task in ("entry", "all"):  run_entry()
if task in ("bid",   "all"):  run_bid()
print("\n✓ Done.")
