"""
01_build_did_sample.py
─────────────────────────────────────────────────────────────────────────────
Build the tender-level and bid-level analysis samples for the Compra Ágil DiD.

Steps
  1. Load combined_sii_merged_filtered (bid-level rows for both datasets).
  1b. Fill Compra Ágil sector using RutUnidad→sector crosswalk.
  2. Pull licitaciones bid amounts (Valor Total Ofertado) and adjudication
     metadata (FechaAdjudicacion, Estado) from chilecompra_panel.parquet.
  3. Convert estimated values to UTM; assign value bands and DiD indicators.
  4. Compute new-entrant flags (first bid date across full dataset).
  5. Compute SME indicators from both the Compra Ágil tamano field and SII
     tramoventas; run and save SME-definition diagnostics.
  6. Collapse to tender-level sample (one row per tender).
  7. Construct bid-level sample with log(submitted price / reference price).
  8. Save both samples as parquet.

Outputs
  output/did/samples/did_tender_sample.parquet
  output/did/samples/did_bid_sample.parquet
  output/did/tables/sme_diagnostics.csv
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# ── Path setup ────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from did_utils import (
    COMBINED,
    LIC_PANEL,
    RUT_SECTOR_CROSSWALK,
    OUT_SAMPLES,
    OUT_TABLES,
    REFORM_DATE,
    add_utm_value,
    assign_band,
    load_utm_table,
)

# ── Column lists ──────────────────────────────────────────────────────────────
COMBINED_COLS = [
    "dataset", "tender_id", "rut_bidder", "dv_bidder", "rut_bidder_raw", "rut_unidad",
    "region_buyer", "fecha_pub",
    "source_year", "source_month",
    "monto_estimado", "monto_oferta",
    "is_selected", "is_key_dup",
    "tipo", "sector",
    "n_oferentes",        # licitaciones header-level bidder count
    "tamano",             # CA: firm size from platform
    "estado",             # CA: tender status
    "same_region",        # 1 = bidder region matches buyer region
    # SII fields
    "tramoventas",
    "ntrabajadores",
    "rubro",
    "tipodecontribuyente",
]

LIC_BID_COLS = [
    "Codigo",
    "RutProveedor",
    "Oferta seleccionada",
    "Valor Total Ofertado",
    "MontoLineaAdjudica",
    "Monto Estimado Adjudicado",
    "FechaAdjudicacion",
    "FechaPublicacion",
    "Estado",
    "MontoEstimado",
    "source_year",
    "source_month",
]

# ── SME definition helpers ────────────────────────────────────────────────────
# SII tramoventas: we detect the encoding in the diagnostics step.
# SME = codes 1..9 (micro + pequeña + mediana + sin información/ventas).
# Compra Ágil tamano: string labels from the platform.
CA_SME_LABELS = {
    "micro empresa", "microempresa",
    "pequeña empresa", "pequeña", "pequena empresa", "pequena",
    "mediana empresa", "mediana",
}

def _is_sme_tamano(s: pd.Series) -> pd.Series:
    """Flag SME from the Compra Ágil tamano field (string, case-insensitive)."""
    cleaned = s.astype(str).str.lower().str.strip()
    return cleaned.isin(CA_SME_LABELS).astype("Int8").where(s.notna(), other=pd.NA)


def _normalize_ascii_text(v) -> str:
    txt = str(v).strip().lower()
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _parse_integer_code(v) -> int | None:
    """Parse values like 7, 7.0, '7', '7.0' into int(7)."""
    if pd.isna(v):
        return None
    try:
        f = float(str(v).strip().replace(",", "."))
    except Exception:
        return None
    if not np.isfinite(f):
        return None
    i = int(round(f))
    return i if abs(f - i) < 1e-9 else None


def _sme_sii_mapping(unique_vals: list) -> dict:
    """
    Build a {value: is_sme} map from the observed tramoventas values.
    Handles both numeric codes and string labels.
    Canonical SII rule (no exceptions):
      1..9   -> SME (1)
      10..13 -> non-SME (0)
    """
    mapping = {}
    for v in unique_vals:
        if pd.isna(v):
            mapping[v] = pd.NA
            continue
        code = _parse_integer_code(v)
        if code is not None:
            if 1 <= code <= 9:
                mapping[v] = 1
            elif 10 <= code <= 13:
                mapping[v] = 0
            else:
                mapping[v] = pd.NA
            continue

        vs = _normalize_ascii_text(v)
        if ("sin informacion" in vs) or ("sin info" in vs) or ("sin ventas" in vs):
            mapping[v] = 1
        elif any(x in vs for x in ("micro", "pequena", "mediana")):
            mapping[v] = 1
        elif "grande" in vs:
            mapping[v] = 0
        else:
            mapping[v] = pd.NA
    return mapping


def _is_sme_sii(s: pd.Series) -> pd.Series:
    """Flag SME from SII tramoventas (with auto-detected encoding)."""
    unique_vals = s.dropna().unique().tolist()
    m = _sme_sii_mapping(unique_vals)
    return s.map(m).astype("Int8")


# ── Step 1: Load combined file ────────────────────────────────────────────────
def load_combined() -> pd.DataFrame:
    print("  Loading combined_sii_merged_filtered …")
    avail = pq.read_schema(COMBINED).names
    cols  = [c for c in COMBINED_COLS if c in avail]
    missing = [c for c in COMBINED_COLS if c not in avail]
    if missing:
        print(f"    [WARN] columns absent from combined file: {missing}")
    df = pd.read_parquet(COMBINED, columns=cols)
    df = df[~df["is_key_dup"].fillna(False)].copy()
    df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
    df = df[df["fecha_pub"].notna()].copy()
    print(f"    Rows loaded (post dedup): {len(df):,}")
    return df


def _clean_key(s: pd.Series) -> pd.Series:
    out = s.astype("string").str.strip()
    out = out.mask(out.str.lower().isin({"", "none", "nan", "null", "nat"}))
    return out


def _first_nonnull(s: pd.Series):
    s = s.dropna()
    if len(s) == 0:
        return np.nan
    return s.iloc[0]


def _build_bidder_id(df: pd.DataFrame) -> pd.Series:
    """
    Canonical bidder id for unique-bidder counting in RUT-DV form.
    Priority:
      1) rut_bidder + dv_bidder
      2) parsed RUT-DV from rut_bidder_raw
      3) synthetic per-row id when any part is missing
    """
    idx = df.index
    rut_num = pd.to_numeric(df.get("rut_bidder"), errors="coerce").round().astype("Int64")
    dv = (
        df.get("dv_bidder", pd.Series(pd.NA, index=idx, dtype="string"))
        .astype("string")
        .str.strip()
        .str.upper()
        .str.extract(r"([0-9K])")[0]
    )

    raw = df.get("rut_bidder_raw", pd.Series(pd.NA, index=idx, dtype="string")).astype("string")
    raw_clean = raw.str.strip().str.upper().str.replace(r"[^0-9K]", "", regex=True)
    raw_rut = pd.to_numeric(raw_clean.str.extract(r"^(\d+)")[0], errors="coerce").round().astype("Int64")
    raw_dv = raw_clean.str.extract(r"([0-9K])$")[0]

    rut_num = rut_num.where(rut_num.notna(), raw_rut)
    dv = dv.where(dv.notna(), raw_dv)

    bidder_id = rut_num.astype("string") + "-" + dv.astype("string")
    bad = rut_num.isna() | dv.isna()
    if bad.any():
        synthetic = "__missing_bidder_" + pd.Series(np.arange(len(df)), index=idx).astype(str)
        bidder_id = bidder_id.copy()
        bidder_id.loc[bad] = synthetic.loc[bad].astype("string")
    return bidder_id.astype("string")


def fill_compra_agil_sector_from_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing sector on Compra Ágil rows using rut_unidad→sector crosswalk.
    """
    out = df.copy()
    if "dataset" not in out.columns or "rut_unidad" not in out.columns:
        print("  [WARN] Missing dataset/rut_unidad columns; skip sector fill.")
        return out
    if "sector" not in out.columns:
        out["sector"] = pd.NA

    if not RUT_SECTOR_CROSSWALK.exists():
        print(f"  [WARN] Crosswalk file not found: {RUT_SECTOR_CROSSWALK}")
        print("        Proceeding without Compra Ágil sector fill.")
        return out

    xw = pd.read_parquet(RUT_SECTOR_CROSSWALK)
    if "rut_unidad" not in xw.columns:
        print(f"  [WARN] Crosswalk missing 'rut_unidad': {RUT_SECTOR_CROSSWALK}")
        return out
    sec_col = "sector_from_rutunidad" if "sector_from_rutunidad" in xw.columns else "sector"
    if sec_col not in xw.columns:
        print(f"  [WARN] Crosswalk missing sector column: {RUT_SECTOR_CROSSWALK}")
        return out

    mapper = (
        xw[["rut_unidad", sec_col]]
        .dropna(subset=["rut_unidad", sec_col])
        .drop_duplicates("rut_unidad")
        .set_index("rut_unidad")[sec_col]
    )

    ds = out["dataset"].astype("string")
    is_ca = ds.eq("compra_agil").fillna(False)

    rut_clean = _clean_key(out["rut_unidad"])
    sector_clean = _clean_key(out["sector"])
    need_fill = is_ca & sector_clean.isna()
    mapped = rut_clean.map(mapper)
    fill_mask = need_fill & mapped.notna()

    out.loc[fill_mask, "sector"] = mapped.loc[fill_mask]

    ca_total = int(is_ca.sum())
    miss_before = int(need_fill.sum())
    n_filled = int(fill_mask.sum())
    miss_after = int((is_ca & _clean_key(out["sector"]).isna()).sum())
    print("  Compra Ágil sector fill from crosswalk:")
    print(f"    CA rows total         : {ca_total:,}")
    print(f"    Missing sector before : {miss_before:,} ({100*miss_before/max(ca_total,1):.2f}%)")
    print(f"    Filled from crosswalk : {n_filled:,} ({100*n_filled/max(miss_before,1):.2f}% of missing)")
    print(f"    Missing sector after  : {miss_after:,} ({100*miss_after/max(ca_total,1):.2f}%)")

    return out


# ── Step 2a: Pull licitaciones submitted bids ─────────────────────────────────
def load_lic_bids() -> pd.DataFrame:
    """
    From chilecompra_panel, compute per-(tender, bidder) total submitted bid
    by summing Valor Total Ofertado across line items.

    Returns DataFrame with columns:
      tender_id, rut_bidder_raw, submitted_bid_lic,
      is_selected_lic, winning_bid_lic, fecha_adj, estado_lic
    """
    print("  Loading licitaciones bid amounts from chilecompra_panel …")
    avail = pq.read_schema(LIC_PANEL).names
    cols  = [c for c in LIC_BID_COLS if c in avail]
    lic   = pd.read_parquet(LIC_PANEL, columns=cols)

    lic["tender_id"]       = lic["Codigo"].astype(str).str.strip()
    lic["rut_bidder_raw"]  = lic["RutProveedor"].astype(str).str.strip()
    lic["is_sel"]          = lic["Oferta seleccionada"].astype(str).str.strip().eq("Seleccionada")
    lic["vtotal"]          = pd.to_numeric(lic.get("Valor Total Ofertado"), errors="coerce")
    lic["mla"]             = pd.to_numeric(lic.get("MontoLineaAdjudica"), errors="coerce")
    lic["mea"]             = pd.to_numeric(lic.get("Monto Estimado Adjudicado"), errors="coerce")
    lic["fecha_adj"]       = pd.to_datetime(lic.get("FechaAdjudicacion"), errors="coerce")
    lic["fecha_pub_panel"] = pd.to_datetime(lic.get("FechaPublicacion"), errors="coerce")
    lic["estado_raw"]      = lic.get("Estado", pd.Series(dtype=str)).astype(str).str.strip().str.lower()

    # Per-(tender, bidder): total submitted bid = sum of Valor Total Ofertado
    bid_totals = (
        lic.groupby(["tender_id", "rut_bidder_raw"])
        .agg(
            submitted_bid_lic=("vtotal", "sum"),
            is_selected_lic  =("is_sel",  "max"),
        )
        .reset_index()
    )
    bid_totals["submitted_bid_lic"] = bid_totals["submitted_bid_lic"].replace(0, np.nan)

    # Per-tender: winning bid, adjudication date, estado
    sel_rows = lic[lic["is_sel"]].copy()
    tender_meta = (
        sel_rows.groupby("tender_id")
        .agg(
            mla_sum   =("mla", "sum"),
            mea_max   =("mea", "max"),
            fecha_adj =("fecha_adj",       "first"),
            estado_lic=("estado_raw",      "first"),
            fecha_pub_panel=("fecha_pub_panel", "first"),
        )
        .reset_index()
    )
    tender_meta["winning_bid_lic"] = np.where(
        tender_meta["mea_max"].notna() & (tender_meta["mea_max"] > 0),
        tender_meta["mea_max"],
        tender_meta["mla_sum"],
    )
    tender_meta.loc[tender_meta["winning_bid_lic"] <= 0, "winning_bid_lic"] = np.nan
    tender_meta = tender_meta[["tender_id", "winning_bid_lic",
                               "fecha_adj", "estado_lic", "fecha_pub_panel"]].copy()

    out = bid_totals.merge(tender_meta, on="tender_id", how="left")
    print(f"    Bid rows: {len(out):,}  |  Tenders: {out['tender_id'].nunique():,}")
    return out


# ── Step 2b: Pull licitaciones desierto & days-to-award at tender level ───────
def extract_tender_metadata(lic_bids: pd.DataFrame) -> pd.DataFrame:
    """
    From the licitaciones bid pull, produce one row per tender with:
      tender_id, winning_bid_lic, fecha_adj, estado_lic, is_desierto_lic,
      days_to_award_lic
    (fecha_pub_panel is used for days_to_award but we'll use fecha_pub from
    combined for consistency; we keep it here as a cross-check.)
    """
    cols = ["tender_id", "winning_bid_lic", "fecha_adj",
            "estado_lic", "fecha_pub_panel"]
    meta = lic_bids[cols].drop_duplicates("tender_id").copy()
    meta["is_desierto_lic"] = meta["estado_lic"].str.contains(
        "desierta|desierto", na=False, regex=True
    ).astype("Int8")
    return meta


# ── Step 3: New-entrant flags ─────────────────────────────────────────────────
def compute_new_entrant(df: pd.DataFrame, bidder_col: str = "bidder_id") -> pd.Series:
    """
    For each row, flag whether the bidder's RUT appears for the first time
    on or after REFORM_DATE (i.e., never appeared before the reform).

    Returns an Int8 Series aligned to df.index.
    A bidder is a "new entrant" if their earliest bid date in the full dataset
    is >= REFORM_DATE.
    """
    print("  Computing new-entrant flags …")
    if bidder_col not in df.columns:
        raise KeyError(f"Missing bidder id column: {bidder_col}")

    first_dates = (
        df[df[bidder_col].notna() & df["fecha_pub"].notna()]
        .groupby(bidder_col)["fecha_pub"]
        .min()
        .rename("first_bid_date")
    )
    df2 = df[[bidder_col]].copy()
    df2 = df2.merge(first_dates, left_on=bidder_col,
                    right_index=True, how="left")
    flag = (df2["first_bid_date"] >= REFORM_DATE).astype("Int8")
    flag[df2["first_bid_date"].isna()] = pd.NA
    return flag.values


# ── Step 4: SME diagnostics ───────────────────────────────────────────────────
def run_sme_diagnostics(df: pd.DataFrame) -> None:
    """
    Print and save a comparison of SME classification from:
      (a) tamano   — Compra Ágil platform field (CA rows only)
      (b) tramoventas — SII annual sales bracket

    Diagnostics saved to output/did/tables/sme_diagnostics.csv
    """
    print("\n  ── SME diagnostics ──────────────────────────────────────────")

    # 1) tamano value counts
    print("\n  tamano (CA platform field) — value counts:")
    tv = df["tamano"].fillna("(missing)").value_counts()
    print(tv.to_string())

    # 2) tramoventas value counts
    print("\n  tramoventas (SII) — value counts:")
    sii_v = df["tramoventas"].fillna("(missing)").value_counts()
    print(sii_v.to_string())

    # 3) SME flags
    df = df.copy()
    df["sme_tamano"] = _is_sme_tamano(df["tamano"])
    df["sme_sii"]    = _is_sme_sii(df["tramoventas"])

    print("\n  sme_tamano distribution:")
    print(df["sme_tamano"].value_counts(dropna=False).to_string())
    print("\n  sme_sii distribution:")
    print(df["sme_sii"].value_counts(dropna=False).to_string())

    # 4) Agreement rate: rows where both are non-null
    both = df[df["sme_tamano"].notna() & df["sme_sii"].notna()].copy()
    if len(both) == 0:
        print("\n  [WARN] No rows with both tamano and tramoventas non-null.")
        diag = pd.DataFrame({"n_both": [0], "pct_agree": [np.nan]})
    else:
        agree_n = (both["sme_tamano"] == both["sme_sii"]).sum()
        agree_p = 100.0 * agree_n / len(both)
        print(f"\n  Agreement rate (both non-null): {agree_n:,}/{len(both):,} = {agree_p:.2f}%")

        cross = pd.crosstab(
            both["sme_tamano"].astype(str),
            both["sme_sii"].astype(str),
            rownames=["sme_tamano"],
            colnames=["sme_sii"],
            margins=True,
        )
        print("\n  Cross-tabulation (tamano vs SII):")
        print(cross.to_string())

        diag = pd.DataFrame({
            "n_ca_rows"          : [len(df[df["dataset"] == "compra_agil"])],
            "n_tamano_nonull"    : [int(df["sme_tamano"].notna().sum())],
            "n_sii_nonnull"      : [int(df["sme_sii"].notna().sum())],
            "n_both_nonnull"     : [len(both)],
            "n_agree"            : [int(agree_n)],
            "pct_agree"          : [round(agree_p, 4)],
        })

    diag.to_csv(OUT_TABLES / "sme_diagnostics.csv", index=False)
    print(f"\n  Saved: sme_diagnostics.csv")


# ── Step 5: Collapse to tender level ──────────────────────────────────────────
def build_tender_sample(
    df_bid: pd.DataFrame,
    lic_meta: pd.DataFrame,
) -> pd.DataFrame:
    """
    Collapse bid-level data to one row per tender.

    Tender-level outcomes computed here:
      n_bidders, n_local, n_nonlocal, share_local_bidders
      single_bidder
      any_sme_sii, sme_share_sii, winner_is_sme_sii
      share_bidders_not_in_sii, winner_not_in_sii
      share_sme_local_bidders, share_sme_nonlocal_bidders
      any_sme_local_bidder
      any_sme_tamano, sme_share_tamano, winner_is_sme_tamano  (CA only)
      winner_is_local
      new_entrant_winner, any_new_entrant
      log_win_price_ratio  (log winning_bid / monto_estimado)
      bid_cv               (CV of submitted bids across bidders)
      is_desierto          (from CA estado or licitaciones estado_lic)
      days_to_award        (licitaciones only, from adjudication date)

    Bidder counts/shares are based on unique bidder ids within each tender
    (not raw row counts).
    """
    print("  Collapsing to tender level …")
    df = df_bid.copy()

    # ── Locality ─────────────────────────────────────────────────────────
    # Preserve missing region-match information; do not coerce unknown to 0.
    sr = pd.to_numeric(df["same_region"], errors="coerce")
    df["is_local"] = np.where(sr == 1, 1.0, np.where(sr == 0, 0.0, np.nan))
    df["is_nonlocal"] = np.where(sr == 0, 1.0, np.where(sr == 1, 0.0, np.nan))

    # ── Submitted bid (unified: CA uses monto_oferta; lic uses submitted_bid_lic)
    df["submitted_bid"] = np.where(
        df["dataset"] == "compra_agil",
        df["monto_oferta"],
        df.get("submitted_bid_lic", np.nan),
    )
    df["submitted_bid"] = pd.to_numeric(df["submitted_bid"], errors="coerce")
    df["monto_oferta"] = pd.to_numeric(df.get("monto_oferta"), errors="coerce")
    df["winning_bid_lic"] = pd.to_numeric(df.get("winning_bid_lic"), errors="coerce")
    df["monto_estimado"] = pd.to_numeric(df["monto_estimado"], errors="coerce")

    # ── Winner flag ───────────────────────────────────────────────────────
    if "is_selected_lic" in df.columns:
        lic_mask = df["dataset"] == "licitaciones"
        df.loc[lic_mask, "is_selected"] = df.loc[lic_mask, "is_selected_lic"].fillna(
            df.loc[lic_mask, "is_selected"]
        )
    df["sel_flag"] = df["is_selected"].fillna(False).astype(bool)

    # Core tender metadata (first row within tender)
    meta = (
        df.groupby("tender_id", sort=False)
        .agg(
            dataset=("dataset", "first"),
            rut_unidad=("rut_unidad", "first"),
            region_buyer=("region_buyer", "first"),
            fecha_pub=("fecha_pub", "first"),
            source_year=("source_year", "first"),
            source_month=("source_month", "first"),
            monto_estimado=("monto_estimado", "first"),
            monto_utm=("monto_utm", "first"),
            band=("band", "first"),
            treated=("treated", "first"),
            post=("post", "first"),
            did=("did", "first"),
            year_month=("year_month", "first"),
            estado=("estado", "first"),
            is_desierto_lic=("is_desierto_lic", "first"),
            n_oferentes=("n_oferentes", "first"),
            sector=("sector", "first"),
        )
    )

    # One row per (tender, bidder): avoid inflating bidder counts when the same
    # bidder appears in multiple product-line rows within the tender.
    if "bidder_id" not in df.columns:
        df["bidder_id"] = _build_bidder_id(df)
    bidder_panel = (
        df.groupby(["tender_id", "bidder_id"], sort=False)
        .agg(
            is_local=("is_local", "first"),
            is_nonlocal=("is_nonlocal", "first"),
            sme_sii=("sme_sii", "first"),
            sme_tamano=("sme_tamano", "first"),
            is_new_entrant=("is_new_entrant", "first"),
            tramoventas=("tramoventas", "first"),
            submitted_bid=("submitted_bid", _first_nonnull),
        )
        .reset_index()
    )
    # Defensive check: one row should equal one unique bidder inside each tender.
    dup_tb = bidder_panel.duplicated(subset=["tender_id", "bidder_id"]).sum()
    if dup_tb:
        print(f"  [WARN] Found {dup_tb:,} duplicate tender×bidder rows; dropping duplicates.")
        bidder_panel = bidder_panel.drop_duplicates(subset=["tender_id", "bidder_id"], keep="first")
    bid_tid = bidder_panel["tender_id"]

    sme_sii_bp  = pd.to_numeric(bidder_panel["sme_sii"], errors="coerce").astype("float64")
    bidder_panel["is_local_flag"]    = (pd.to_numeric(bidder_panel["is_local"], errors="coerce").astype("float64") == 1).astype("int8")
    bidder_panel["is_nonlocal_flag"] = (pd.to_numeric(bidder_panel["is_nonlocal"], errors="coerce").astype("float64") == 1).astype("int8")
    bidder_panel["sme_flag"]         = (sme_sii_bp == 1).astype("int8")
    bidder_panel["large_flag"]       = (sme_sii_bp == 0).astype("int8")   # in SII, not SME
    if "tramoventas" in bidder_panel.columns:
        bidder_panel["nonsii_flag"]  = bidder_panel["tramoventas"].isna().astype("int8")
    else:
        bidder_panel["nonsii_flag"]  = sme_sii_bp.isna().astype("int8")
    counts = bidder_panel.groupby("tender_id", sort=False).agg(
        n_bidders       =("bidder_id",      "nunique"),
        n_local         =("is_local_flag",  "sum"),
        n_nonlocal      =("is_nonlocal_flag","sum"),
        n_sme_bidders   =("sme_flag",       "sum"),
        n_large_bidders =("large_flag",     "sum"),
        n_nonsii_bidders=("nonsii_flag",    "sum"),
    )
    meta = meta.join(counts)
    # Share among bidders with known region match status.
    known_region_bidders = meta["n_local"] + meta["n_nonlocal"]
    meta["share_local_bidders"] = np.where(
        known_region_bidders > 0,
        meta["n_local"] / known_region_bidders,
        np.nan,
    )
    meta["single_bidder"] = (meta["n_bidders"] == 1).astype("int8")

    # SME / entrant composition
    for col, any_name, share_name in [
        ("sme_sii", "any_sme_sii", "sme_share_sii"),
        ("sme_tamano", "any_sme_tamano", "sme_share_tamano"),
        ("is_new_entrant", "any_new_entrant", None),
    ]:
        if col in bidder_panel.columns:
            s = pd.to_numeric(bidder_panel[col], errors="coerce")
            nonnull_n = s.groupby(bid_tid, sort=False).count()
            s_max = s.groupby(bid_tid, sort=False).max()
            meta[any_name] = s_max.where(nonnull_n > 0, np.nan).astype("float64")
            if share_name is not None:
                s_mean = s.groupby(bid_tid, sort=False).mean()
                meta[share_name] = s_mean.where(nonnull_n > 0, np.nan).astype("float64")
        else:
            meta[any_name] = np.nan
            if share_name is not None:
                meta[share_name] = np.nan

    # Additional SII/SME composition outcomes
    if "tramoventas" in bidder_panel.columns:
        in_sii = bidder_panel["tramoventas"].notna().astype("float64")
        not_in_sii = 1.0 - in_sii
        meta["share_bidders_not_in_sii"] = not_in_sii.groupby(bid_tid, sort=False).mean().astype("float64")
    else:
        meta["share_bidders_not_in_sii"] = np.nan

    sme_sii = pd.to_numeric(bidder_panel.get("sme_sii"), errors="coerce").astype("float64")
    is_local = pd.to_numeric(bidder_panel.get("is_local"), errors="coerce").astype("float64")
    is_nonlocal = pd.to_numeric(bidder_panel.get("is_nonlocal"), errors="coerce").astype("float64")

    sme_local = pd.Series(
        np.where(
            (sme_sii == 1) & (is_local == 1),
            1.0,
            np.where(sme_sii.notna() & is_local.notna(), 0.0, np.nan),
        ),
        index=bidder_panel.index,
    )
    sme_nonlocal = pd.Series(
        np.where(
            (sme_sii == 1) & (is_nonlocal == 1),
            1.0,
            np.where(sme_sii.notna() & is_nonlocal.notna(), 0.0, np.nan),
        ),
        index=bidder_panel.index,
    )
    meta["share_sme_local_bidders"] = sme_local.groupby(bid_tid, sort=False).mean().astype("float64")
    meta["share_sme_nonlocal_bidders"] = sme_nonlocal.groupby(bid_tid, sort=False).mean().astype("float64")
    any_sme_local_n = sme_local.groupby(bid_tid, sort=False).count()
    any_sme_local_max = sme_local.groupby(bid_tid, sort=False).max()
    meta["any_sme_local_bidder"] = any_sme_local_max.where(any_sme_local_n > 0, np.nan).astype("float64")

    # Large-firm and non-SII shares
    large_local = pd.Series(
        np.where(
            (sme_sii == 0) & (is_local == 1),
            1.0,
            np.where(sme_sii.notna() & is_local.notna(), 0.0, np.nan),
        ),
        index=bidder_panel.index,
    )
    nonsii_flag_bp = bidder_panel["nonsii_flag"].astype("float64")
    nonsii_local = pd.Series(
        np.where(
            (nonsii_flag_bp == 1) & (is_local == 1),
            1.0,
            np.where(is_local.notna(), 0.0, np.nan),
        ),
        index=bidder_panel.index,
    )
    nonnull_sme = sme_sii.groupby(bid_tid, sort=False).count()
    meta["share_large_bidders"] = (
        (sme_sii == 0).astype("float64")
        .groupby(bid_tid, sort=False).mean()
        .where(nonnull_sme > 0, np.nan)
        .astype("float64")
    )
    meta["share_large_local_bidders"]  = large_local.groupby(bid_tid, sort=False).mean().astype("float64")
    meta["share_nonsii_local_bidders"] = nonsii_local.groupby(bid_tid, sort=False).mean().astype("float64")

    # Winner characteristics from selected rows only
    tid = df["tender_id"]
    sel_n = df["sel_flag"].astype("int8").groupby(tid, sort=False).sum().astype("float64")
    meta["_selected_n"] = sel_n

    win_local = df["is_local"].where(df["sel_flag"]).groupby(tid, sort=False).max()
    meta["winner_is_local"] = np.where(meta["_selected_n"] > 0, win_local, np.nan)

    for col, out_col in [
        ("sme_sii", "winner_is_sme_sii"),
        ("sme_tamano", "winner_is_sme_tamano"),
        ("is_new_entrant", "new_entrant_winner"),
    ]:
        if col in df.columns:
            s_sel = pd.to_numeric(df[col], errors="coerce").where(df["sel_flag"])
            has_nonnull = s_sel.groupby(tid, sort=False).count()
            s_max = s_sel.groupby(tid, sort=False).max()
            meta[out_col] = np.where(has_nonnull > 0, s_max, np.nan)
        else:
            meta[out_col] = np.nan

    if "tramoventas" in df.columns:
        winner_not_in_sii = pd.Series(
            np.where(df["tramoventas"].notna(), 0.0, 1.0),
            index=df.index,
        ).where(df["sel_flag"])
        winner_not_n = winner_not_in_sii.groupby(tid, sort=False).count()
        winner_not_max = winner_not_in_sii.groupby(tid, sort=False).max()
        meta["winner_not_in_sii"] = winner_not_max.where(winner_not_n > 0, np.nan).astype("float64")
    else:
        meta["winner_not_in_sii"] = np.nan

    # ── New winner characteristics ────────────────────────────────────────
    sme_sii_df = pd.to_numeric(df.get("sme_sii"), errors="coerce")
    is_local_df = pd.to_numeric(df.get("is_local"), errors="coerce")

    # winner_is_large: winner is in SII and NOT SME
    large_flag_df = pd.Series(
        np.where(sme_sii_df.notna(), (sme_sii_df == 0).astype("float64"), np.nan),
        index=df.index,
    )
    w_large = large_flag_df.where(df["sel_flag"])
    n_w_large = w_large.groupby(tid, sort=False).count()
    meta["winner_is_large"] = np.where(n_w_large > 0, w_large.groupby(tid, sort=False).max(), np.nan)

    # Composite winner × local flags
    for _flag, _name in [
        (
            pd.Series(np.where((sme_sii_df == 1) & (is_local_df == 1), 1.0,
                               np.where(sme_sii_df.notna() & is_local_df.notna(), 0.0, np.nan)),
                      index=df.index),
            "winner_is_sme_local",
        ),
        (
            pd.Series(np.where((sme_sii_df == 0) & (is_local_df == 1), 1.0,
                               np.where(sme_sii_df.notna() & is_local_df.notna(), 0.0, np.nan)),
                      index=df.index),
            "winner_is_large_local",
        ),
    ]:
        w_val = _flag.where(df["sel_flag"])
        n_w = w_val.groupby(tid, sort=False).count()
        meta[_name] = np.where(n_w > 0, w_val.groupby(tid, sort=False).max(), np.nan)

    if "tramoventas" in df.columns:
        nonsii_local_df = pd.Series(
            np.where(
                df["tramoventas"].isna() & (is_local_df == 1), 1.0,
                np.where(is_local_df.notna(), 0.0, np.nan),
            ),
            index=df.index,
        )
        w_nl = nonsii_local_df.where(df["sel_flag"])
        n_w_nl = w_nl.groupby(tid, sort=False).count()
        meta["winner_is_nonsii_local"] = np.where(n_w_nl > 0, w_nl.groupby(tid, sort=False).max(), np.nan)
    else:
        meta["winner_is_nonsii_local"] = np.nan

    # Winning bid by dataset
    ca_win = df["monto_oferta"].where((df["dataset"] == "compra_agil") & df["sel_flag"]).groupby(
        tid, sort=False
    ).sum(min_count=1)
    ca_win = ca_win.where(ca_win > 0)
    lic_win = df["winning_bid_lic"].groupby(tid, sort=False).first()
    meta["winning_bid"] = np.where(meta["dataset"] == "licitaciones", lic_win, ca_win)
    meta["winning_bid"] = pd.to_numeric(meta["winning_bid"], errors="coerce")
    meta.loc[meta["winning_bid"] <= 0, "winning_bid"] = np.nan

    # Cost outcome
    valid_log = (
        meta["winning_bid"].notna()
        & (meta["winning_bid"] > 0)
        & meta["monto_estimado"].notna()
        & (meta["monto_estimado"] > 0)
    )
    meta["log_win_price_ratio"] = np.where(
        valid_log,
        np.log(meta["winning_bid"] / meta["monto_estimado"]),
        np.nan,
    )

    # Bid dispersion
    sub_pos = bidder_panel["submitted_bid"].where(bidder_panel["submitted_bid"] > 0)
    sub_stats = sub_pos.groupby(bidder_panel["tender_id"], sort=False).agg(["count", "mean", "std"])
    meta["bid_cv"] = np.where(
        (sub_stats["count"] >= 2) & (sub_stats["mean"] > 0),
        sub_stats["std"] / sub_stats["mean"],
        np.nan,
    )

    # Minimum bid / reference price ratio
    min_bid = sub_pos.groupby(bidder_panel["tender_id"], sort=False).min()
    _valid_min = (
        min_bid.notna() & (min_bid > 0)
        & meta["monto_estimado"].notna() & (meta["monto_estimado"] > 0)
    )
    meta["log_min_price_ratio"] = np.where(
        _valid_min,
        np.log(min_bid / meta["monto_estimado"]),
        np.nan,
    )

    # Desierto
    ca_state = meta["estado"]
    ca_desierto = np.where(
        ca_state.notna(),
        ca_state.astype(str).str.lower().isin(("desierta", "desierto", "cancelada")),
        np.nan,
    )
    meta["is_desierto"] = np.where(meta["dataset"] == "compra_agil", ca_desierto, meta["is_desierto_lic"])

    tender = meta.reset_index().drop(columns=["estado", "is_desierto_lic", "_selected_n"], errors="ignore")

    # Merge in days_to_award for licitaciones
    if "fecha_adj" in lic_meta.columns:
        pub_dates = (
            df[df["dataset"] == "licitaciones"]
            .drop_duplicates("tender_id")[["tender_id", "fecha_pub"]]
        )
        days_df = lic_meta[["tender_id", "fecha_adj"]].merge(
            pub_dates, on="tender_id", how="left"
        )
        days_df["days_to_award"] = (
            days_df["fecha_adj"] - days_df["fecha_pub"]
        ).dt.days.astype("Int64")
        days_df = days_df[days_df["days_to_award"] > 0][
            ["tender_id", "days_to_award"]
        ]
        tender = tender.merge(days_df, on="tender_id", how="left")
    else:
        tender["days_to_award"] = pd.NA

    # Merge desierto for licitaciones if not already set
    if "is_desierto_lic" in lic_meta.columns:
        dis_meta = lic_meta[["tender_id", "is_desierto_lic"]].copy()
        tender = tender.merge(dis_meta, on="tender_id", how="left")
        lic_mask = tender["dataset"] == "licitaciones"
        tender.loc[lic_mask & tender["is_desierto"].isna(), "is_desierto"] = (
            tender.loc[lic_mask & tender["is_desierto"].isna(), "is_desierto_lic"]
        )
        tender = tender.drop(columns=["is_desierto_lic"], errors="ignore")

    print(f"    Tender-level rows: {len(tender):,}")
    return tender


def check_bidder_count_alignment(tender: pd.DataFrame) -> None:
    """
    Validate bidder-count measure against licitaciones NumeroOferentes.
    Saves diagnostics to output/did/tables/bidder_count_validation.csv.
    """
    if "n_oferentes" not in tender.columns:
        print("  [WARN] bidder-count check skipped: n_oferentes not available.")
        return

    lic = tender[tender["dataset"] == "licitaciones"].copy()
    lic["n_bidders"] = pd.to_numeric(lic["n_bidders"], errors="coerce")
    lic["n_oferentes"] = pd.to_numeric(lic["n_oferentes"], errors="coerce")
    sub = lic[lic["n_bidders"].notna() & lic["n_oferentes"].notna()].copy()
    if len(sub) == 0:
        print("  [WARN] bidder-count check skipped: no non-missing overlap.")
        return

    pearson = float(sub["n_bidders"].corr(sub["n_oferentes"], method="pearson"))
    spearman = float(sub["n_bidders"].corr(sub["n_oferentes"], method="spearman"))
    diff = sub["n_bidders"] - sub["n_oferentes"]
    mae = float(diff.abs().mean())
    rmse = float(np.sqrt((diff ** 2).mean()))
    exact_share = float((diff == 0).mean())
    within1_share = float((diff.abs() <= 1).mean())
    n_obs = int(len(sub))

    # By-year diagnostics
    by_year = (
        sub.assign(year=pd.to_datetime(sub["fecha_pub"], errors="coerce").dt.year)
        .dropna(subset=["year"])
        .groupby("year", sort=True)
        .apply(
            lambda g: pd.Series(
                {
                    "n_obs": int(len(g)),
                    "pearson_corr": float(g["n_bidders"].corr(g["n_oferentes"], method="pearson")),
                    "spearman_corr": float(g["n_bidders"].corr(g["n_oferentes"], method="spearman")),
                    "mae": float((g["n_bidders"] - g["n_oferentes"]).abs().mean()),
                }
            )
        )
        .reset_index()
    )

    summary = pd.DataFrame(
        [
            {
                "scope": "overall_licitaciones",
                "n_obs": n_obs,
                "pearson_corr": pearson,
                "spearman_corr": spearman,
                "mae": mae,
                "rmse": rmse,
                "exact_match_share": exact_share,
                "within_1_share": within1_share,
            }
        ]
    )
    out = OUT_TABLES / "bidder_count_validation.csv"
    summary.to_csv(out, index=False)
    print(f"  Saved: {out.name}")
    out_year = OUT_TABLES / "bidder_count_validation_by_year.csv"
    by_year.to_csv(out_year, index=False)
    print(f"  Saved: {out_year.name}")

    print("  Bidder-count validation (n_bidders vs NumeroOferentes, licitaciones):")
    print(f"    n_obs         : {n_obs:,}")
    print(f"    Pearson corr  : {pearson:.4f}")
    print(f"    Spearman corr : {spearman:.4f}")
    print(f"    MAE           : {mae:.4f}")
    print(f"    RMSE          : {rmse:.4f}")
    print(f"    Exact match   : {100.0 * exact_share:.2f}%")
    print(f"    Within +/-1   : {100.0 * within1_share:.2f}%")


# ── Step 6: Bid-level sample ──────────────────────────────────────────────────
def build_bid_sample(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return bid-level DataFrame with log(submitted price / reference price).
    Includes all bidders in the value bands, both datasets.
    """
    print("  Building bid-level sample …")
    cols = [
        "dataset", "tender_id", "bidder_id", "rut_bidder_raw", "rut_unidad",
        "region_buyer", "fecha_pub", "source_year", "source_month",
        "monto_estimado", "monto_utm", "band", "treated", "post", "did",
        "year_month", "submitted_bid", "is_selected",
        "same_region", "is_new_entrant",
        "sme_sii", "sme_tamano", "sector",
    ]
    cols_present = [c for c in cols if c in df.columns]
    bid = df[cols_present].copy()

    # One row per (tender, bidder) to keep bidder-level outcomes consistent
    # with the unique-bidder counting used throughout the pipeline.
    # We sort selected bids first, then keep the first row per tender×bidder.
    before_rows = len(bid)
    sort_cols = [c for c in ["tender_id", "bidder_id", "is_selected"] if c in bid.columns]
    if sort_cols:
        asc = [True] * len(sort_cols)
        if "is_selected" in sort_cols:
            asc[sort_cols.index("is_selected")] = False
        bid = bid.sort_values(sort_cols, ascending=asc)

    bid = bid.drop_duplicates(subset=["tender_id", "bidder_id"], keep="first").copy()
    print(f"    Deduplicated bid rows (tender×bidder): {len(bid):,} / {before_rows:,}")

    bid["log_sub_price_ratio"] = np.where(
        bid["submitted_bid"].notna()
        & (bid["submitted_bid"] > 0)
        & bid["monto_estimado"].notna()
        & (bid["monto_estimado"] > 0),
        np.log(bid["submitted_bid"] / bid["monto_estimado"]),
        np.nan,
    )
    valid = bid["log_sub_price_ratio"].notna() & np.isfinite(bid["log_sub_price_ratio"])
    print(f"    Bid rows with valid log_sub_price_ratio: {valid.sum():,} / {len(bid):,}")
    return bid


def filter_o2_or_obras(df: pd.DataFrame) -> pd.DataFrame:
    """Backward-compat alias for the obras_o2 preset."""
    return apply_sample_filter(df, sample="obras_o2")


def apply_sample_filter(
    df: pd.DataFrame,
    sample: str = "all",
    sector_contains: list[str] | None = None,
    tipo_in: list[str] | None = None,
    dataset_in: list[str] | None = None,
) -> pd.DataFrame:
    """
    Flexible sample restriction.

    Presets (`sample`)
      all             : no preset restriction
      obras_o2        : tipo == O2 OR sector contains 'obras'
      obras_sector    : sector contains 'obras'
      tipo_o2         : tipo == O2
      municipalidades : sector contains 'municipal'

    Additional filters (combined with AND):
      sector_contains : keep rows where sector contains ANY listed substring
      tipo_in         : keep rows where tipo is in listed values
      dataset_in      : keep rows where dataset is in listed values
    """
    out = df.copy()
    mask = pd.Series(True, index=out.index)
    tipo = out["tipo"].astype(str).str.upper().str.strip()
    sector = out["sector"].astype(str).str.lower().str.strip()
    dataset = out["dataset"].astype(str).str.strip()

    preset_desc = "all rows"
    if sample == "obras_o2":
        mask &= tipo.eq("O2") | sector.str.contains("obras", na=False)
        preset_desc = "tipo == O2 OR sector contains 'obras'"
    elif sample == "obras_sector":
        mask &= sector.str.contains("obras", na=False)
        preset_desc = "sector contains 'obras'"
    elif sample == "tipo_o2":
        mask &= tipo.eq("O2")
        preset_desc = "tipo == O2"
    elif sample == "municipalidades":
        mask &= sector.str.contains("municipal", na=False)
        preset_desc = "sector contains 'municipal'"

    if sector_contains:
        sec_terms = [s.lower().strip() for s in sector_contains if str(s).strip()]
        if sec_terms:
            sec_mask = pd.Series(False, index=out.index)
            for term in sec_terms:
                sec_mask |= sector.str.contains(term, na=False)
            mask &= sec_mask
            preset_desc += f" AND sector contains any of {sec_terms}"

    if tipo_in:
        tipo_set = {str(t).upper().strip() for t in tipo_in if str(t).strip()}
        if tipo_set:
            mask &= tipo.isin(tipo_set)
            preset_desc += f" AND tipo in {sorted(tipo_set)}"

    if dataset_in:
        ds_set = {str(d).strip() for d in dataset_in if str(d).strip()}
        if ds_set:
            mask &= dataset.isin(ds_set)
            preset_desc += f" AND dataset in {sorted(ds_set)}"

    out = out[mask].copy()
    print(f"  Applying sample filter: {preset_desc}")
    print(f"    Rows kept: {len(out):,} / {len(df):,} ({100.0 * len(out) / len(df):.2f}%)")
    print("    Dataset mix after filter:")
    print(out["dataset"].value_counts(dropna=False).to_string())
    if len(out):
        print("    Top sectors after filter:")
        print(out["sector"].value_counts(dropna=False).head(8).to_string())
    return out


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Build DiD samples.")
    parser.add_argument(
        "--sample",
        default="all",
        choices=["all", "obras_o2", "obras_sector", "tipo_o2", "municipalidades"],
        help="Preset sample restriction.",
    )
    parser.add_argument(
        "--obras-only",
        action="store_true",
        help="Alias for --sample obras_o2.",
    )
    parser.add_argument(
        "--sector-contains",
        nargs="+",
        default=None,
        help="Additional filter: keep rows where sector contains any listed term.",
    )
    parser.add_argument(
        "--tipo-in",
        nargs="+",
        default=None,
        help="Additional filter: keep rows where tipo is in listed codes.",
    )
    parser.add_argument(
        "--dataset-in",
        nargs="+",
        default=None,
        help="Additional filter: keep rows where dataset is in listed values.",
    )
    args = parser.parse_args()
    if args.obras_only:
        args.sample = "obras_o2"

    print("=" * 70)
    print("01_build_did_sample.py — Building DiD analysis samples")
    print("=" * 70)

    utm = load_utm_table()
    print(f"  UTM table: {len(utm)} months loaded.")

    # ── Load combined bid-level data ──────────────────────────────────────
    df = load_combined()
    df = fill_compra_agil_sector_from_crosswalk(df)
    df["bidder_id"] = _build_bidder_id(df)

    # ── Load licitaciones supplementary data ──────────────────────────────
    lic_bids = load_lic_bids()
    lic_meta = extract_tender_metadata(lic_bids)

    # ── Merge licitaciones bid amounts into combined ───────────────────────
    print("  Merging licitaciones bid amounts …")
    lic_bid_slim = lic_bids[["tender_id", "rut_bidder_raw",
                              "submitted_bid_lic", "is_selected_lic"]].copy()
    df = df.merge(lic_bid_slim, on=["tender_id", "rut_bidder_raw"], how="left")

    # ── Merge licitaciones tender metadata ────────────────────────────────
    df = df.merge(lic_meta, on="tender_id", how="left")

    # ── UTM conversion & band assignment ──────────────────────────────────
    print("  Converting to UTM and assigning value bands …")
    df = add_utm_value(df, utm)
    df = assign_band(df)
    print(f"  Rows in 1–200 UTM bands: {len(df):,}")
    print(df["band"].value_counts().to_string())

    # ── New-entrant flags ──────────────────────────────────────────────────
    df["is_new_entrant"] = compute_new_entrant(df, bidder_col="bidder_id")
    n_new = df["is_new_entrant"].fillna(0).astype(int).sum()
    print(f"  New entrants (first bid >= reform): {n_new:,} bid-rows")

    # ── SME flags ─────────────────────────────────────────────────────────
    print("  Computing SME flags …")
    df["sme_tamano"] = _is_sme_tamano(df["tamano"])
    df["sme_sii"]    = _is_sme_sii(df["tramoventas"])

    # ── SME diagnostics (on CA rows, where both fields may coexist) ───────
    ca_rows = df[df["dataset"] == "compra_agil"].copy()
    run_sme_diagnostics(ca_rows)

    # ── Unified submitted bid column ──────────────────────────────────────
    df["submitted_bid"] = np.where(
        df["dataset"] == "compra_agil",
        df["monto_oferta"],
        df["submitted_bid_lic"],
    )

    # ── Optional subset filter ────────────────────────────────────────────
    if (
        args.sample != "all"
        or args.sector_contains is not None
        or args.tipo_in is not None
        or args.dataset_in is not None
    ):
        df = apply_sample_filter(
            df,
            sample=args.sample,
            sector_contains=args.sector_contains,
            tipo_in=args.tipo_in,
            dataset_in=args.dataset_in,
        )

    # ── Tender-level sample ────────────────────────────────────────────────
    tender = build_tender_sample(df, lic_meta)
    check_bidder_count_alignment(tender)

    print("\n  Tender-level sample summary:")
    print(f"    Total tenders   : {len(tender):,}")
    for band in ["control_low", "treated", "control_high"]:
        n = (tender["band"] == band).sum()
        print(f"    {band:<16}: {n:,}")
    print(f"    Post-reform     : {tender['post'].sum():,}")
    print(f"    Missing log_win : {tender['log_win_price_ratio'].isna().sum():,}")

    # ── Bid-level sample ──────────────────────────────────────────────────
    bid = build_bid_sample(df)

    # ── Save ──────────────────────────────────────────────────────────────
    tender_out = OUT_SAMPLES / "did_tender_sample.parquet"
    bid_out    = OUT_SAMPLES / "did_bid_sample.parquet"

    tender.to_parquet(tender_out, index=False)
    bid.to_parquet(bid_out, index=False)

    print(f"\n  Saved: {tender_out}")
    print(f"  Saved: {bid_out}")
    print(f"\n  Tender sample size: {len(tender):,} rows, "
          f"{tender_out.stat().st_size / 1e6:.1f} MB")
    print(f"  Bid sample size   : {len(bid):,} rows, "
          f"{bid_out.stat().st_size / 1e6:.1f} MB")
    print("\nDone.")


if __name__ == "__main__":
    main()
