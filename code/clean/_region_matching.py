"""
Shared region normalization and matching helpers for SII ↔ ChileCompra merges.
"""

from __future__ import annotations

import re
import unicodedata

import numpy as np
import pandas as pd

REGION_KEY_TO_CC_LABEL = {
    "ARICA_PARINACOTA": "Región de Arica y Parinacota",
    "TARAPACA": "Región de Tarapacá",
    "ANTOFAGASTA": "Región de Antofagasta",
    "ATACAMA": "Región de Atacama",
    "COQUIMBO": "Región de Coquimbo",
    "VALPARAISO": "Región de Valparaíso",
    "OHIGGINS": "Región del Libertador General Bernardo O'Higgins",
    "MAULE": "Región del Maule",
    "BIOBIO": "Región del Biobío",
    "NUBLE": "Región de Ñuble",
    "ARAUCANIA": "Región de La Araucanía",
    "LOS_RIOS": "Región de Los Ríos",
    "LOS_LAGOS": "Región de Los Lagos",
    "AYSEN": "Región de Aysén del General Carlos Ibáñez del Campo",
    "MAGALLANES": "Región de Magallanes y de la Antártica Chilena",
    "METROPOLITANA": "Región Metropolitana de Santiago",
    "EXTRANJERO": "Extranjero",
}


def _normalize_text(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = text.replace("'", " ").replace("`", " ").replace("´", " ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def region_key_from_value(value: object) -> str | float:
    text = _normalize_text(value)
    if text is None:
        return np.nan

    if "SIN INFORMACION" in text:
        return np.nan
    if "EXTRANJER" in text:
        return "EXTRANJERO"
    if "METROPOLITANA" in text:
        return "METROPOLITANA"
    if "ARICA" in text or "PARINACOTA" in text:
        return "ARICA_PARINACOTA"
    if "TARAPACA" in text:
        return "TARAPACA"
    if "ANTOFAGASTA" in text:
        return "ANTOFAGASTA"
    if "ATACAMA" in text:
        return "ATACAMA"
    if "COQUIMBO" in text:
        return "COQUIMBO"
    if "VALPARAISO" in text:
        return "VALPARAISO"
    if "OHIGGINS" in text or "LIBERTADOR" in text:
        return "OHIGGINS"
    if "MAULE" in text:
        return "MAULE"
    if "BIO BIO" in text or "BIOBIO" in text:
        return "BIOBIO"
    if "NUBLE" in text or re.search(r"\bUBLE\b", text):
        return "NUBLE"
    if "ARAUCANIA" in text:
        return "ARAUCANIA"
    if "LOS RIOS" in text:
        return "LOS_RIOS"
    if "LOS LAGOS" in text:
        return "LOS_LAGOS"
    if "AYSEN" in text or "IBANEZ DEL CAMPO" in text:
        return "AYSEN"
    if "MAGALLANES" in text or "ANTARTICA" in text:
        return "MAGALLANES"

    return np.nan


def region_key_series(s: pd.Series) -> pd.Series:
    return s.map(region_key_from_value).astype("object")


def same_region_from_series(sii_region: pd.Series, buyer_region: pd.Series) -> pd.Series:
    sii_key = region_key_series(sii_region)
    buyer_key = region_key_series(buyer_region)
    both = sii_key.notna() & buyer_key.notna()
    out = pd.Series(np.nan, index=sii_key.index, dtype="float64")
    out.loc[both] = (sii_key.loc[both] == buyer_key.loc[both]).astype(float)
    return out
