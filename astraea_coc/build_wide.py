from __future__ import annotations

import pandas as pd
from .utils import norm_token

import re

BOOL_LIKE = re.compile(r"^(Yes|No|Nonexistent)$", re.IGNORECASE)


def _wide_map(df, section_prefix: str, max_index: int):
    if df is None or df.empty:
        return {f"{section_prefix}_{i}": "" for i in range(1, max_index + 1)}
    tmp = df[["index", "value"]].copy() if "value" in df.columns else df[["index"]].copy()
    if "value" in tmp.columns:
        tmp["value"] = tmp["value"].apply(norm_token)
        d = {int(k): v for k, v in zip(tmp["index"], tmp["value"])}
    else:
        d = {}
    out = {}
    for i in range(1, max_index + 1):
        out[f"{section_prefix}_{i}"] = d.get(i, "")
    return out


def build_wide(
    *,
    meta_vals: dict[str, str],

    # DataFrames that need special shaping
    df_1b1=None,

    df_1c1=None,
    df_1c2=None,
    df_1c3=None,
    df_1c4_basic=None,
    df_1c4c=None,
    df_1c5_basic=None,
    df_1c5c=None,
    df_1c6=None,
    df_1c7=None,
    df_1c7b=None,
    df_1c7c=None,

    df_1d1=None,
    df_1d4=None,
    df_1d6=None,
    df_1d9b=None,

    # Everything else (val_*/narr_*) comes in here automatically
    **scalars,
) -> pd.DataFrame:
    """
    Build the wide 1A/1B/1C/1D/1E row.

    Parsers may add new scalar keys (val_* or narr_*). Those will:
      1) Never crash this function (thanks to **scalars).
      2) Be auto-projected into wide columns at the end.

    Only add new explicit parameters here when introducing a NEW DataFrame
    that needs special shaping.
    """

    def S(key: str, default: str = "") -> str:
        v = scalars.get(key, default)
        return "" if v is None else str(v)

    wide: dict[str, str] = {
        "1a_1a": meta_vals.get("coc_name", ""),
        "1a_1b": meta_vals.get("coc_number", ""),
        "1a_2":  meta_vals.get("collab_app", ""),
        "1a_3":  meta_vals.get("designation", ""),
        "1a_4":  meta_vals.get("hmis_lead", ""),
    }

    # 1B-1 triplets (1..33)
    MAX_ORG_INDEX = 33
    if df_1b1 is not None and not df_1b1.empty:
        df_1b1 = df_1b1.copy()
        for c in ("meetings", "voted", "ces"):
            if c in df_1b1.columns:
                df_1b1[c] = df_1b1[c].apply(norm_token)
        lookup_1b1 = (
            df_1b1
            .set_index("org_type_index")[["meetings", "voted", "ces"]]
            .to_dict(orient="index")
        )
    else:
        lookup_1b1 = {}

    for i in range(1, MAX_ORG_INDEX + 1):
        rec = lookup_1b1.get(i, {})
        wide[f"1b_1_{i}_meetings"] = rec.get("meetings", "")
        wide[f"1b_1_{i}_voted"]    = rec.get("voted", "")
        wide[f"1b_1_{i}_ces"]      = rec.get("ces", "")

    # 1B text extras
    wide["1b_1a"] = S("narr_1b1a")
    wide["1b_2"]  = S("narr_1b2")
    wide["1b_3"]  = S("narr_1b3")
    wide["1b_4"]  = S("narr_1b4")

    # 1C-1, 1C-2
    wide.update(_wide_map(df_1c1, "1c_1", 17))
    wide.update(_wide_map(df_1c2, "1c_2", 4))

    # 1C-3
    if df_1c3 is not None:
        wide.update(_wide_map(df_1c3, "1c_3", 5))

    # 1C-4 basic yes/no
    if df_1c4_basic is not None and not df_1c4_basic.empty:
        df_1c4_basic = df_1c4_basic[df_1c4_basic["index"].between(1, 4)]
        df_1c4_basic = df_1c4_basic.drop_duplicates(subset="index", keep="first")
        d4 = {int(i): v for i, v in zip(df_1c4_basic["index"], df_1c4_basic["value"])}
    else:
        d4 = {}
    for i in range(1, 5):
        wide[f"1c_4_{i}"] = d4.get(i, "")
    wide["1c_4a"] = S("narr_1c4a")
    wide["1c_4b"] = S("narr_1c4b")

    # 1C-4c dual tokens
    if df_1c4c is not None and not df_1c4c.empty:
        for i in range(1, 10):
            row = df_1c4c[df_1c4c["index"] == i]
            wide[f"1c_4c_{i}_mou"] = row["mou"].iloc[0] if not row.empty else ""
            wide[f"1c_4c_{i}_oth"] = row["oth"].iloc[0] if not row.empty else ""
    else:
        for i in range(1, 10):
            wide[f"1c_4c_{i}_mou"] = ""
            wide[f"1c_4c_{i}_oth"] = ""

    # 1C-5 basic yes/no
    if df_1c5_basic is not None and not df_1c5_basic.empty:
        d5 = {int(i): v for i, v in zip(df_1c5_basic["index"], df_1c5_basic["value"])}
    else:
        d5 = {}
    for i in range(1, 4):
        wide[f"1c_5_{i}"] = d5.get(i, "")
    wide["1c_5a"] = S("narr_1c5a")
    wide["1c_5b"] = S("narr_1c5b")

    # 1C-5c dual tokens
    if df_1c5c is not None and not df_1c5c.empty:
        for i in range(1, 7):
            row = df_1c5c[df_1c5c["index"] == i]
            wide[f"1c_5c_{i}_proj"] = row["proj"].iloc[0] if not row.empty else ""
            wide[f"1c_5c_{i}_ces"]  = row["ces"].iloc[0]  if not row.empty else ""
    else:
        for i in range(1, 7):
            wide[f"1c_5c_{i}_proj"] = ""
            wide[f"1c_5c_{i}_ces"]  = ""

    wide["1c_5d"] = S("narr_1c5d")
    wide["1c_5e"] = S("narr_5e")
    wide["1c_5f"] = S("narr_5f")

    # 1C-6 yes/no
    if df_1c6 is not None and not df_1c6.empty:
        wide.update(_wide_map(df_1c6, "1c_6", 3))
    else:
        for i in range(1, 4):
            wide[f"1c_6_{i}"] = ""

    # 1C-6a + 1C-7a narratives
    wide["1c_6a"] = S("narr_1c6a")
    wide["1c_7a"] = S("narr_1c7a")

    # 1C-7b – Moving On Strategy with Affordable Housing Providers (4 items, Yes/No)
    wide.update(_wide_map(df_1c7b, "1c_7b", 4))

    # 1C-7c – PHA programs included in CE (7 items, Yes/No)
    wide.update(_wide_map(df_1c7c, "1c_7c", 7))

    # 1C-7d – joint CoC–PHA applications
    val_1c7d_1 = S("val_1c7d_1")
    narr_1c7d_2 = S("narr_1c7d_2")
    wide["1c_7d_1"] = val_1c7d_1

    if narr_1c7d_2 and narr_1c7d_2.strip():
        wide["1c_7d_2"] = narr_1c7d_2.strip()
    elif val_1c7d_1 != "Yes":
        wide["1c_7d_2"] = "Empty"
    else:
        wide["1c_7d_2"] = ""

    # 1C-7e – coordination with PHA(s) for HCV/EHV
    wide["1c_7e"] = S("val_1c7e")

    # 1C-7 – PHAs table → up to 2 rows into wide columns
    max_pha = 2
    for i in range(1, max_pha + 1):
        wide[f"1c_7_pha_name_{i}"]      = ""
        wide[f"1c_7_ph_hhm_{i}"]        = ""
        wide[f"1c_7_ph_limit_hhm_{i}"]  = ""
        wide[f"1c_7_psh_{i}"]           = ""

    if df_1c7 is not None and not df_1c7.empty:
        df_1c7 = df_1c7.reset_index(drop=True)
        for i in range(min(max_pha, len(df_1c7))):
            row = df_1c7.iloc[i]
            j = i + 1
            wide[f"1c_7_pha_name_{j}"]     = row.get("pha_name", "")
            wide[f"1c_7_ph_hhm_{j}"]       = row.get("ph_hhm", "")
            wide[f"1c_7_ph_limit_hhm_{j}"] = row.get("ph_limit_hhm", "")
            wide[f"1c_7_psh_{j}"]          = row.get("psh", "")

    # 1D-1 – public systems (1..4)
    wide.update(_wide_map(df_1d1, "1d_1", 4))

    # 1D-2 – numeric Housing First stats
    wide["1d_2_1"] = S("val_1d2_1")
    wide["1d_2_2"] = S("val_1d2_2")
    wide["1d_2_3"] = S("val_1d2_3")

    # 1D-2a – narrative + 1D-3 narrative
    wide["1d_2a"] = S("narr_1d2a")
    wide["1d_3"]  = S("narr_1d3")

    # 1D-4 – Strategies to Prevent Criminalization of Homelessness
    if df_1d4 is not None and not df_1d4.empty:
        df_1d4 = df_1d4.copy()
        df_1d4 = df_1d4[df_1d4["index"].between(1, 3)]
        for i in range(1, 4):
            row = df_1d4[df_1d4["index"] == i]
            wide[f"1d_4_{i}_policymakers"] = row["engaged"].iloc[0] if not row.empty else ""
            wide[f"1d_4_{i}_prevent_crim"] = row["implemented"].iloc[0] if not row.empty else ""
    else:
        for i in range(1, 4):
            wide[f"1d_4_{i}_policymakers"] = ""
            wide[f"1d_4_{i}_prevent_crim"] = ""

    # 1D-5 – Rapid Rehousing beds (HIC or HMIS)
    wide["1d_5_hmis"] = S("val_1d5_source")
    wide["1d_5_2023"] = S("val_1d5_2023")
    wide["1d_5_2024"] = S("val_1d5_2024")

    # 1D-6 – Mainstream benefits yes/no (1..6)
    if df_1d6 is not None and not df_1d6.empty:
        wide.update(_wide_map(df_1d6, "1d_6", 6))
    else:
        for i in range(1, 7):
            wide[f"1d_6_{i}"] = ""

    # 1D narratives
    wide["1d_6a"] = S("narr_1d6a")
    wide["1d_7"]  = S("narr_1d7")
    wide["1d_7a"] = S("narr_1d7a")
    wide["1d_8"]  = S("narr_1d8")
    wide["1d_8a"] = S("narr_1d8a")
    wide["1d_8b"] = S("narr_1d8b")

    # --- 1D-9: racial equity assessment ---
    wide["1d_9_1"] = S("val_1d9_1")
    wide["1d_9_2"] = S("val_1d9_2")
    wide["1d_9a"]  = S("narr_1d9a")

    # 1D-9b – 11 strategy items (Yes/No)
    if df_1d9b is not None:
        wide.update(_wide_map(df_1d9b, "1d_9b", 11))
    else:
        for i in range(1, 12):
            wide[f"1d_9b_{i}"] = ""

    wide["1d_9c"] = S("narr_1d9c")
    wide["1d_9d"] = S("narr_1d9d")

    # --- 1D-10: lived experience integration ---
    wide["1d_10"] = S("narr_1d10")

    wide["1d_10a_1_years"]       = S("val_1d10a_1_years")
    wide["1d_10a_1_unsheltered"] = S("val_1d10a_1_unsheltered")
    wide["1d_10a_2_years"]       = S("val_1d10a_2_years")
    wide["1d_10a_2_unsheltered"] = S("val_1d10a_2_unsheltered")
    wide["1d_10a_3_years"]       = S("val_1d10a_3_years")
    wide["1d_10a_3_unsheltered"] = S("val_1d10a_3_unsheltered")
    wide["1d_10a_4_years"]       = S("val_1d10a_4_years")
    wide["1d_10a_4_unsheltered"] = S("val_1d10a_4_unsheltered")

    wide["1d_10b"] = S("narr_1d10b")
    wide["1d_10c"] = S("narr_1d10c")

    # 1D-11
    wide["1d_11"] = S("val_1d11")

    # --- 1E: Project Capacity, Review, and Ranking ---
    wide["1e_1_1"] = S("val_1e_1_1")
    wide["1e_1_2"] = S("val_1e_1_2")

    for i in range(1, 7):
        wide[f"1e_2_{i}"] = S(f"val_1e_2_{i}")

    wide["1e_2a_1"] = S("val_1e_2a_1")
    wide["1e_2a_2"] = S("val_1e_2a_2")
    wide["1e_2a_3"] = S("val_1e_2a_3")

    wide["1e_2b"] = S("narr_1e_2b")
    wide["1e_3"]  = S("narr_1e_3")
    wide["1e_4"]  = S("narr_1e_4")
    wide["1e_4a"] = S("val_1e_4a")

    for i in range(1, 5):
        wide[f"1e_5_{i}"] = S(f"val_1e_5_{i}")

    wide["1e_5a"] = S("narr_1e_5a")
    wide["1e_5b"] = S("narr_1e_5b")
    wide["1e_5c"] = S("narr_1e_5c")
    wide["1e_5d"] = S("narr_1e_5d")

    wide["2a_1"] = S("val_2a_1")
    wide["2a_2"] = S("val_2a_2")
    wide["2a_3"] = S("val_2a_3")


    # ------------------------------
    # Auto-add any scalar fields that map directly to wide columns.
    # This keeps build_wide future-proof for new val_/narr_ keys.
    # ------------------------------

    for k, v in scalars.items():
        if not k.startswith(("val_", "narr_")):
            continue
        col = k.split("_", 1)[1]

        if col in wide:
            continue

        if k.startswith("val_") and isinstance(v, str):
            s = v.strip()
            # Only normalize real yes/no-ish fields
            if BOOL_LIKE.fullmatch(s):
                wide[col] = norm_token(s)
            else:
                wide[col] = s  # preserve capitalization + punctuation
        else:
            wide[col] = "" if v is None else str(v)

    # --- Ensure later-section columns exist even if not parsed yet ---
    # (placeholders so your wide_df has the full column list you requested)
    for key in [
        "1e_1_1", "1e_1_2",
        "1e_2_1", "1e_2_2", "1e_2_3", "1e_2_4", "1e_2_5", "1e_2_6",
        "1e_2a_1", "1e_2a_2", "1e_2a_3",
        "1e_2b",
        "1e_3",
        "1e_4", "1e_4a",
        "1e_5_1", "1e_5_2", "1e_5_3", "1e_5_4",
        "1e_5a", "1e_5b", "1e_5c", "1e_5d",
        "2a_1", "2a_2", "2a_3", "2a_4",
        "2a_5_1_non_vsp", "2a_5_1_vsp", "2a_5_1_hmis", "2a_5_1_coverage",
        "2a_5_2_non_vsp", "2a_5_2_vsp", "2a_5_2_hmis", "2a_5_2_coverage",
        "2a_5_3_non_vsp", "2a_5_3_vsp", "2a_5_3_hmis", "2a_5_3_coverage",
        "2a_5_4_non_vsp", "2a_5_4_vsp", "2a_5_4_hmis", "2a_5_4_coverage",
        "2a_5_5_non_vsp", "2a_5_5_vsp", "2a_5_5_hmis", "2a_5_5_coverage",
        "2a_5_6_non_vsp", "2a_5_6_vsp", "2a_5_6_hmis", "2a_5_6_coverage",
        "2a_5a", "2a_6",
        "2b_1", "2b_2", "2b_3", "2b_4",
        "2c_1", "2c_1a_1", "2c_1a_2", "2c_2", "2c_3", "2c_4", "2c_5", "2c_5a",
        "3a_1", "3a_2",
        "3c_1",
        "4a_1", "4a_1a_1", "4a_1a_2",
    ]:
        wide.setdefault(key, "")

    return pd.DataFrame([wide])


def col_order_extended() -> list[str]:
    cols = []
    cols += ["1a_1a", "1a_1b", "1a_2", "1a_3", "1a_4"]
    for i in range(1, 34):
        cols += [f"1b_1_{i}_meetings", f"1b_1_{i}_voted", f"1b_1_{i}_ces"]
    cols += ["1b_1a", "1b_2", "1b_3", "1b_4"]
    cols += [f"1c_1_{i}" for i in range(1, 18)]
    cols += [f"1c_2_{i}" for i in range(1, 5)]
    cols += [f"1c_3_{i}" for i in range(1, 6)]
    cols += [f"1c_4_{i}" for i in range(1, 5)] + ["1c_4a", "1c_4b"]
    for i in range(1, 10):
        cols += [f"1c_4c_{i}_mou", f"1c_4c_{i}_oth"]

    # --- 1C-5 / 1C-6 / 1C-7 ordering ---

    cols += [f"1c_5_{i}" for i in range(1, 4)] + ["1c_5a", "1c_5b"]

    for i in range(1, 7):
        cols += [f"1c_5c_{i}_proj", f"1c_5c_{i}_ces"]

    cols += ["1c_5d", "1c_5e", "1c_5f", "1c_6a"]

    for i in range(1, 3):
        cols += [
            f"1c_7_pha_name_{i}",
            f"1c_7_ph_hhm_{i}",
            f"1c_7_ph_limit_hhm_{i}",
            f"1c_7_psh_{i}",
        ]
    cols += ["1c_7a"]

    cols += [f"1c_7b_{i}" for i in range(1, 5)]
    cols += [f"1c_7c_{i}" for i in range(1, 8)]
    cols += ["1c_7d_1", "1c_7d_2", "1c_7e"]

    # 1D
    cols += [f"1d_1_{i}" for i in range(1, 5)]
    cols += ["1d_2_1", "1d_2_2", "1d_2_3", "1d_2a"]
    cols += ["1d_3"]

    for i in range(1, 4):
        cols += [f"1d_4_{i}_policymakers", f"1d_4_{i}_prevent_crim"]

    cols += ["1d_5_hmis", "1d_5_2023", "1d_5_2024"]
    cols += [f"1d_6_{i}" for i in range(1, 7)]
    cols += ["1d_6a", "1d_7", "1d_7a", "1d_8", "1d_8a", "1d_8b"]

    cols += ["1d_9_1", "1d_9_2", "1d_9a"]
    cols += [f"1d_9b_{i}" for i in range(1, 12)]
    cols += [
        "1d_9c", "1d_9d", "1d_10",
        "1d_10a_1_years", "1d_10a_1_unsheltered",
        "1d_10a_2_years", "1d_10a_2_unsheltered",
        "1d_10a_3_years", "1d_10a_3_unsheltered",
        "1d_10a_4_years", "1d_10a_4_unsheltered",
        "1d_10b", "1d_10c", "1d_11",
    ]

    cols += [
        "1e_1_1", "1e_1_2",
        "1e_2_1", "1e_2_2", "1e_2_3", "1e_2_4", "1e_2_5", "1e_2_6",
        "1e_2a_1", "1e_2a_2", "1e_2a_3",
        "1e_2b",
        "1e_3",
        "1e_4", "1e_4a",
        "1e_5_1", "1e_5_2", "1e_5_3", "1e_5_4",
        "1e_5a", "1e_5b", "1e_5c", "1e_5d",
        "2a_1", "2a_2", "2a_3", "2a_4",
        "2a_5_1_non_vsp", "2a_5_1_vsp", "2a_5_1_hmis", "2a_5_1_coverage",
        "2a_5_2_non_vsp", "2a_5_2_vsp", "2a_5_2_hmis", "2a_5_2_coverage",
        "2a_5_3_non_vsp", "2a_5_3_vsp", "2a_5_3_hmis", "2a_5_3_coverage",
        "2a_5_4_non_vsp", "2a_5_4_vsp", "2a_5_4_hmis", "2a_5_4_coverage",
        "2a_5_5_non_vsp", "2a_5_5_vsp", "2a_5_5_hmis", "2a_5_5_coverage",
        "2a_5_6_non_vsp", "2a_5_6_vsp", "2a_5_6_hmis", "2a_5_6_coverage",
        "2a_5a", "2a_6",
        "2b_1", "2b_2", "2b_3", "2b_4",
        "2c_1", "2c_1a_1", "2c_1a_2", "2c_2", "2c_3", "2c_4", "2c_5", "2c_5a",
        "3a_1", "3a_2",
        "3c_1",
        "4a_1", "4a_1a_1", "4a_1a_2",
    ]
    return cols
