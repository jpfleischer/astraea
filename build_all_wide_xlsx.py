#!/usr/bin/env python3
"""
build_all_wide_xlsx.py

Loop over all 2024 CoC application PDFs in a directory, starting
alphabetically from NJ-509 onward, run the astraea_coc pipeline on each
(in parallel), collect the resulting wide_df, and save one big stacked
sheet to an Excel workbook.
"""

from pathlib import Path
import argparse
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

import pandas as pd

from astraea_coc.pipeline import run_all
from astraea_coc.build_wide import col_order_extended


def select_pdfs_2024_from_nj509(apps_dir: Path) -> list[Path]:
    """Return sorted list of 2024 PDFs, starting at NJ-509 and onward."""
    # All PDFs in dir
    all_pdfs = [p for p in apps_dir.glob("*.pdf")]

    # Filter: 2024 only (filename contains '2024')
    pdf_2024 = [p for p in all_pdfs if "2024" in p.name]
    if not pdf_2024:
        print(f"ERROR: no 2024 .pdf files found in {apps_dir}", file=sys.stderr)
        return []

    # Sort alphabetically by filename
    pdf_2024.sort(key=lambda p: p.name.lower())

    # Find index of NJ-509 (e.g., 'NJ_509_2024.pdf' or 'NJ-509-2024.pdf')
    start_idx = 0
    anchor_found = False
    for i, p in enumerate(pdf_2024):
        name = p.name.lower()
        if "nj_509" in name or "nj-509" in name:
            start_idx = i
            anchor_found = True
            break

    if not anchor_found:
        print(
            "WARNING: No 2024 PDF containing 'NJ_509' or 'NJ-509' in the name "
            "was found. Processing all 2024 PDFs instead.",
            file=sys.stderr,
        )

    # Slice from NJ-509 onward (or from start if anchor not found)
    return pdf_2024[start_idx:]


def process_one_pdf(pdf: Path) -> pd.DataFrame | None:
    """
    Run the pipeline on a single PDF and return its wide_df with a __source_pdf column.

    Any exception is caught and logged; returns None in that case so the caller
    can just skip it.
    """
    try:
        print(f"[START] {pdf.name}", flush=True)
        # Use the PDF's parent dir as out_dir so per-PDF CSV/TXT still get written
        res = run_all(pdf, out_dir=pdf.parent)
    except Exception as exc:
        print(f"[ERROR] processing {pdf.name}: {exc}", file=sys.stderr)
        traceback.print_exc()
        return None

    wide_df = res.get("wide_df")
    if wide_df is None or wide_df.empty:
        print(
            f"[WARN] wide_df is empty or missing for {pdf.name}",
            file=sys.stderr,
        )
        return None

    wide_copy = wide_df.copy()
    wide_copy["__source_pdf"] = pdf.name
    print(f"[OK]   {pdf.name} -> {len(wide_copy)} row(s)", flush=True)
    return wide_copy


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build one stacked Excel workbook with wide_df rows for all 2024 "
            "CoC PDFs, starting alphabetically from NJ-509 onward, parsing in parallel."
        )
    )
    parser.add_argument(
        "apps_dir",
        help="Directory containing CoC application PDFs (e.g., apps-.../apps)",
    )
    parser.add_argument(
        "-o",
        "--output-xlsx",
        default="coc_apps_all_wide_2024_from_NJ509.xlsx",
        help="Output Excel filename (default: coc_apps_all_wide_2024_from_NJ509.xlsx)",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=os.cpu_count() or 4,
        help="Number of worker processes to use (default: CPU count).",
    )
    args = parser.parse_args()

    apps_dir = Path(args.apps_dir).expanduser().resolve()
    if not apps_dir.is_dir():
        print(f"ERROR: {apps_dir} is not a directory", file=sys.stderr)
        return 1

    pdf_paths = select_pdfs_2024_from_nj509(apps_dir)
    if not pdf_paths:
        return 1

    print(f"Will process {len(pdf_paths)} PDFs (2024 only, from NJ-509 onward):")
    for p in pdf_paths:
        print(f"  - {p.name}")

    all_wide: list[pd.DataFrame] = []

    # Parallel processing of PDFs
    print(f"\nUsing {args.jobs} worker process(es).\n")
    with ProcessPoolExecutor(max_workers=args.jobs) as executor:
        future_to_pdf = {executor.submit(process_one_pdf, pdf): pdf for pdf in pdf_paths}

        for fut in as_completed(future_to_pdf):
            pdf = future_to_pdf[fut]
            try:
                df = fut.result()
            except Exception as exc:
                # Should be rare, since process_one_pdf already catches exceptions.
                print(
                    f"[ERROR] Worker crashed while processing {pdf.name}: {exc}",
                    file=sys.stderr,
                )
                traceback.print_exc()
                continue

            if df is None or df.empty:
                continue

            all_wide.append(df)

    if not all_wide:
        print("ERROR: No wide_df rows collected from any PDFs.", file=sys.stderr)
        return 1

    # Stack into one big DataFrame
    combined = pd.concat(all_wide, ignore_index=True)

    # Reorder columns: official col_order_extended first, then extras (like __source_pdf)
    base_cols = col_order_extended()
    ordered_cols = [c for c in base_cols if c in combined.columns]
    extra_cols = [c for c in combined.columns if c not in ordered_cols]
    combined = combined[ordered_cols + extra_cols]
    # ---- NEW: sort by 2nd column of the final sheet ----
    if combined.shape[1] >= 2:
        second_col = combined.columns[1]
        combined = combined.sort_values(
            by=second_col,
            kind="stable",
            key=lambda s: s.astype(str).str.lower(),  # case-insensitive, safe for NaNs
        ).reset_index(drop=True)
    # ----------------------------------------------------

    out_path = Path(args.output_xlsx).expanduser().resolve()
    combined.to_excel(out_path, index=False)
    print(f"\nWrote {len(combined)} rows to {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
