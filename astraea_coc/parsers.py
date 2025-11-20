
from __future__ import annotations
import re
import pandas as pd
from .utils import norm_token

def parse_triple_table(norm_lines: list[str]) -> pd.DataFrame:
    TOK = r"(Yes|No|Nonexistent)"
    TRIPLE_RX = re.compile(rf"\b{TOK}\s+{TOK}\s+{TOK}\b$", flags=re.IGNORECASE)
    LEAD_RX   = re.compile(r"^(\d+)\.\s*(.*)$")

    def has_triple(s: str) -> bool:
        return TRIPLE_RX.search(s) is not None

    def extract_triple(s: str):
        m = TRIPLE_RX.search(s)
        return [m.group(i).strip().title() for i in range(1, 4)] if m else [None, None, None]

    def strip_triple(s: str):
        m = TRIPLE_RX.search(s);  return s[:m.start()].rstrip() if m else s

    rows, i = [], 0
    while i < len(norm_lines):
        line = norm_lines[i].strip(); i += 1
        m = LEAD_RX.match(line)
        if not m:
            continue
        idx = int(m.group(1)); label = m.group(2)
        stack = [label]

        while not has_triple(" ".join(stack)) and i < len(norm_lines):
            nxt = norm_lines[i].strip()
            if LEAD_RX.match(nxt): break
            stack.append(nxt); i += 1

        full = " ".join(stack)
        if not has_triple(full):
            continue

        tokens = extract_triple(full)
        clean_label = re.sub(r"\s+", " ", strip_triple(full)).strip()

        rows.append({
            "org_type_index": idx,
            "org_type": clean_label,
            "meetings": norm_token(tokens[0]),
            "voted": norm_token(tokens[1]),
            "ces": norm_token(tokens[2]),
        })
    df = pd.DataFrame(rows).sort_values("org_type_index").reset_index(drop=True)
    allowed = {"Yes","No","Nonexistent"}
    for c in ["meetings","voted","ces"]:
        df[c] = df[c].apply(norm_token)
    if not df.empty:
        assert df[["meetings","voted","ces"]].isin(allowed).all(axis=1).all(), "Unexpected tokens detected in 1B-1."
    return df

def parse_numbered_yesno(norm_lines: list[str], allowed=("Yes","No","Nonexistent")) -> pd.DataFrame:
    LEAD = re.compile(r"^(\d+)\.\s*(.*)$")
    TOK  = re.compile(rf"\b({'|'.join(allowed)})\b$", re.IGNORECASE)

    rows, i = [], 0
    while i < len(norm_lines):
        line = norm_lines[i].strip(); i += 1
        m = LEAD.match(line)
        if not m:
            continue

        idx = int(m.group(1)); label = m.group(2)
        stack = [label]

        while not TOK.search(" ".join(stack)) and i < len(norm_lines):
            nxt = norm_lines[i].strip()
            if LEAD.match(nxt): break
            stack.append(nxt); i += 1

        full = " ".join(stack)
        m2 = TOK.search(full)
        if not m2:
            continue

        value = norm_token(m2.group(1).title())
        label_clean = re.sub(r"\s+", " ", full[:m2.start()].strip())

        rows.append({"index": idx, "label": label_clean, "value": value})

    df = pd.DataFrame(rows).sort_values("index").reset_index(drop=True)
    if not df.empty:
        df["value"] = df["value"].apply(norm_token)
        assert df["value"].isin(set(allowed)).all(), "Unexpected tokens detected in numbered Yes/No section."
    return df

def parse_numbered_dual_tokens(norm_lines, suffixes=("left","right"),
                               allowed=("Yes","No","Nonexistent")) -> pd.DataFrame:
    LEAD = re.compile(r"^(\d+)[\.\)\-]?\s+(.*)$")
    TOK  = re.compile(rf"(Yes|No|Nonexistent)\b", re.IGNORECASE)

    rows, i = [], 0
    while i < len(norm_lines):
        line = norm_lines[i].strip(); i += 1
        m = LEAD.match(line)
        if not m:
            continue
        idx = int(m.group(1)); label = m.group(2)

        stack = [label]
        while i < len(norm_lines):
            probe = " ".join(stack)
            toks = TOK.findall(probe)
            if len(toks) >= 2:
                break
            nxt = norm_lines[i].strip()
            if LEAD.match(nxt):
                break
            stack.append(nxt); i += 1

        full = " ".join(stack)
        toks = [t.title() for t in TOK.findall(full)]
        if len(toks) < 2:
            continue

        val0 = norm_token(toks[-2])
        val1 = norm_token(toks[-1])

        tail = re.search(r"(Yes|No|Nonexistent)\s+(Yes|No|Nonexistent)\s*$", full, flags=re.IGNORECASE)
        clean_label = full[:tail.start()].strip() if tail else full

        rows.append({"index": idx, "label": clean_label, suffixes[0]: val0, suffixes[1]: val1})

    df = pd.DataFrame(rows).sort_values("index").reset_index(drop=True)
    return df

def parse_1c7_pha(norm_lines: list[str]) -> pd.DataFrame:
    """
    Parse the 1C-7 PHA table into rows:
      pha_name, ph_hhm (percent), ph_limit_hhm (homeless pref text), psh (Yes/No).
    """
    rows = []
    pending = None  # {"line": str, "cont": [str, ...]}

    header_keywords = [
        "Public Housing Agency Name",
        "Enter the Percent of New Admissions",
        "During FY 2023 who were experiencing",
        "homelessness at entry",
        "participants no longer",
        "supportive services",
        "Moving On?",
        "CoC-PHA Crosswalk Report",
        "Enter information in the chart below",
        "FY2024 CoC Application Page",
    ]

    for ln in norm_lines:
        # skip obvious non-table lines
        if any(k in ln for k in header_keywords):
            continue
        if ln.startswith("Applicant:") or ln.startswith("Project:"):
            continue
        if ln.startswith("1C-7."):
            continue
        if ln.startswith("NOFO Section"):
            continue

        # new row line if it contains a percentage
        if re.search(r"\d+%\s", ln):
            # flush previous pending row (if any)
            if pending is not None:
                full = pending["line"]
                cont = pending["cont"]
                m = re.search(r"(\d+)%", full)
                if m:
                    name = full[:m.start()].strip()
                    # continuation lines belong to the name (e.g., "Affairs")
                    if cont:
                        name = name + " " + " ".join(cont)
                    tail = full[m.end():].strip()
                    toks = tail.split()
                    if len(toks) >= 2:
                        psh = toks[-1]
                        ph_limit = " ".join(toks[:-1])
                        rows.append({
                            "pha_name": name,
                            "ph_hhm": m.group(1) + "%",
                            "ph_limit_hhm": ph_limit,
                            "psh": psh,
                        })

            # start new pending row
            pending = {"line": ln, "cont": []}

        else:
            # continuation line (e.g., "Affairs")
            if pending is not None:
                pending["cont"].append(ln)

    # flush last pending row
    if pending is not None:
        full = pending["line"]
        cont = pending["cont"]
        m = re.search(r"(\d+)%", full)
        if m:
            name = full[:m.start()].strip()
            if cont:
                name = name + " " + " ".join(cont)
            tail = full[m.end():].strip()
            toks = tail.split()
            if len(toks) >= 2:
                psh = toks[-1]
                ph_limit = " ".join(toks[:-1])
                rows.append({
                    "pha_name": name,
                    "ph_hhm": m.group(1) + "%",
                    "ph_limit_hhm": ph_limit,
                    "psh": psh,
                })

    return pd.DataFrame(rows)
