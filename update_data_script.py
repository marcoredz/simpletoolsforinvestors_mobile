"""
Daily update of bond data.

Sources and priority (highest to lowest):
  1. data/overrides.yml     -> manual corrections, always take precedence
  2. data/bonds_master.json  -> local reference data, written when an ISIN first appears
  3. SimpleToolsForInvestors "End of Day Data" CSV -> prices, yields, and new ISINs

Output (same schema as before; the app remains unchanged):
  docs/output_enriched.json

Service file:
  data/source_diffs.json     -> differences between source and master reference data (not applied)

Local usage:
  python update_data_script.py                  # download + processing
  python update_data_script.py --skip-download  # use the existing downloaded.csv

All data paths are relative to this script's directory, regardless of the working directory.
"""

from urllib.parse import urljoin
from datetime import datetime, date, timedelta
from pathlib import Path
import argparse
import json
import math
import os
import re
import sys

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = SCRIPT_DIR / "downloaded.csv"
OUTPUT_PATH = SCRIPT_DIR / "docs/output_enriched.json"
MASTER_PATH = SCRIPT_DIR / "data/bonds_master.json"
OVERRIDES_PATH = SCRIPT_DIR / "data/overrides.yml"
DIFFS_PATH = SCRIPT_DIR / "data/source_diffs.json"

# Reference fields: written to the master only when the ISIN first appears.
# If the source later reports a different value, it is only flagged.
STATIC_FIELDS = [
    "isincode", "description", "redemptiondate", "currencycode",
    "issuercode", "issuerdescription", "minimumlot", "status",
    "couponperiodicity", "couponmonths",
]

# Fields that change infrequently: updated from the source, but can be corrected by overrides.
SEMI_STATIC_FIELDS = ["ratingsp", "ratingmoodys", "ratingfitch", "currentcouponrate"]

# Fields retrieved separately from the SimpleTools yieldtable page.
ENRICHED_FIELDS = ["bondid"]

# All other CSV fields are considered daily data and are always taken from the source.

# How often to retry retrieving missing bondid values, in days.
ENRICH_RETRY_DAYS = 7

# If the CSV contains less than this fraction of the securities in the previous output,
# execution stops to avoid publishing an incomplete file.
MIN_ROWS_RATIO = 0.5

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}
HTTP_TIMEOUT = 60


def log_message(step, message, status="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{status}] STEP {step}: {message}")


# ---------------------------------------------------------------------------
# File reading / writing
# ---------------------------------------------------------------------------

def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError as e:
        log_message("IO", f"{path} is not valid JSON: {e}", "ERROR")
        sys.exit(1)


def save_json(path, data, indent=2):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
        f.write("\n")
    tmp.replace(path)


def load_overrides():
    """
    Expected format:
        IT0005XXXXXX:
          minimumlot: 1
          _nota: "free text, ignored"
          _exclude: true   # optional: hides the security from the output
    """
    if not OVERRIDES_PATH.exists():
        log_message("OVR", f"Overrides file not found: {OVERRIDES_PATH}; no overrides applied", "WARNING")
        return {}
    try:
        with open(OVERRIDES_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        # Syntax errors in overrides must not go unnoticed.
        log_message("OVR", f"Syntax error in {OVERRIDES_PATH}: {e}", "ERROR")
        sys.exit(1)
    if not isinstance(data, dict):
        log_message("OVR", f"{OVERRIDES_PATH} must contain an ISIN -> fields mapping", "ERROR")
        sys.exit(1)
    clean = {}
    for isin, fields in data.items():
        if not isinstance(fields, dict):
            log_message("OVR", f"Override for {isin} ignored: it is not a mapping", "WARNING")
            continue
        clean[str(isin).strip().upper()] = fields
    log_message("OVR", f"Caricati override per {len(clean)} ISIN")
    return clean


# ---------------------------------------------------------------------------
# Source download and parsing (unchanged from the previous version)
# ---------------------------------------------------------------------------

def step0_download_stfi_csv():
    log_message(0, "Starting CSV download from STFI")
    url = "https://www.simpletoolsforinvestors.eu/documentivari.php"
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        log_message(0, f"Unable to fetch the page: {e}", "ERROR")
        sys.exit(1)

    soup = BeautifulSoup(response.content, "html.parser")
    csv_link = None
    for a_tag in soup.find_all("a", href=True):
        link_text = " ".join(a_tag.get_text(" ", strip=True).split())
        if link_text.casefold() == "Dati End of Day".casefold():
            csv_link = urljoin(url, a_tag["href"])
            break

    if not csv_link:
        log_message(0, "CSV link not found", "ERROR")
        sys.exit(1)

    try:
        log_message(0, f"Found CSV link: {csv_link}")
        csv_response = requests.get(csv_link, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT)
        csv_response.raise_for_status()
        CSV_PATH.write_bytes(csv_response.content)
        log_message(0, "CSV file successfully downloaded", "SUCCESS")
    except requests.RequestException as e:
        log_message(0, f"Error downloading CSV file: {e}", "ERROR")
        sys.exit(1)


def step1_csv_to_records(file_path):
    log_message(1, "Starting CSV parsing")
    try:
        try:
            df = pd.read_csv(file_path, sep=";", decimal=",", on_bad_lines="skip", encoding="utf-8")
            if len(df.columns) <= 1:
                raise ValueError("separatore ';' non valido")
            log_message(1, "CSV successfully read using ';' separator")
        except Exception:
            df = pd.read_csv(file_path, sep=",", decimal=",", on_bad_lines="skip", encoding="utf-8")
            log_message(1, "CSV successfully read using ',' separator")

        df = df.loc[:, ~df.columns.str.lower().str.startswith("unnamed")]

        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass

        # to_json converts NaN to null; json.loads converts them back to None
        records = json.loads(df.to_json(orient="records", force_ascii=False))
        log_message(1, f"Parsing completed: {len(records)} records found", "SUCCESS")
        return records, list(df.columns)
    except Exception as e:
        log_message(1, f"Error during CSV parsing: {e}", "ERROR")
        return None, None


def step2_fetch_isin_bondid_mapping():
    """Return the ISIN -> bondId mapping, or None if the page is unreachable."""
    log_message(2, "Starting ISIN-bondId mapping retrieval")
    url = "https://www.simpletoolsforinvestors.eu/yieldtable.php?datatype=EOD"
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        table_wrapper = soup.find(id="YieldTable")
        if not table_wrapper:
            raise ValueError("YieldTable not found")

        isin_bond_map = {}
        for row in table_wrapper.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue
            isin = cells[0].text.strip()
            for cell in cells:
                link = cell.find("a")
                if link and "href" in link.attrs:
                    match = re.search(r"bondID=(\d+)", link["href"])
                    if match:
                        isin_bond_map[isin] = match.group(1)
                        break
        log_message(2, f"Mapping completed: {len(isin_bond_map)} ISIN-bondId pairs found", "SUCCESS")
        return isin_bond_map
    except Exception as e:
        log_message(2, f"Error retrieving ISIN-bondId mapping: {e}", "ERROR")
        return None


# ---------------------------------------------------------------------------
# Master data
# ---------------------------------------------------------------------------

def bootstrap_master_from_output():
    """On first run, build the master from the already published JSON so previously
    retrieved bondid values are preserved."""
    existing = load_json(OUTPUT_PATH, default=None)
    if not existing:
        log_message("MASTER", "Nessun master né output esistente: si parte da zero", "WARNING")
        return {}
    master = {}
    for row in existing:
        isin = row.get("isincode")
        if not isin:
            continue
        rec = {f: row.get(f) for f in STATIC_FIELDS + SEMI_STATIC_FIELDS + ENRICHED_FIELDS}
        rec["_meta"] = {
            "first_seen": None,          # unknown: imported from the previous output
            "to_review": False,
            "enrich_last_attempt": None,
        }
        master[isin] = rec
    log_message("MASTER", f"Master creato dal vecchio output: {len(master)} titoli", "SUCCESS")
    return master


def same_value(a, b):
    if a is None or b is None:
        return a is None and b is None
    num = (int, float)
    if isinstance(a, num) and isinstance(b, num) and not isinstance(a, bool) and not isinstance(b, bool):
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    return str(a).strip() == str(b).strip()


def needs_enrichment(rec, today):
    if rec.get("bondid") is not None:
        return False
    last = rec.get("_meta", {}).get("enrich_last_attempt")
    if not last:
        return True
    return date.fromisoformat(last) <= today - timedelta(days=ENRICH_RETRY_DAYS)


def enrich(master, isins, today):
    if not isins:
        log_message(3, "No securities to enrich")
        return
    log_message(3, f"Retrieving missing bondid values for {len(isins)} securities")

    mapping = None
    if any(master[i].get("bondid") is None for i in isins):
        mapping = step2_fetch_isin_bondid_mapping()
        if mapping is None:
            log_message(3, "Mapping non disponibile: riproverò alla prossima esecuzione", "WARNING")
            return

    found = 0
    for isin in isins:
        rec = master[isin]
        if rec.get("bondid") is None and mapping and isin in mapping:
            rec["bondid"] = mapping[isin]
        if rec.get("bondid") is not None:
            found += 1
        rec["_meta"]["enrich_last_attempt"] = today.isoformat()
    log_message(3, f"Arricchimento completato: {found}/{len(isins)} completi", "SUCCESS")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-download", action="store_true",
                        help="usa downloaded.csv già presente invece di scaricarlo")
    args = parser.parse_args()

    today = date.today()
    today_s = today.isoformat()

    print("=" * 80)
    print("STARTING BOND DATA PROCESSING")
    print("=" * 80)

    # --- Master data and overrides -----------------------------------------
    master = load_json(MASTER_PATH, default=None)
    if master is None:
        master = bootstrap_master_from_output()
    else:
        log_message("MASTER", f"Master caricato: {len(master)} titoli")
    overrides = load_overrides()

    # --- Source -------------------------------------------------------------
    if not args.skip_download:
        step0_download_stfi_csv()
    rows, csv_columns = step1_csv_to_records(CSV_PATH)
    if not rows:
        log_message(1, "Elaborazione interrotta: CSV vuoto o illeggibile", "ERROR")
        sys.exit(1)

    isin_column = next((c for c in csv_columns if "ISIN" in c.upper()), None)
    if not isin_column:
        log_message(1, "Colonna ISIN non trovata nel CSV", "ERROR")
        sys.exit(1)

    previous_output = load_json(OUTPUT_PATH, default=[]) or []
    if previous_output and len(rows) < MIN_ROWS_RATIO * len(previous_output):
        log_message(1, f"Il CSV ha solo {len(rows)} titoli contro {len(previous_output)} di ieri: "
                       "probabile problema della fonte, non pubblico nulla", "ERROR")
        sys.exit(1)

    # --- Master update ------------------------------------------------------
    new_isins, today_isins, diffs_today = [], [], {}
    for row in rows:
        isin = row.get(isin_column)
        if not isin:
            continue
        isin = str(isin).strip().upper()
        row[isin_column] = isin
        today_isins.append(isin)
        rec = master.get(isin)

        if rec is None:
            rec = {f: row.get(f) for f in STATIC_FIELDS + SEMI_STATIC_FIELDS}
            rec["bondid"] = None
            rec["_meta"] = {"first_seen": today_s, "to_review": True,
                            "enrich_last_attempt": None}
            master[isin] = rec
            new_isins.append(isin)
            continue

        # Do not update "last_seen" every day: that would make the master diff
        # noisy. Record only when a security starts missing from the source.
        meta = rec.setdefault("_meta", {})
        meta.pop("missing_since", None)

        for f in STATIC_FIELDS:
            if f in row and not same_value(rec.get(f), row[f]):
                diffs_today[(isin, f)] = (rec.get(f), row[f])
        for f in SEMI_STATIC_FIELDS:
            if f in row:
                rec[f] = row[f]

    today_set = set(today_isins)
    for isin, rec in master.items():
        if isin not in today_set:
            rec.setdefault("_meta", {}).setdefault("missing_since", today_s)

    log_message("MASTER", f"Titoli nel CSV: {len(today_isins)} | nuovi: {len(new_isins)} | "
                          f"differenze anagrafiche: {len(diffs_today)}")

    # --- Enrichment (bondid) ----------------------------------------------
    enrich(master, [i for i in today_isins if needs_enrichment(master[i], today)], today)

    # --- Differences report ------------------------------------------------
    old_diffs = {(d["isin"], d["field"]): d for d in load_json(DIFFS_PATH, default=[]) or []}
    diffs_out = []
    for (isin, field), (master_val, source_val) in sorted(diffs_today.items()):
        prev = old_diffs.get((isin, field))
        first = prev["first_detected"] if prev and same_value(prev.get("source"), source_val) else today_s
        entry = {"isin": isin, "field": field, "master": master_val, "source": source_val,
                 "first_detected": first, "last_detected": today_s}
        if field in overrides.get(isin, {}):
            entry["override"] = overrides[isin][field]
        diffs_out.append(entry)

    # --- Build output for the app ------------------------------------------
    master_fields = set(STATIC_FIELDS + SEMI_STATIC_FIELDS)
    output, applied_overrides = [], 0
    for row in rows:
        isin = row.get(isin_column)
        if not isin:
            continue
        ovr = overrides.get(isin, {})
        if ovr.get("_exclude"):
            continue
        rec = master[isin]
        out = {}
        for col in csv_columns:
            out[col] = rec.get(col) if col in master_fields else row.get(col)
        out["issueprice"] = row.get("issueprice")
        out["bondid"] = rec.get("bondid")
        for field, value in ovr.items():
            if str(field).startswith("_"):
                continue
            if field not in out:
                log_message("OVR", f"{isin}: il campo '{field}' non esiste nell'output, lo aggiungo", "WARNING")
            out[field] = value
            applied_overrides += 1
        output.append(out)

    for isin in overrides:
        if isin not in master:
            log_message("OVR", f"Override per {isin}: ISIN sconosciuto (refuso?)", "WARNING")
        elif isin not in today_set:
            log_message("OVR", f"Override per {isin}: titolo non presente nel CSV di oggi")

    # --- Save ---------------------------------------------------------------
    save_json(MASTER_PATH, dict(sorted(master.items())))
    save_json(DIFFS_PATH, diffs_out)
    save_json(OUTPUT_PATH, output)
    log_message("SAVE", f"Output: {len(output)} titoli, {applied_overrides} override applicati", "SUCCESS")

    write_summary(new_isins, diffs_out, master)

    print("=" * 80)
    print("PROCESSING COMPLETED SUCCESSFULLY!")
    print("=" * 80)


def write_summary(new_isins, diffs, master):
    """Summary displayed on the GitHub Actions run page."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    lines = ["## Aggiornamento obbligazioni", ""]
    lines.append(f"**Nuovi titoli:** {len(new_isins)}")
    if new_isins:
        lines += ["", "| ISIN | Descrizione | Lotto minimo |", "|---|---|---|"]
        for isin in new_isins:
            r = master[isin]
            lines.append(f"| {isin} | {r.get('description')} | {r.get('minimumlot')} |")
    lines += ["", f"**Differenze anagrafiche aperte (fonte ≠ master):** {len(diffs)}"]
    if diffs:
        lines += ["", "| ISIN | Campo | Master | Fonte | Override | Dal |", "|---|---|---|---|---|---|"]
        for d in diffs[:100]:
            lines.append(f"| {d['isin']} | {d['field']} | {d['master']} | {d['source']} | "
                         f"{d.get('override', '')} | {d['first_detected']} |")
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
