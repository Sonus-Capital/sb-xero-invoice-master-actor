import asyncio
import csv
import io
import json
import re
import urllib.request
from typing import Dict, List, Tuple

from apify import Actor


# ---------- helpers ----------

def norm(s):
    if s is None:
        return ""
    return str(s).strip()


def safe_float(s):
    s = norm(s)
    if not s:
        return 0.0
    # strip commas just in case
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0

from decimal import Decimal, InvalidOperation

def amount_key(value: str) -> str:
    """
    Normalise a currency string into integer cents for stable matching.
    e.g. '123.45' -> '12345', '123.4500' -> '12345'.
    Returns '' if we can't parse it.
    """
    s = norm(value)
    if not s:
        return ""
    try:
        dec = Decimal(s)
    except InvalidOperation:
        return ""
    # convert to cents with rounding
    cents = int((dec * 100).quantize(Decimal("1")))
    return str(cents)

INVOICE_CORE_RE = re.compile(r"[A-Za-z]*[-_ ]*(\d+)$")

def invoice_core(raw: str) -> str:
    """
    Normalise Xero invoice numbers like:
      'SB-2016-00123', 'ACCREC-1234', '2016-000123'
    down to a common 'core', e.g. '1234' or the trailing numeric part.
    Fallback is the cleaned alphanumeric string.
    """
    s = norm(raw)
    if not s:
        return ""

    m = INVOICE_CORE_RE.search(s)
    if m:
        return m.group(1)

    # fallback: drop spaces and punctuation, keep alphanumerics
    return re.sub(r"[^0-9A-Za-z]", "", s)

def normalize_newlines(text: str) -> str:
    # Make csv module happier with mixed line endings
    return text.replace("\r\n", "\n").replace("\r", "\n")


async def fetch_csv_rows(url: str, label: str) -> List[Dict]:
    """Download CSV from URL and return list of dict rows."""
    Actor.log.info(f"Downloading {label} CSV from {url}")
    try:
        with urllib.request.urlopen(url) as resp:
            raw = resp.read()
    except Exception as e:
        Actor.log.error(f"Failed to download {label} CSV from {url}: {e}")
        raise

    text = raw.decode("utf-8", errors="replace")
    text = normalize_newlines(text)
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(r) for r in reader]
    Actor.log.info(f"{label} rows: {len(rows)}")
    return rows


def build_ledger_key(row: Dict, idx: int) -> str:
    """
    Build a join key for ledger rows using:
      Date + Contact + Gross/Net AUD (normalised to cents).

    Ledger header snippet (as per your note):
      Date, Source, Contact, Contact Group, Description,
      Invoice Number, Reference, Currency,
      Debit (AUD), Credit (AUD), Gross (AUD), Net (AUD), GST (AUD), ...
    """
    date = norm(row.get("Date"))
    contact = norm(row.get("Contact"))
    # Prefer Gross (AUD), fall back to Net (AUD) if needed
    amt_str = row.get("Gross (AUD)") or row.get("Net (AUD)")
    amt_norm = amount_key(amt_str)

    if date and contact and amt_norm:
        return f"DCAMT::{date}::{contact}::{amt_norm}"

    # Slightly weaker fallback: date + contact only
    if date and contact:
        return f"DC::{date}::{contact}"

    # Last resort
    return f"LEDGER_ROW::{idx}"


def build_master_key(row: Dict, idx: int) -> str:
    """
    Build a join key for 2016_Master_Financials rows using:
      Date + Contact + Amount aud (normalised to cents).

    Master header snippet (your note):
      Date, Type, Year, Xero number, Invoice ID, Line item ID, Key,
      Contact, Description, Reference, Account code, Tracking horse,
      Currency, Line amount, Tax amount, Fx date, Fx rate,
      Amount aud, Gst aud, ...
    """
    date = norm(row.get("Date"))
    contact = norm(row.get("Contact"))
    # Prefer Amount aud, fall back to Line amount if needed
    amt_str = row.get("Amount aud") or row.get("Amount AUD") or row.get("Line amount")
    amt_norm = amount_key(amt_str)

    if date and contact and amt_norm:
        return f"DCAMT::{date}::{contact}::{amt_norm}"

    if date and contact:
        return f"DC::{date}::{contact}"

    return f"MASTER_ROW::{idx}"


def summarise_ledger_group(rows: List[Dict]) -> Dict:
    """Aggregate a group of ledger rows into a single summary dict."""
    if not rows:
        return {}

    first = rows[0]
    out: Dict[str, object] = {}

    def copy_field(src_key: str, dst_key: str):
        out[dst_key] = norm(first.get(src_key))

    copy_field("Date", "Ledger_Date")
    copy_field("Source", "Ledger_Source")
    copy_field("Contact", "Ledger_Contact")
    copy_field("Contact Group", "Ledger_Contact_Group")
    copy_field("Description", "Ledger_Description")
    copy_field("Invoice Number", "Ledger_Invoice_Number")
    copy_field("Reference", "Ledger_Reference")
    copy_field("Currency", "Ledger_Currency")
    copy_field("Account Code", "Ledger_Account_Code")
    copy_field("Account", "Ledger_Account")
    copy_field("Account Type", "Ledger_Account_Type")
    copy_field("Horse", "Ledger_Horse")
    copy_field("Related account", "Ledger_Related_Account")

    # sums
    gross_aud_sum = sum(safe_float(r.get("Gross (AUD)")) for r in rows)
    net_aud_sum = sum(safe_float(r.get("Net (AUD)")) for r in rows)
    gst_aud_sum = sum(safe_float(r.get("GST (AUD)")) for r in rows)

    out["Ledger_Gross_AUD_Sum"] = gross_aud_sum
    out["Ledger_Net_AUD_Sum"] = net_aud_sum
    out["Ledger_GST_AUD_Sum"] = gst_aud_sum
    out["Ledger_Row_Count"] = len(rows)

    return out


def summarise_master_group(rows: List[Dict]) -> Dict:
    """Aggregate a group of master financial rows into a single summary dict."""
    if not rows:
        return {}

    first = rows[0]
    out: Dict[str, object] = {}

    def copy_field(src_key: str, dst_key: str):
        out[dst_key] = norm(first.get(src_key))

    copy_field("Date", "Master_Date")
    copy_field("Type", "Master_Type")
    copy_field("Year", "Master_Year")
    copy_field("Xero number", "Master_Xero_Number")
    copy_field("Invoice ID", "Master_Invoice_ID")
    copy_field("Contact", "Master_Contact")
    copy_field("Description", "Master_Description")
    copy_field("Reference", "Master_Reference")
    copy_field("Account code", "Master_Account_Code")
    copy_field("Tracking horse", "Master_Tracking_Horse")
    copy_field("Currency", "Master_Currency")
    copy_field("Attributed to", "Master_Attributed_To")
    copy_field("Attribution method", "Master_Attribution_Method")
    copy_field("Horse", "Master_Horse")
    copy_field("Progeny", "Master_Progeny")
    copy_field("Category bucket", "Master_Category_Bucket")
    copy_field("Likely related", "Master_Likely_Related")
    copy_field("Xero link", "Master_Xero_Link")
    copy_field("Source", "Master_Source")
    copy_field("Untracked flag", "Master_Untracked_Flag")
    copy_field("Untracked reason", "Master_Untracked_Reason")
    copy_field("Reviewer decision", "Master_Reviewer_Decision")
    copy_field("Reviewer notes", "Master_Reviewer_Notes")
    copy_field("Doc ID", "Master_Doc_ID")
    copy_field("Has attachments", "Master_Has_Attachments")
    copy_field("Source Doc", "Master_Source_Doc")

    amount_aud_sum = sum(safe_float(r.get("Amount aud")) for r in rows)
    gst_aud_sum = sum(safe_float(r.get("Gst aud")) for r in rows)
    line_amt_sum = sum(safe_float(r.get("Line amount")) for r in rows)

    out["Master_Amount_AUD_Sum"] = amount_aud_sum
    out["Master_GST_AUD_Sum"] = gst_aud_sum
    out["Master_Line_Amount_Sum"] = line_amt_sum
    out["Master_Row_Count"] = len(rows)

    return out


def build_output_rows(
    year: str,
    ledger_rows: List[Dict],
    master_rows: List[Dict],
) -> Tuple[List[Dict], Dict]:
    """Merge groups and produce flattened invoice master rows + summary stats."""
    # group by key
    ledger_groups: Dict[str, List[Dict]] = {}
    for idx, r in enumerate(ledger_rows):
        k = build_ledger_key(r, idx)
        r["__invoice_key"] = k
        ledger_groups.setdefault(k, []).append(r)

    master_groups: Dict[str, List[Dict]] = {}
    for idx, r in enumerate(master_rows):
        k = build_master_key(r, idx)
        r["__invoice_key"] = k
        master_groups.setdefault(k, []).append(r)

    all_keys = set(ledger_groups.keys()) | set(master_groups.keys())
    keys_both = [k for k in all_keys if k in ledger_groups and k in master_groups]
    keys_ledger_only = [k for k in all_keys if k in ledger_groups and k not in master_groups]
    keys_master_only = [k for k in all_keys if k in master_groups and k not in ledger_groups]

    out_rows: List[Dict] = []

    # 1) Keys with both ledger + master
    for k in keys_both:
        lg = ledger_groups.get(k, [])
        mg = master_groups.get(k, [])

        ledger_summary = summarise_ledger_group(lg)
        master_summary = summarise_master_group(mg)

        # Emit one row per MASTER row (line-item detail preserved), ledger summarised
        for mrow in mg:
            row_out: Dict[str, object] = {}
            row_out["Year"] = year
            row_out["Invoice_Key"] = k
            row_out["Join_Status"] = "Both"

            # copy ledger summary
            row_out.update(ledger_summary)

            # copy master row fields prefixed
            for mk, mv in mrow.items():
                if mk.startswith("__"):
                    continue
                col_name = f"MasterRow_{mk.replace(' ', '_')}"
                row_out[col_name] = mv

            # also copy master summary (sums) if you want
            for sk, sv in master_summary.items():
                if sk not in row_out:
                    row_out[sk] = sv

            out_rows.append(row_out)

    # 2) Ledger-only keys
    for k in keys_ledger_only:
        lg = ledger_groups.get(k, [])
        ledger_summary = summarise_ledger_group(lg)

        row_out: Dict[str, object] = {}
        row_out["Year"] = year
        row_out["Invoice_Key"] = k
        row_out["Join_Status"] = "Ledger_Only"
        row_out.update(ledger_summary)
        out_rows.append(row_out)

    # 3) Master-only keys
    for k in keys_master_only:
        mg = master_groups.get(k, [])
        master_summary = summarise_master_group(mg)

        for mrow in mg:
            row_out: Dict[str, object] = {}
            row_out["Year"] = year
            row_out["Invoice_Key"] = k
            row_out["Join_Status"] = "Master_Only"

            # no ledger summary
            for mk, mv in mrow.items():
                if mk.startswith("__"):
                    continue
                col_name = f"MasterRow_{mk.replace(' ', '_')}"
                row_out[col_name] = mv

            for sk, sv in master_summary.items():
                if sk not in row_out:
                    row_out[sk] = sv

            out_rows.append(row_out)

    summary = {
        "keys_total": len(all_keys),
        "keys_both": len(keys_both),
        "keys_ledger_only": len(keys_ledger_only),
        "keys_master_only": len(keys_master_only),
        "ledger_rows": len(ledger_rows),
        "master_rows": len(master_rows),
        "output_rows": len(out_rows),
    }

    return out_rows, summary


def rows_to_csv(rows: List[Dict]) -> str:
    if not rows:
        return ""

    fieldnames_set = set()
    for r in rows:
        fieldnames_set.update(r.keys())

    base = ["Year", "Invoice_Key", "Join_Status"]
    ledger_cols = sorted([c for c in fieldnames_set if c.startswith("Ledger_")])
    master_cols = sorted([c for c in fieldnames_set if c.startswith("Master_") or c.startswith("MasterRow_")])
    other_cols = sorted(
        [c for c in fieldnames_set if c not in base and c not in ledger_cols and c not in master_cols]
    )

    fieldnames = base + ledger_cols + master_cols + other_cols

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return buf.getvalue()


# ---------- main ----------

async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        Actor.log.info(f"Actor input keys: {list(actor_input.keys())}")

        year = norm(actor_input.get("Year"))
        ledger_url = norm(actor_input.get("LedgerUrl"))
        master_url = norm(actor_input.get("MasterUrl"))

        if not year or not ledger_url or not master_url:
            Actor.log.error("Missing required input fields. Need Year, LedgerUrl, MasterUrl.")
            await Actor.push_data({
                "ok": False,
                "error": "Missing required input fields. Expected Year, LedgerUrl, MasterUrl.",
                "actor_input": actor_input,
            })
            return

        # Download CSVs
        try:
            ledger_rows = await fetch_csv_rows(ledger_url, "ledger")
        except Exception as e:
            await Actor.push_data({
                "ok": False,
                "error": f"Failed to download or parse ledger CSV: {e}",
                "which": "ledger",
                "ledger_url": ledger_url,
            })
            return

        try:
            master_rows = await fetch_csv_rows(master_url, "master")
        except Exception as e:
            await Actor.push_data({
                "ok": False,
                "error": f"Failed to download or parse master CSV: {e}",
                "which": "master",
                "master_url": master_url,
            })
            return

        if not ledger_rows and not master_rows:
            Actor.log.error("Both ledger and master CSVs are empty.")
            await Actor.push_data({
                "ok": False,
                "error": "Both ledger and master CSVs are empty.",
                "year": year,
            })
            return

        # Merge
        out_rows, summary = build_output_rows(year, ledger_rows, master_rows)

        Actor.log.info(
            f"Merge summary: keys_total={summary['keys_total']}, "
            f"both={summary['keys_both']}, ledger_only={summary['keys_ledger_only']}, "
            f"master_only={summary['keys_master_only']}, "
            f"output_rows={summary['output_rows']}"
        )

        csv_text = rows_to_csv(out_rows)
        filename = f"invoice_master_{year}.csv" if year else "invoice_master.csv"

        # Store CSV in default key-value store
        await Actor.set_value(
            filename,
            csv_text,
            content_type="text/csv; charset=utf-8",
        )

        # Also push a tiny summary item to dataset
        await Actor.push_data({
            "ok": True,
            "year": year,
            "csv_key": filename,
            **summary,
        })

        Actor.log.info(
            f"Done. Year={year}, rows={summary['output_rows']}, "
            f"keys={summary['keys_total']}, file={filename}"
        )


if __name__ == "__main__":
    asyncio.run(main())
