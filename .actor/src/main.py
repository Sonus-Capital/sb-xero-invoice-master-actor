import csv
import io
import json
import urllib.request
from collections import defaultdict

from apify import Actor
import asyncio


def norm(s):
    if s is None:
        return ""
    return str(s).strip()


def to_float(row, field):
    raw = norm(row.get(field))
    if not raw:
        return 0.0
    # Strip commas, handle plain numbers
    raw = raw.replace(",", "")
    try:
        return float(raw)
    except Exception:
        return 0.0


async def download_csv(url: str, label: str):
    if not url:
        raise ValueError(f"Missing URL for {label}")
    Actor.log.info(f"Downloading {label} CSV from {url}")
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    text = data.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(r) for r in reader]
    Actor.log.info(f"{label} rows: {len(rows)}")
    return rows


async def main():
    async with Actor:
        actor_input = await Actor.get_input() or {}

        year = norm(actor_input.get("Year"))
        ledger_url = norm(actor_input.get("LedgerUrl"))  # unused for now
        master_url = norm(actor_input.get("MasterUrl"))

        Actor.log.info(f"Actor input keys: {list(actor_input.keys())}")

        if not year or not master_url:
            Actor.log.error("Year or MasterUrl missing from input.")
            await Actor.set_value(
                "TX_MASTER_ERROR",
                {
                    "ok": False,
                    "error": "Year and MasterUrl are required.",
                    "input": actor_input,
                },
            )
            return

        # ------------------------------------------------------------------
        # 1) Download MASTER FINANCIALS ONLY (invoice / line-item world)
        # ------------------------------------------------------------------
        try:
            master_rows = await download_csv(master_url, "master")
        except Exception as e:
            msg = f"Failed to download or parse master CSV: {e}"
            Actor.log.error(msg)
            await Actor.set_value(
                "TX_MASTER_ERROR",
                {
                    "ok": False,
                    "error": msg,
                    "year": year,
                    "master_url": master_url,
                },
            )
            return

        # If there are no master rows, bail early
        if not master_rows:
            Actor.log.error("Master CSV contained 0 data rows.")
            await Actor.set_value(
                "TX_MASTER_ERROR",
                {
                    "ok": False,
                    "error": "Master CSV contained 0 data rows.",
                    "year": year,
                    "master_url": master_url,
                },
            )
            return

        # ------------------------------------------------------------------
        # 2) Build invoice-level summary from master rows only
        # ------------------------------------------------------------------

        # We’ll group primarily by Invoice ID, then Xero number, then Key
        invoices = {}

        def get_invoice_key(row, idx):
            inv_id = norm(row.get("Invoice ID"))
            xno = norm(row.get("Xero number"))
            key = norm(row.get("Key"))
            if inv_id:
                return inv_id
            if xno:
                return f"XNO::{xno}"
            if key:
                return f"KEY::{key}"
            return f"ROW::{idx}"

        for idx, row in enumerate(master_rows):
            inv_key = get_invoice_key(row, idx)

            if inv_key not in invoices:
                invoices[inv_key] = {
                    "Invoice_Key": inv_key,
                    "Year": norm(row.get("Year")),
                    "Type": norm(row.get("Type")),
                    "Xero number": norm(row.get("Xero number")),
                    "Invoice ID": norm(row.get("Invoice ID")),
                    "Contact": norm(row.get("Contact")),
                    "Reference": norm(row.get("Reference")),
                    "Currency": norm(row.get("Currency")),
                    "Invoice date": norm(row.get("Date")),  # earliest later
                    "Line_count": 0,
                    "Line_amount_total": 0.0,
                    "Tax_amount_total": 0.0,
                    "Amount_aud_total": 0.0,
                    "Gst_aud_total": 0.0,
                    # aggregations
                    "Horses": set(),
                    "Progeny": set(),
                    "Category_buckets": set(),
                    "Any_untracked": False,
                    "Any_untracked_reason": set(),
                    "Any_reviewer_decision": set(),
                    "Any_reviewer_notes": set(),
                    "Has_attachments_any": False,
                    "First_Xero_link": "",
                    "First_Source_Doc": "",
                }

            agg = invoices[inv_key]

            # Keep the earliest (lexicographic) non-empty date we see
            d = norm(row.get("Date"))
            if d:
                if not agg["Invoice date"]:
                    agg["Invoice date"] = d
                else:
                    # crude but stable: pick lexicographically smaller
                    if d < agg["Invoice date"]:
                        agg["Invoice date"] = d

            # Core identifiers – if missing in agg but present here, fill
            for field in ["Year", "Type", "Xero number", "Invoice ID", "Contact", "Reference", "Currency"]:
                if not norm(agg.get(field)) and norm(row.get(field)):
                    agg[field] = norm(row.get(field))

            # Quantities
            agg["Line_count"] += 1
            agg["Line_amount_total"] += to_float(row, "Line amount")
            agg["Tax_amount_total"] += to_float(row, "Tax amount")
            agg["Amount_aud_total"] += to_float(row, "Amount aud")
            agg["Gst_aud_total"] += to_float(row, "Gst aud")

            # Horses / Progeny / Categories
            h = norm(row.get("Horse") or row.get("Tracking horse"))
            if h:
                agg["Horses"].add(h)

            p = norm(row.get("Progeny"))
            if p:
                agg["Progeny"].add(p)

            cat = norm(row.get("Category bucket"))
            if cat:
                agg["Category_buckets"].add(cat)

            # Untracked / review info
            if norm(row.get("Untracked flag")):
                agg["Any_untracked"] = True

            ur = norm(row.get("Untracked reason"))
            if ur:
                agg["Any_untracked_reason"].add(ur)

            rd = norm(row.get("Reviewer decision"))
            if rd:
                agg["Any_reviewer_decision"].add(rd)

            rn = norm(row.get("Reviewer notes"))
            if rn:
                agg["Any_reviewer_notes"].add(rn)

            # Attachments / links
            if norm(row.get("Has attachments")):
                agg["Has_attachments_any"] = True

            if not agg["First_Xero_link"]:
                xl = norm(row.get("Xero link"))
                if xl:
                    agg["First_Xero_link"] = xl

            if not agg["First_Source_Doc"]:
                sd = norm(row.get("Source Doc"))
                if sd:
                    agg["First_Source_Doc"] = sd

        # ------------------------------------------------------------------
        # 3) Flatten aggregations and write CSV
        # ------------------------------------------------------------------

        # Fixed output column order
        fieldnames = [
            "Invoice_Key",
            "Year",
            "Type",
            "Xero number",
            "Invoice ID",
            "Contact",
            "Reference",
            "Currency",
            "Invoice date",
            "Line_count",
            "Line_amount_total",
            "Tax_amount_total",
            "Amount_aud_total",
            "Gst_aud_total",
            "Horses",
            "Progeny",
            "Category_buckets",
            "Any_untracked",
            "Any_untracked_reason",
            "Any_reviewer_decision",
            "Any_reviewer_notes",
            "Has_attachments_any",
            "First_Xero_link",
            "First_Source_Doc",
        ]

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for inv_key, agg in invoices.items():
            row_out = dict(agg)

            # turn sets into sorted semicolon-joined strings
            for set_field in [
                "Horses",
                "Progeny",
                "Category_buckets",
                "Any_untracked_reason",
                "Any_reviewer_decision",
                "Any_reviewer_notes",
            ]:
                val = agg.get(set_field)
                if isinstance(val, set):
                    row_out[set_field] = "; ".join(sorted(v for v in val if v))
                else:
                    row_out[set_field] = norm(val)

            # booleans -> Y/N
            row_out["Any_untracked"] = "Y" if agg.get("Any_untracked") else ""
            row_out["Has_attachments_any"] = "Y" if agg.get("Has_attachments_any") else ""

            writer.writerow(row_out)

        csv_text = buf.getvalue()
        filename = f"invoice_master_{year}.csv" if year else "invoice_master.csv"

        await Actor.set_value(
            filename,
            csv_text,
            content_type="text/csv; charset=utf-8",
        )

        summary = {
            "ok": True,
            "year": year,
            "invoice_rows": len(invoices),
            "line_rows": len(master_rows),
            "csv_key": filename,
        }

        Actor.log.info(
            f"Done. Year={year}, invoices={len(invoices)}, "
            f"lines={len(master_rows)}, file={filename}"
        )

        await Actor.set_value("TX_MASTER_SUMMARY", summary)


if __name__ == "__main__":
    asyncio.run(main())
