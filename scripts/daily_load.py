"""
Daily ingestion script: upload today's RETRAN_NODs CSV to GCS,
append to BigQuery raw table, then run dbt.
Run manually or triggered by Kestra.

Usage:
    python3 daily_load.py [YYYY-MM-DD]
    If date is omitted, uses today.

Every run ends with one timestamped "BQ LOAD OK/FAILED" line, which the NOD
morning digest (~/scripts/morning_digest.py) reads from nod_daily_load.log.
"""
import csv
import sys
import os
import subprocess
import tempfile
from datetime import date, datetime
from google.cloud import storage, bigquery

PROJECT_ID  = "aiagentsintensive"
BUCKET_NAME = "aiagentsintensive-nod-lake"
DATASET_ID  = "nod_raw"
TABLE_ID    = "nods"
NOD_DIR     = "/home/highview/Projects/NOD-RealEstate/retran"
REJECT_DIR  = f"{NOD_DIR}/bq_rejects"
GCS_PREFIX  = "nods"
DBT_DIR     = "/home/highview/Projects/NOD-RealEstate/SoCal-NOD-Tracker/dbt"


def status(msg):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} BQ LOAD {msg}", flush=True)


def nod_rows(local_path):
    """From 2026-10-19 the daily CSV has a trailing filing_kind column that tags
    the Lis Pendens / HOA delinquency rows retran mixes into its NOD feed. The
    BigQuery load is positional (autodetect=False), so keep NOD rows only and
    drop the column. Older files pass through unchanged.
    Returns (header, rows, dropped_count)."""
    with open(local_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    if "filing_kind" not in header:
        return header, rows, 0
    k = header.index("filing_kind")
    keep = [r[:k] + r[k + 1:] for r in rows
            if (r[k] if k < len(r) else "") in ("NOD", "")]
    return header[:k] + header[k + 1:], keep, len(rows) - len(keep)


def bad_reason(row, schema):
    """Why BigQuery would reject this row, or None. Checks the column count and
    that FLOAT/INTEGER columns parse; retran exports occasionally carry a stray
    quote that shifts every later field (e.g. 2026-08-13, doc 26-00582460)."""
    if len(row) != len(schema):
        return f"{len(row)} columns, table has {len(schema)}"
    for value, field in zip(row, schema):
        v = value.strip().replace(",", "")   # BigQuery reads '159,500' as 159500
        if not v:
            continue
        try:
            if field.field_type in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                float(v)
            elif field.field_type in ("INTEGER", "INT64"):
                int(v)
        except ValueError:
            return f"{field.name}={value[:40]!r} is not {field.field_type}"
    return None


def main():
    run_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    local_path = f"{NOD_DIR}/RETRAN_NODs_{run_date}.csv"

    if not os.path.exists(local_path):
        print(f"ERROR: {local_path} not found")
        status(f"FAILED date={run_date} reason=csv-not-found")
        sys.exit(1)

    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    schema = bq_client.get_table(table_ref).schema

    header, rows, dropped = nod_rows(local_path)
    if dropped:
        print(f"Dropped {dropped} non-NOD row(s) (Lis Pendens / HOA delinquency)")

    # Quarantine rows BigQuery would reject, so one bad row can't sink the day.
    good, rejects = [], []
    doc = header.index("document_number") if "document_number" in header else None
    for r in rows:
        reason = bad_reason(r, schema)
        if reason:
            rejects.append(r)
            print(f"Quarantined row (doc {r[doc] if doc is not None and doc < len(r) else '?'}): {reason}")
        else:
            good.append(r)
    reject_path = None
    if rejects:
        os.makedirs(REJECT_DIR, exist_ok=True)
        reject_path = f"{REJECT_DIR}/RETRAN_NODs_{run_date}_rejects.csv"
        with open(reject_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rejects)
        print(f"  → {len(rejects)} row(s) saved to {reject_path}")

    tmp = tempfile.NamedTemporaryFile("w", suffix=".csv", newline="", encoding="utf-8", delete=False)
    with tmp:
        w = csv.writer(tmp)
        w.writerow(header)
        w.writerows(good)

    # Step 1: Upload to GCS
    print(f"Uploading {local_path} to GCS...")
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob_name = f"{GCS_PREFIX}/RETRAN_NODs_{run_date}.csv"
    try:
        bucket.blob(blob_name).upload_from_filename(tmp.name)
    finally:
        os.unlink(tmp.name)
    gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    print(f"  → {gcs_uri}")

    # Step 2: Append to BigQuery
    print("Loading into BigQuery (append)...")
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
    )
    job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    try:
        job.result()
    except Exception as e:
        print(f"ERROR: BigQuery load failed: {e}")
        status(f"FAILED date={run_date} reason=load-job quarantined={len(rejects)}")
        sys.exit(1)
    print(f"  → Appended {job.output_rows} rows to {table_ref}")

    # Step 3: Run dbt
    print("Running dbt...")
    result = subprocess.run(["dbt", "run"], cwd=DBT_DIR, capture_output=True, text=True)
    print(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
    if result.returncode != 0:
        print("ERROR: dbt run failed")
        print(result.stderr[-1000:])
        status(f"FAILED date={run_date} reason=dbt appended={job.output_rows} quarantined={len(rejects)}")
        sys.exit(1)
    status(f"OK date={run_date} appended={job.output_rows} quarantined={len(rejects)}"
           + (f" rejects={reject_path}" if reject_path else ""))
    print("Done!")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException as e:
        status(f"FAILED date={sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()} "
               f"reason={type(e).__name__}")
        raise
