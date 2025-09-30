# jobs.py

# app.py
from flask import Flask
app = Flask(__name__, static_folder='static', static_url_path='/static')

import os
from datetime import date, timedelta
import argparse
from datetime import date, timedelta
import json

from nbp import create_app
from nbp.models import db, Entity
from nbp.services.stats import recompute_all_florida

def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def _upsert_entities(rows, dry_run: bool) -> int:
    """Upsert a list of entity dicts. Returns number of inserts."""
    inserted = 0
    if dry_run:
        return inserted

    def _officers_to_json(v):
        if v is None:
            return None
        if isinstance(v, (list, dict)):
            return json.dumps(v, ensure_ascii=False)
        return str(v)

    batch_size = int(os.getenv("NBP_FLUSH_EVERY", "300"))
    counter = 0

    for rec in rows:
        rec["name"] = (rec.get("name") or "")[:255]
        if rec.get("entity_type"):
            rec["entity_type"] = rec["entity_type"][:50]
        if rec.get("last_event"):
            rec["last_event"] = rec["last_event"][:100]
        if rec.get("doc_number"):
            rec["doc_number"] = rec["doc_number"][:100]

        existing = Entity.query.filter_by(doc_number=rec["doc_number"]).first()

        if "officers_json" not in rec and "officers" in rec:
            rec["officers_json"] = _officers_to_json(rec["officers"])

        update_keys = (
            "name","entity_type","filing_date","city","county","state",
            "registered_agent","principal_address","mailing_address",
            "fei_ein","effective_date","last_event","event_date_filed",
            "event_effective_date","registered_agent_address","officers_json",
        )

        if existing:
            changed = False
            for k in update_keys:
                if k not in rec: 
                    continue
                v = rec.get(k)
                if v is not None and getattr(existing, k) != v:
                    setattr(existing, k, v)
                    changed = True
            if changed:
                db.session.add(existing)
        else:
            db.session.add(Entity(
                name=rec.get("name") or "",
                entity_type=rec.get("entity_type"),
                filing_date=rec.get("filing_date") or date.today(),
                city=rec.get("city") or None,
                county=rec.get("county") or None,
                state=rec.get("state") or "FL",
                registered_agent=rec.get("registered_agent") or None,
                principal_address=rec.get("principal_address") or None,
                mailing_address=rec.get("mailing_address") or None,
                fei_ein=rec.get("fei_ein") or None,
                effective_date=rec.get("effective_date") or None,
                last_event=rec.get("last_event") or None,
                event_date_filed=rec.get("event_date_filed") or None,
                event_effective_date=rec.get("event_effective_date") or None,
                registered_agent_address=rec.get("registered_agent_address") or None,
                officers_json=rec.get("officers_json") or _officers_to_json(rec.get("officers")),
                doc_number=(rec.get("doc_number") or "")[:100],
            ))
            inserted += 1

        counter += 1
        if counter % batch_size == 0:
            try:
                db.session.commit()
                print(f"[sunbiz] committed batch of {batch_size} (total {counter})")
            except Exception as e:
                db.session.rollback()
                print(f"[sunbiz] batch commit failed at {counter}: {e}")

    # final flush
    if counter % batch_size != 0:
        try:
            db.session.commit()
            print(f"[sunbiz] final commit of {counter}")
        except Exception as e:
            db.session.rollback()
            print(f"[sunbiz] final commit failed: {e}")

    return inserted

def run_all(bootstrap=False):
    """
    Run the Sunbiz ingestion + stats recompute.
    - bootstrap=True: scrape last 60 days (initial backfill)
    - bootstrap=False: scrape the last N days (default 1)
    """
    app = create_app()
    with app.app_context():
        use_browser = os.getenv("NBP_USE_BROWSER", "1") == "1"
        dry_run     = os.getenv("NBP_DRY_RUN", "0") == "1"

        if bootstrap:
            days_back = 60
            target_dates = {date.today() - timedelta(days=i) for i in range(days_back)}
        else:
            days_back = int(os.getenv("NBP_DAYS_BACK", "1"))
            target_dates = {date.today() - timedelta(days=i) for i in range(days_back)}

        print("[sunbiz] starting", {
            "bootstrap": bootstrap,
            "days_back": days_back,
            "dry_run": dry_run,
            "browser": use_browser
        })

        total_seen = 0
        total_inserted = 0

        if use_browser:
            from nbp.services.scrape_sunbiz_playwright import fetch_recent_by_name_prefixes_parallel

            prefixes_env = os.getenv("NBP_PREFIXES", "")
            if prefixes_env.strip():
                prefixes = [p.strip() for p in prefixes_env.split(",") if p.strip()]
            else:
                prefixes = list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

            concurrency = int(os.getenv("NBP_CONCURRENCY", "2"))  # be polite by default
            batch_size  = int(os.getenv("NBP_PREFIX_BATCH", "2"))  # scrape a couple of prefixes, then upsert

            window_days = (90 if bootstrap else int(os.getenv("NBP_WINDOW_DAYS", "90")))
            print(f"[sunbiz] crawl plan: prefixes={len(prefixes)} window_days={window_days} batch={batch_size} concurrency={concurrency}")

            for i in range(0, len(prefixes), batch_size):
                batch = prefixes[i:i+batch_size]
                try:
                    print(f"[sunbiz] fetching batch {i//batch_size+1}/{(len(prefixes)+batch_size-1)//batch_size}: {batch}")
                    batch_size = int(os.getenv("NBP_PREFIX_BATCH", "2"))
                    for pref_batch in _chunks(prefixes, batch_size):
                        print(f"[sunbiz] fetching batch {pref_batch}")
                        rows = fetch_recent_by_name_prefixes_parallel(
                            window_days=(90 if bootstrap else int(os.getenv("NBP_WINDOW_DAYS", "90"))),
                            prefixes=pref_batch,
                            concurrency=int(os.getenv("NBP_CONCURRENCY", "2")),
                        )
                        print(f"[sunbiz] parsed rows this batch: {len(rows)} from {pref_batch}")
                        inserted = _upsert_entities(rows, dry_run)
                        total_inserted += inserted
                        total_seen += len(rows)
                        print(f"[sunbiz] cumulative seen={total_seen} inserted={total_inserted}")
                except Exception as e:
                    print(f"[sunbiz] ERROR fetching batch {batch}: {e}")
                    rows = []

                print(f"[sunbiz] parsed rows this batch: {len(rows)}")
                total_seen += len(rows)
                inserted = _upsert_entities(rows, dry_run)
                total_inserted += inserted
                print(f"[sunbiz] cumulative seen={total_seen} inserted={total_inserted}")

        else:
            print("[sunbiz] requests mode not supported here")

        # Recompute rollups for SEO pages
        try:
            n = recompute_all_florida()
            print("[stats] recomputed jurisdictions:", n)
        except Exception as e:
            print("[stats] ERROR recomputing:", e)

        print("[sunbiz] done", {
            "seen": total_seen,
            "inserted": total_inserted,
            "dry_run": dry_run
        })


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", action="store_true", help="Scrape last 60 days")
    args = parser.parse_args()
    run_all(bootstrap=args.bootstrap)

