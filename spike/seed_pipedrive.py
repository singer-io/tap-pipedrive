"""
Pipedrive Test Data Seeder
==========================
Seeds a Pipedrive account with enough data to exercise:

  - PAGINATION   : >100 records in deals, persons, organizations, activities,
                   notes so every page-based and cursor-based paginator fires.
  - REPLICATION  : Two waves of records separated by a deliberate pause so
                   a bookmark set after Wave 1 correctly picks up only Wave 2
                   during an incremental sync.

Usage:
    # Full seed (Wave 1 + pause + Wave 2)
    python spike/seed_pipedrive.py --config config.json

    # Wave 1 only (then manually set bookmark, run tap, then run Wave 2)
    python spike/seed_pipedrive.py --config config.json --wave 1

    # Wave 2 only (after tap has consumed Wave 1 and saved a state)
    python spike/seed_pipedrive.py --config config.json --wave 2

Streams seeded:
    pipelines, stages, organizations, persons, deals, deal_fields,
    activity_types, activities, notes, products, deal_products,
    filters, files, dealflow (stage-change events)

Streams NOT seeded via script (see seed_pipedrive.md):
    currencies, users
"""

import argparse
import json
import os
import tempfile
import time

import requests

BASE_V1 = "https://api.pipedrive.com/v1"
BASE_V2 = "https://api.pipedrive.com/api/v2"

# ---------------------------------------------------------------------------
# Volume constants
# ---------------------------------------------------------------------------
# Pipedrive default page size is 100. We create slightly more than that so
# every paginated stream must fetch at least 2 pages.
PAGINATION_TARGET = 110   # records per paginated stream per wave
DEALFLOW_MOVES    = 25    # stage-change events per wave


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(session, url, params=None):
    resp = session.get(url, params=params or {})
    resp.raise_for_status()
    return resp.json()


def _post(session, url, payload=None, files=None):
    if files:
        resp = session.post(url, data=payload, files=files)
    else:
        resp = session.post(url, json=payload)
    if not resp.ok:
        log(f"  ERROR {resp.status_code} → {resp.text[:300]}")
    resp.raise_for_status()
    return resp.json()


def _put(session, url, payload):
    resp = session.put(url, json=payload)
    resp.raise_for_status()
    return resp.json()


def _existing_by_name(session, url, name_field="name"):
    """Return id of the first item whose name matches, or None."""
    page = _get(session, url, params={"limit": 500})
    for item in (page.get("data") or []):
        if item.get(name_field) == name_field:
            return item["id"]
    return None


def find_by_name(items, name, field="name"):
    for item in (items or []):
        if item.get(field) == name:
            return item
    return None


def log(msg):
    print(f"[seed] {msg}", flush=True)


def _throttle():
    """Brief sleep to stay well within Pipedrive rate limits."""
    time.sleep(0.12)


def _get_all_pages(session, url, params=None):
    """Fetch all records from a V1 paginated endpoint."""
    params = dict(params or {})
    params.setdefault("limit", 500)
    params["start"] = 0
    results = []
    while True:
        page = _get(session, url, params)
        data = page.get("data") or []
        results.extend(data)
        pagination = (page.get("additional_data") or {}).get("pagination", {})
        if not pagination.get("more_items_in_collection"):
            break
        params["start"] = pagination["next_start"]
    return results


# ---------------------------------------------------------------------------
# Pipelines  (stream: pipelines)
# ---------------------------------------------------------------------------

def seed_pipelines(session):
    log("Seeding pipelines ...")
    existing = _get_all_pages(session, f"{BASE_V1}/pipelines")

    result = []
    for name in ["Stitch Pipeline Alpha", "Stitch Pipeline Beta"]:
        p = find_by_name(existing, name)
        if p:
            log(f"  Pipeline '{name}' already exists (id={p['id']})")
            result.append(p)
        else:
            resp = _post(session, f"{BASE_V1}/pipelines", {"name": name, "active": True})
            p = resp["data"]
            log(f"  Created pipeline '{name}' id={p['id']}")
            result.append(p)
            _throttle()
    return result  # list of pipeline dicts


# ---------------------------------------------------------------------------
# Stages  (stream: stages)
# 4 stages per pipeline → 8 total; tap must paginate if limit < 8.
# ---------------------------------------------------------------------------

STAGE_NAMES = ["Prospect", "Qualified", "Proposal", "Closed Won"]

def seed_stages(session, pipelines):
    log("Seeding stages ...")
    all_stage_ids = []
    for pipeline in pipelines:
        pid = pipeline["id"]
        existing = _get_all_pages(session, f"{BASE_V1}/stages", {"pipeline_id": pid})
        for name in STAGE_NAMES:
            s = find_by_name(existing, name)
            if s:
                log(f"  Stage '{name}' (pipeline {pid}) already exists (id={s['id']})")
                all_stage_ids.append(s["id"])
            else:
                resp = _post(session, f"{BASE_V1}/stages", {"name": name, "pipeline_id": pid})
                sid = resp["data"]["id"]
                log(f"  Created stage '{name}' id={sid}")
                all_stage_ids.append(sid)
                _throttle()
    return all_stage_ids


# ---------------------------------------------------------------------------
# Organizations  (stream: organizations)
# Pagination target: PAGINATION_TARGET records per wave.
# ---------------------------------------------------------------------------

CITIES = ["San Francisco", "New York", "Chicago", "Austin", "Seattle",
          "Boston", "Denver", "Atlanta", "Miami", "Portland"]

def seed_organizations(session, wave, per_wave=None):
    per_wave = per_wave or PAGINATION_TARGET
    log(f"Seeding organizations (wave {wave}, {per_wave} records) ...")
    existing = _get_all_pages(session, f"{BASE_V1}/organizations")
    existing_names = {o["name"] for o in existing}

    created = 0
    for i in range(per_wave):
        name = f"Stitch Org W{wave}-{i+1:04d}"
        if name in existing_names:
            continue
        city = CITIES[i % len(CITIES)]
        resp = _post(session, f"{BASE_V1}/organizations", {
            "name": name,
            "address": f"{i+1} Test Ave, {city}, CA 9{40000 + i}"
        })
        created += 1
        if created % 20 == 0:
            log(f"    ... {created}/{per_wave} organizations created")
        _throttle()

    log(f"  Created {created} organizations in wave {wave}")
    return _get_all_pages(session, f"{BASE_V1}/organizations")


# ---------------------------------------------------------------------------
# Persons  (stream: persons)
# Pagination target: PAGINATION_TARGET records per wave.
# ---------------------------------------------------------------------------

def seed_persons(session, org_list, wave, per_wave=None):
    per_wave = per_wave or PAGINATION_TARGET
    log(f"Seeding persons (wave {wave}, {per_wave} records) ...")
    existing = _get_all_pages(session, f"{BASE_V1}/persons")
    existing_names = {p["name"] for p in existing}

    org_ids = [o["id"] for o in org_list]
    created = 0
    for i in range(per_wave):
        name = f"Stitch Person W{wave}-{i+1:04d}"
        if name in existing_names:
            continue
        resp = _post(session, f"{BASE_V1}/persons", {
            "name": name,
            "email": f"stitch.person.w{wave}.{i+1:04d}@stitchtest.invalid",
            "phone": f"+1-800-{wave:01d}{i:06d}",
            "org_id": org_ids[i % len(org_ids)]
        })
        created += 1
        if created % 20 == 0:
            log(f"    ... {created}/{per_wave} persons created")
        _throttle()

    log(f"  Created {created} persons in wave {wave}")
    return _get_all_pages(session, f"{BASE_V1}/persons")


# ---------------------------------------------------------------------------
# Products  (stream: products)
# ---------------------------------------------------------------------------

def seed_products(session):
    log("Seeding products ...")
    existing = _get_all_pages(session, f"{BASE_V1}/products")

    products_data = [
        {"name": "Stitch Data Connector",     "code": "SDC-001", "unit": "license",
         "prices": [{"price": 299.00, "currency": "USD"}]},
        {"name": "Stitch Professional Plan",  "code": "SPP-002", "unit": "month",
         "prices": [{"price": 999.00, "currency": "USD"}]},
        {"name": "Stitch Enterprise Edition", "code": "SEE-003", "unit": "year",
         "prices": [{"price": 9999.00, "currency": "USD"}]},
    ]

    result = []
    for p in products_data:
        ex = find_by_name(existing, p["name"])
        if ex:
            log(f"  Product '{p['name']}' already exists (id={ex['id']})")
            result.append(ex)
        else:
            resp = _post(session, f"{BASE_V1}/products", p)
            prod = resp["data"]
            log(f"  Created product '{p['name']}' id={prod['id']}")
            result.append(prod)
            _throttle()
    return result


# ---------------------------------------------------------------------------
# Deals  (stream: deals)
# Pagination target: PAGINATION_TARGET records per wave. Mix of statuses so
# both open and won/lost deals appear in every replication key window.
# ---------------------------------------------------------------------------

DEAL_STATUSES_CYCLE = ["open", "open", "open", "won", "open", "open", "open", "lost"]

def seed_deals(session, pipeline_ids, stage_ids, person_ids, org_ids, wave, per_wave=None):
    per_wave = per_wave or PAGINATION_TARGET
    log(f"Seeding deals (wave {wave}, {per_wave} records) ...")
    existing = _get_all_pages(session, f"{BASE_V1}/deals",
                              {"status": "all_not_deleted", "limit": 500})
    existing_titles = {d["title"] for d in existing}

    values = [1500, 3200, 7800, 14000, 28500, 52000, 99000, 150000, 4500, 8900]
    created = 0

    for i in range(per_wave):
        title = f"Stitch Deal W{wave}-{i+1:04d}"
        if title in existing_titles:
            continue

        status = DEAL_STATUSES_CYCLE[i % len(DEAL_STATUSES_CYCLE)]
        stage_idx = i % len(stage_ids)
        pipeline_idx = stage_idx // len(STAGE_NAMES)  # match stage to its pipeline

        resp = _post(session, f"{BASE_V1}/deals", {
            "title":       title,
            "status":      status,
            "value":       values[i % len(values)],
            "currency":    "USD",
            "pipeline_id": pipeline_ids[pipeline_idx % len(pipeline_ids)],
            "stage_id":    stage_ids[stage_idx],
            "person_id":   person_ids[i % len(person_ids)],
            "org_id":      org_ids[i % len(org_ids)],
        })
        created += 1
        if created % 20 == 0:
            log(f"    ... {created}/{per_wave} deals created")
        _throttle()

    log(f"  Created {created} deals in wave {wave}")
    return _get_all_pages(session, f"{BASE_V1}/deals",
                          {"status": "all_not_deleted", "limit": 500})


# ---------------------------------------------------------------------------
# Dealflow  (stream: dealflow)
# Move DEALFLOW_MOVES open deals to a different stage → creates dealChange
# events with field_key='stage_id' captured by the dealflow stream.
# ---------------------------------------------------------------------------

def seed_dealflow(session, deals, stage_ids, wave):
    log(f"Seeding dealflow stage-change events (wave {wave}) ...")
    open_deals = [d for d in deals if d.get("status") == "open"]
    moved = 0
    for i, deal in enumerate(open_deals[:DEALFLOW_MOVES]):
        current_stage = deal.get("stage_id")
        if current_stage in stage_ids:
            next_stage = stage_ids[(stage_ids.index(current_stage) + 1) % len(stage_ids)]
        else:
            next_stage = stage_ids[0]
        _put(session, f"{BASE_V1}/deals/{deal['id']}", {"stage_id": next_stage})
        moved += 1
        if moved % 10 == 0:
            log(f"    ... {moved}/{DEALFLOW_MOVES} deals moved")
        _throttle()
    log(f"  Created {moved} dealflow events in wave {wave}")


# ---------------------------------------------------------------------------
# Deal Products  (stream: deal_products)
# Pagination target: PAGINATION_TARGET attachments per wave.
# ---------------------------------------------------------------------------

def seed_deal_products(session, deals, products, wave):
    log(f"Seeding deal_products (wave {wave}) ...")
    attached = 0
    for i in range(PAGINATION_TARGET):
        deal    = deals[i % len(deals)]
        product = products[i % len(products)]
        existing = (_get(session, f"{BASE_V1}/deals/{deal['id']}/products").get("data") or [])
        if any(p.get("product_id") == product["id"] for p in existing):
            continue
        _post(session, f"{BASE_V1}/deals/{deal['id']}/products", {
            "product_id": product["id"],
            "item_price":  product.get("prices", [{}])[0].get("price", 100),
            "quantity":    (i % 5) + 1,
            "currency":    "USD"
        })
        attached += 1
        if attached % 20 == 0:
            log(f"    ... {attached} deal_products attached")
        _throttle()
    log(f"  Attached {attached} deal_products in wave {wave}")


# ---------------------------------------------------------------------------
# Activity Types  (stream: activity_types)
# ---------------------------------------------------------------------------

def seed_activity_types(session):
    log("Seeding activity_types ...")
    existing = (_get(session, f"{BASE_V1}/activityTypes").get("data") or [])

    types = [
        ("Stitch Demo Call",       "call"),
        ("Stitch Follow-up Email", "email"),
        ("Stitch On-site Visit",   "meeting"),
        ("Stitch Webinar",         "lunch"),
    ]
    for name, icon_key in types:
        ex = find_by_name(existing, name)
        if ex:
            log(f"  Activity type '{name}' already exists (id={ex['id']})")
        else:
            resp = _post(session, f"{BASE_V1}/activityTypes", {"name": name, "icon_key": icon_key})
            log(f"  Created activity type '{name}' id={resp['data']['id']}")
            _throttle()


# ---------------------------------------------------------------------------
# Activities  (stream: activities)
# Pagination target: PAGINATION_TARGET records per wave.
# ---------------------------------------------------------------------------

ACTIVITY_TYPES_ROTATE = ["call", "email", "meeting", "lunch", "task"]
DUE_DATES = ["2026-06-01", "2026-06-15", "2026-07-01", "2026-07-15", "2026-08-01"]

def seed_activities(session, deal_ids, person_ids, wave, per_wave=None):
    per_wave = per_wave or PAGINATION_TARGET
    log(f"Seeding activities (wave {wave}, {per_wave} records) ...")
    existing = _get_all_pages(session, f"{BASE_V1}/activities")
    existing_subjects = {a.get("subject", "") for a in existing}

    created = 0
    for i in range(per_wave):
        subject = f"Stitch Activity W{wave}-{i+1:04d}"
        if subject in existing_subjects:
            continue
        _post(session, f"{BASE_V1}/activities", {
            "subject":   subject,
            "type":      ACTIVITY_TYPES_ROTATE[i % len(ACTIVITY_TYPES_ROTATE)],
            "deal_id":   deal_ids[i % len(deal_ids)],
            "person_id": person_ids[i % len(person_ids)],
            "due_date":  DUE_DATES[i % len(DUE_DATES)],
            "due_time":  f"{8 + (i % 10):02d}:00",
            "duration":  "00:30",
            "done":      1 if i % 5 == 0 else 0,
            "note":      f"Stitch auto-seeded activity wave={wave} seq={i+1}"
        })
        created += 1
        if created % 20 == 0:
            log(f"    ... {created}/{per_wave} activities created")
        _throttle()
    log(f"  Created {created} activities in wave {wave}")


# ---------------------------------------------------------------------------
# Notes  (stream: notes)
# Pagination target: PAGINATION_TARGET records per wave.
# ---------------------------------------------------------------------------

def seed_notes(session, deal_ids, person_ids, org_ids, wave, per_wave=None):
    per_wave = per_wave or PAGINATION_TARGET
    log(f"Seeding notes (wave {wave}, {per_wave} records) ...")
    existing = _get_all_pages(session, f"{BASE_V1}/notes")
    existing_contents = {n.get("content", "") for n in existing}

    created = 0
    for i in range(per_wave):
        content = f"Stitch seed note wave={wave} seq={i+1:04d}: auto-generated for integration testing."
        if content in existing_contents:
            continue
        payload = {"content": content}
        r = i % 3
        if r == 0:
            payload["deal_id"]   = deal_ids[i % len(deal_ids)]
        elif r == 1:
            payload["person_id"] = person_ids[i % len(person_ids)]
        else:
            payload["org_id"]    = org_ids[i % len(org_ids)]
        _post(session, f"{BASE_V1}/notes", payload)
        created += 1
        if created % 20 == 0:
            log(f"    ... {created}/{per_wave} notes created")
        _throttle()
    log(f"  Created {created} notes in wave {wave}")


# ---------------------------------------------------------------------------
# Filters  (stream: filters)
# ---------------------------------------------------------------------------

def seed_filters(session):
    log("Seeding filters ...")
    existing = (_get(session, f"{BASE_V1}/filters", params={"type": "deals"}).get("data") or [])

    filter_name = "Stitch — Open Deals Filter"
    ex = find_by_name(existing, filter_name)
    if ex:
        log(f"  Filter '{filter_name}' already exists (id={ex['id']})")
        return

    # Fetch the numeric ID for the 'status' deal field
    deal_fields = (_get(session, f"{BASE_V1}/dealFields", params={"limit": 200}).get("data") or [])
    status_field = next((f for f in deal_fields if f.get("key") == "status"), None)
    if not status_field:
        log("  Could not find 'status' deal field — skipping filter creation.")
        return

    resp = _post(session, f"{BASE_V1}/filters", {
        "name": filter_name,
        "type": "deals",
        "conditions": {
            "glue": "and",
            "conditions": [
                {
                    "glue": "and",
                    "conditions": [
                        {
                            "object": "deal",
                            "field_id": status_field["id"],
                            "operator": "=",
                            "value": "open",
                            "extra_value": None
                        }
                    ]
                }
            ]
        }
    })
    log(f"  Created filter id={resp['data']['id']}")


# ---------------------------------------------------------------------------
# Deal Fields  (stream: deal_fields)
# ---------------------------------------------------------------------------

def seed_deal_fields(session):
    log("Seeding deal_fields (custom) ...")
    existing = _get_all_pages(session, f"{BASE_V1}/dealFields")

    fields = [
        {"name": "Stitch Contract Value",   "field_type": "double"},
        {"name": "Stitch Integration Type", "field_type": "enum",
         "options": [{"label": "API"}, {"label": "File"}, {"label": "Database"}]},
        {"name": "Stitch Go-Live Date",     "field_type": "date"},
        {"name": "Stitch Internal Notes",   "field_type": "text"},
    ]

    for f in fields:
        ex = find_by_name(existing, f["name"])
        if ex:
            log(f"  Deal field '{f['name']}' already exists (id={ex['id']})")
        else:
            resp = _post(session, f"{BASE_V1}/dealFields", f)
            log(f"  Created deal field '{f['name']}' id={resp['data']['id']}")
            _throttle()


# ---------------------------------------------------------------------------
# Files  (stream: files)
# Upload per_wave small attachments spread across deals.
# ---------------------------------------------------------------------------

def seed_files(session, deal_ids, wave, per_wave=10):
    log(f"Seeding files (wave {wave}, {per_wave} files) ...")
    existing = _get_all_pages(session, f"{BASE_V1}/files")
    existing_names = {f.get("name", "") for f in existing}
    content = b"Stitch integration test attachment - safe to delete.\n"
    created = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(per_wave):
            filename = f"stitch_seed_w{wave}_{i+1:04d}.txt"
            if filename in existing_names:
                log(f"  File '{filename}' already exists")
                continue
            fpath = os.path.join(tmpdir, filename)
            with open(fpath, "wb") as fh:
                fh.write(content + f"file={i+1} wave={wave}\n".encode())
            with open(fpath, "rb") as fh:
                _post(
                    session,
                    f"{BASE_V1}/files",
                    payload={"deal_id": deal_ids[i % len(deal_ids)]},
                    files={"file": (filename, fh, "text/plain")},
                )
            created += 1
            _throttle()
    log(f"  Uploaded {created} files in wave {wave}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_session(api_token):
    session = requests.Session()
    session.params = {"api_token": api_token}
    session.headers.update({"Accept": "application/json"})
    return session


def main():
    parser = argparse.ArgumentParser(
        description="Seed Pipedrive test account with enough data for pagination and replication tests."
    )
    parser.add_argument("--api-token", default="", help="Pipedrive API token")
    parser.add_argument("--config", help="Path to tap config.json (reads api_token from it)")
    parser.add_argument(
        "--wave",
        type=int,
        choices=[0, 1, 2],
        default=0,
        help="0=both waves with pause (default), 1=wave 1 only, 2=wave 2 only",
    )
    args = parser.parse_args()

    api_token = args.api_token
    if args.config:
        with open(args.config) as f:
            api_token = json.load(f).get("api_token", api_token)

    if not api_token:
        parser.error("Provide --api-token or --config with an api_token field.")

    session = build_session(api_token)

    log("=" * 60)
    log("Pipedrive Test Data Seeder")
    log(f"Pagination target : {PAGINATION_TARGET} records per stream per wave")
    log(f"Dealflow events   : {DEALFLOW_MOVES} per wave")
    log(f"Wave mode         : {args.wave}  (0=both, 1=wave1 only, 2=wave2 only)")
    log("=" * 60)

    # ── Shared infrastructure (idempotent regardless of wave) ─────────────────
    pipelines  = seed_pipelines(session)          # returns list of pipeline dicts
    pipeline_ids = [p["id"] for p in pipelines]
    stage_ids  = seed_stages(session, pipelines)  # all stage ids across pipelines
    products   = seed_products(session)
    seed_activity_types(session)
    seed_deal_fields(session)

    # ── Wave 1 ────────────────────────────────────────────────────────────────
    if args.wave in (0, 1):
        log("\n── WAVE 1 ──────────────────────────────────────────────────")

        orgs_w1    = seed_organizations(session, wave=1, per_wave=PAGINATION_TARGET)
        persons_w1 = seed_persons(session, orgs_w1, wave=1, per_wave=PAGINATION_TARGET)
        org_ids_w1    = [o["id"] for o in orgs_w1]
        person_ids_w1 = [p["id"] for p in persons_w1]

        deals_w1  = seed_deals(
            session, pipeline_ids, stage_ids,
            person_ids_w1, org_ids_w1,
            wave=1, per_wave=PAGINATION_TARGET
        )
        deal_ids_w1 = [d["id"] for d in deals_w1]

        seed_dealflow(session, deals_w1, stage_ids, wave=1)
        seed_deal_products(session, deals_w1, products, wave=1)
        seed_activities(session, deal_ids_w1, person_ids_w1, wave=1, per_wave=PAGINATION_TARGET)
        seed_notes(session, deal_ids_w1, person_ids_w1, org_ids_w1, wave=1, per_wave=PAGINATION_TARGET)
        seed_filters(session)
        seed_files(session, deal_ids_w1, wave=1, per_wave=10)

    # ── Replication pause ─────────────────────────────────────────────────────
    # After Wave 1, run the tap to get a full sync and save state.
    # The bookmark will be set to "now". Wave 2 records arrive after this pause
    # and should be the ONLY records emitted during the next incremental sync.
    if args.wave == 0:
        PAUSE = 15
        log(f"\n── REPLICATION PAUSE ({PAUSE}s) ─────────────────────────────────")
        log("   NOW is a good time to:")
        log("     1. Run the tap to perform a full sync (saves state bookmark)")
        log("     2. Note the bookmark timestamp printed in the state output")
        log("   Wave 2 records will be created after this pause. An incremental")
        log("   sync from the saved bookmark should return only Wave 2 records.")
        for remaining in range(PAUSE, 0, -1):
            print(f"\r   Waiting {remaining:2d}s ...", end="", flush=True)
            time.sleep(1)
        print()

    # ── Wave 2 ────────────────────────────────────────────────────────────────
    if args.wave in (0, 2):
        W2 = PAGINATION_TARGET // 2  # half volume — enough to verify incremental pickup
        log(f"\n── WAVE 2 (volume={W2} per stream) ────────────────────────────")

        orgs_all    = seed_organizations(session, wave=2, per_wave=W2)
        persons_all = seed_persons(session, orgs_all, wave=2, per_wave=W2)
        org_ids_all    = [o["id"] for o in orgs_all]
        person_ids_all = [p["id"] for p in persons_all]

        deals_all = seed_deals(
            session, pipeline_ids, stage_ids,
            person_ids_all, org_ids_all,
            wave=2, per_wave=W2
        )
        deal_ids_all = [d["id"] for d in deals_all]

        seed_dealflow(session, deals_all, stage_ids, wave=2)
        seed_deal_products(session, deals_all, products, wave=2)
        seed_activities(session, deal_ids_all, person_ids_all, wave=2, per_wave=W2)
        seed_notes(session, deal_ids_all, person_ids_all, org_ids_all, wave=2, per_wave=W2)
        seed_files(session, deal_ids_all, wave=2, per_wave=5)

    log("\n" + "=" * 60)
    log("Seeding complete.")
    log("")
    log("Approximate record counts (wave1 / wave2):")
    log(f"  deals, organizations, persons  : {PAGINATION_TARGET} / {PAGINATION_TARGET // 2}  (pagination fires at >100)")
    log(f"  activities, notes              : {PAGINATION_TARGET} / {PAGINATION_TARGET // 2}")
    log(f"  deal_products                  : {PAGINATION_TARGET} / {PAGINATION_TARGET // 2}")
    log(f"  dealflow events                : {DEALFLOW_MOVES} / {DEALFLOW_MOVES}")
    log("")
    log("Next steps:")
    log("  1. Full sync:      tap-pipedrive --config config.json --catalog catalog.json")
    log("  2. Save state, then run --wave 2 to populate incremental-only records")
    log("  3. Incremental:    tap-pipedrive --config config.json --catalog catalog.json --state state.json")
    log("=" * 60)


if __name__ == "__main__":
    main()
