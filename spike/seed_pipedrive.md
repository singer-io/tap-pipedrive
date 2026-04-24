# Pipedrive Test Account — Data Seeding Guide

This document describes which tap streams can be seeded via the script
[`spike/seed_pipedrive.py`](seed_pipedrive.py), which cannot, and how to
add data manually through the Pipedrive web UI.

---

## Running the Script

```bash
# From the tap-pipedrive root directory
pip install requests
python spike/seed_pipedrive.py --api-token <YOUR_API_TOKEN>

# Or point at the existing config.json
python spike/seed_pipedrive.py --api-token dummy --config config.json
```

The script is **idempotent**: it checks for existing objects by name before
creating them, so re-running is safe.

---

## Streams Seeded via Script

| Stream | Pipedrive Resource | API Endpoint | Notes |
|---|---|---|---|
| `pipelines` | Pipeline | `POST /v1/pipelines` | Creates "Stitch Test Pipeline" |
| `stages` | Stage | `POST /v1/stages` | Creates 4 stages inside the test pipeline |
| `organizations` | Organization | `POST /v1/organizations` | Creates 3 test organizations |
| `persons` | Person | `POST /v1/persons` | Creates 3 persons linked to organizations |
| `products` | Product | `POST /v1/products` | Creates 2 products with USD pricing |
| `deals` | Deal | `POST /v1/deals` | Creates 4 deals (open + won) linked to persons/orgs/stages |
| `dealflow` | Deal Stage-Change Events | `PUT /v1/deals/{id}` (stage move) | Moves deals between stages to generate `dealChange` events captured by this stream |
| `deal_products` | Deal ↔ Product | `POST /v1/deals/{id}/products` | Attaches products to deals |
| `activity_types` | Activity Type | `POST /v1/activityTypes` | Creates 2 custom activity types |
| `activities` | Activity | `POST /v1/activities` | Creates 3 activities linked to deals and persons |
| `notes` | Note | `POST /v1/notes` | Creates 3 notes linked to deals, persons, and orgs |
| `filters` | Filter | `POST /v1/filters` | Creates 1 deal filter |
| `deal_fields` | Custom Deal Field | `POST /v1/dealFields` | Creates 2 custom fields (numeric + enum) |
| `files` | File | `POST /v1/files` (multipart) | Uploads a small text attachment linked to a deal |

**Total: 14 of 16 streams seeded via script.**

---

## Streams That Cannot Be Seeded via Script

### `currencies`

**Reason:** Pipedrive does not expose a `POST /currencies` endpoint.
The currencies list is system-managed. Only the account owner can enable
additional currencies.

**How to add via Web UI:**
1. Log in to Pipedrive.
2. Go to **Settings → Company Settings → Currencies**.
3. Click **Add currency** and select any additional currency (e.g. EUR, GBP).
4. Save. The tap will then pick up all enabled currencies on next sync.

---

### `users`

**Reason:** Pipedrive does not allow creating user accounts via the API.
Users must be invited through the web UI, and the API token holder must
have **Admin** role to see other users.

**How to add via Web UI:**
1. Log in to Pipedrive as an **Admin** user.
2. Go to **Settings → Manage Users → Users**.
3. Click **Add User**, enter an email address (can be a test email alias
   such as `stitch+test1@yourdomain.com`).
4. Set role to **Regular User** and click **Send Invitation**.
5. Once accepted, the user appears in the `users` stream on next sync.

> **Note:** The `users` stream uses the `/recents` endpoint with
> `items=user`. Even a single active user (the API token owner) will
> produce at least one record; you do not strictly need extra users for
> basic coverage.

---

## Stream Dependency Order

The script seeds resources in the correct dependency order because some
objects require others to exist first:

```
pipelines
  └── stages
        └── deals
              ├── dealflow      (move deals between stages)
              ├── deal_products (attach products to deals)
              └── notes (linked to deal)

organizations
  └── persons
        └── deals (linked to person + org)
              └── activities (linked to deal + person)

products  (standalone)
activity_types  (standalone)
filters  (standalone)
deal_fields  (standalone)
files  (attached to deal)
```

---

## Verifying Data After Seeding

Run discovery to confirm all streams are available:

```bash
tap-pipedrive --config config.json --discover > catalog.json
```

Select all streams and run a sync:

```bash
tap-pipedrive --config config.json --catalog catalog.json
```

You should see Singer `RECORD` messages for each of the 14 scriptable streams.
For `currencies` and `users`, add them via the web UI steps above if records
are missing.

---

## Re-seeding / Cleanup

Because all objects are created with a "Stitch" prefix in their names,
they are easy to find and delete manually:

- Deals titled `Stitch Test Deal — *`
- Pipeline named `Stitch Test Pipeline`
- Persons with email `*@stitchtest.invalid`
- Organizations starting with `Stitch`
- Filters named `Stitch — *`
- Deal fields named `Stitch *`
- Files named `stitch_seed_attachment.txt`

Alternatively delete and re-create the test account to start fresh.
