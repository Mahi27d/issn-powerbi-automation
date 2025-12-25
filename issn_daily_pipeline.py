import csv
import time
import requests
from datetime import date
from tqdm import tqdm
import os

BATCH_SIZE = 100
SLEEP = 2
HEADERS = {"User-Agent": "ISSN-Metadata-Bot/1.0"}

# Load ISSNs
issns = []
with open("issn_master.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        issns.append(row["issn"])

def fetch_crossref(issn):
    try:
        r = requests.get(
            f"https://api.crossref.org/journals/{issn}",
            headers=HEADERS,
            timeout=20
        )
        if r.status_code == 200:
            return r.json()["message"]
    except:
        pass
    return {}

def fetch_openalex(issn):
    try:
        r = requests.get(
            "https://api.openalex.org/sources",
            params={"filter": f"issn:{issn}"},
            headers=HEADERS,
            timeout=20
        )
        if r.status_code == 200 and r.json()["results"]:
            return r.json()["results"][0]
    except:
        pass
    return {}

rows = []
today = date.today()

for i in tqdm(range(0, len(issns), BATCH_SIZE)):
    batch = issns[i:i+BATCH_SIZE]

    for issn in batch:
        cr = fetch_crossref(issn)
        oa = fetch_openalex(issn)

        rows.append({
            "issn": issn,
            "journal_title": cr.get("title"),
            "publisher": cr.get("publisher"),
            "doi_prefix": cr.get("prefix"),
            "country": oa.get("country_code"),
            "open_access": oa.get("is_oa"),
            "fetch_date": today
        })

    time.sleep(SLEEP)

os.makedirs("data", exist_ok=True)

with open("data/issn_metadata.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print("CSV GENERATED SUCCESSFULLY")
