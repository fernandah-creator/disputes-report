import json, urllib.request, ssl, datetime, os
from collections import defaultdict

METABASE_URL = os.environ["METABASE_URL"]
METABASE_KEY = os.environ["METABASE_API_KEY"]
SHEETS_ID    = os.environ["SHEETS_ID"]
CLIENT_ID    = os.environ["GOOGLE_CLIENT_ID"]
CLIENT_SECRET= os.environ["GOOGLE_CLIENT_SECRET"]
REFRESH_TOKEN= os.environ["GOOGLE_REFRESH_TOKEN"]
CARD_ID      = 16937

TRACKER_TABS = ["Nov2025","Dec25","Jan","Feb","March","April"]
WON_STATUSES = {
    "dispute won - 1st presentment",
    "dispute won - 2nd presentment",
    " dispute won - 2nd presentment",
    "dispute won - 2nd presentment",
}

def get_access_token():
    body = json.dumps({
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token"
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["access_token"]

def read_sheet(tab, token):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values/{tab}!A:H"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read()).get("values", [])
    except Exception as e:
        print(f"  Warning: could not read {tab}: {e}")
        return []

def fetch_tracker():
    print("Getting Google OAuth token...")
    token = get_access_token()
    tracker = defaultdict(lambda: {"won": 0, "nr": 0, "total": 0})
    for tab in TRACKER_TABS:
        rows = read_sheet(tab, token)
        print(f"  {tab}: {len(rows)} rows")
        for row in rows[1:]:  # skip header
            if len(row) < 8: continue
            status = row[0].strip()
            org_id = str(row[7]).strip()
            if not org_id or not status: continue
            tracker[org_id]["total"] += 1
            if status.lower() in WON_STATUSES:
                tracker[org_id]["won"] += 1
            if status.lower() == "needs response":
                tracker[org_id]["nr"] += 1
    print(f"Tracker: {len(tracker)} orgs")
    return tracker

def fetch_metabase():
    today    = datetime.date.today().isoformat()
    year_ago = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
    body = json.dumps({"parameters": [
        {"type":"date/single","value":year_ago,"target":["variable",["template-tag","Date_From"]]},
        {"type":"date/single","value":today,   "target":["variable",["template-tag","Date_To"]]}
    ]}).encode()
    req = urllib.request.Request(
        f"{METABASE_URL}/api/card/{CARD_ID}/query/json",
        data=body,
        headers={"Content-Type":"application/json","x-api-key":METABASE_KEY},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read())

def process(rows, tracker):
    orgs = defaultdict(lambda: {
        "cases":0,"usd":0.0,"reasons":defaultdict(int),"merchants":defaultdict(int),
        "statuses":defaultdict(int),"months":defaultdict(int),"customer":"",
        "card_types":defaultdict(int),"mccs":defaultdict(int)
    })
    all_months = set()
    for r in rows:
        oid = str(r.get("org_id",""))
        if not oid: continue
        o = orgs[oid]
        o["customer"] = r.get("name","") or r.get("customer","")
        o["usd"]     += float(r.get("USD_AMOUNT") or 0)
        o["cases"]   += 1
        raw_dt = str(r.get("transactionDateTime","") or "")
        month  = raw_dt[:7] if len(raw_dt) >= 7 else ""
        if month:
            o["months"][month] += 1
            all_months.add(month)
        o["reasons"][r.get("dispute_reason","") or "other"] += 1
        merch = r.get("merchant_name","") or ""
        if merch: o["merchants"][merch] += 1
        status = r.get("STATUS","") or r.get("status","") or ""
        if status: o["statuses"][status] += 1
        card = r.get("card_type","") or ""
        if card: o["card_types"][card] += 1
        mcc   = str(r.get("mcc","") or "")
        mcc_d = r.get("mccDescription","") or ""
        if mcc: o["mccs"][f"{mcc} ({mcc_d})"] += 1

    all_months = sorted(all_months)
    result = []
    for oid, o in orgs.items():
        total = o["cases"]
        if not total: continue
        # Win rate from tracker
        t = tracker.get(oid)
        if t and t["total"] > 0:
            win_rate = round(t["won"] / t["total"] * 100, 1)
            needs_response_tracker = t["nr"]
        else:
            win_rate = None
            needs_response_tracker = 0

        top_r = sorted(o["reasons"].items(),  key=lambda x:-x[1])
        top_m = sorted(o["merchants"].items(), key=lambda x:-x[1])
        trend = [{"month":m,"cases":o["months"].get(m,0),"usd":0} for m in all_months]
        active = sum(1 for t in trend if t["cases"]>0)
        if active < 2: continue

        result.append({
            "org_id":       oid,
            "customer":     o["customer"],
            "total_cases":  total,
            "total_usd":    round(o["usd"],2),
            "avg_usd":      round(o["usd"]/total,2),
            "win_rate":     win_rate,
            "needs_response_tracker": needs_response_tracker,
            "statuses":     dict(o["statuses"]),
            "raw_statuses": [{"label":k,"count":v} for k,v in sorted(o["statuses"].items(),key=lambda x:-x[1])],
            "top_reasons":  [{"label":k,"count":v} for k,v in top_r[:5]],
            "top_merchants":[{"label":k,"count":v} for k,v in top_m[:5]],
            "top_mccs":     [{"label":k,"count":v} for k,v in sorted(o["mccs"].items(),key=lambda x:-x[1])[:4]],
            "card_types":   dict(o["card_types"]),
            "trend":        trend
        })

    result.sort(key=lambda x:-x["total_cases"])
    mwd = sorted(set(t["month"] for o in result for t in o["trend"] if t["cases"]>0))
    return {
        "generated_at": datetime.datetime.utcnow().isoformat()+"Z",
        "total_cases":  sum(o["total_cases"] for o in result),
        "total_orgs":   len(result),
        "months_range": {"from": mwd[0] if mwd else "", "to": mwd[-1] if mwd else ""},
        "orgs": result
    }

print("Fetching Metabase Q16937...")
rows = fetch_metabase()
print(f"Rows: {len(rows)}")

tracker = fetch_tracker()

output = process(rows, tracker)
print(f"Orgs: {output['total_orgs']}, Cases: {output['total_cases']}")

with open("disputes.json","w") as f:
    json.dump(output, f, ensure_ascii=False)
print("disputes.json saved")
