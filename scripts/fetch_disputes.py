import json, urllib.request, ssl, datetime, os
from collections import defaultdict

METABASE_URL = os.environ["METABASE_URL"]
METABASE_KEY = os.environ["METABASE_API_KEY"]
CARD_ID = 16937

def fetch_rows():
    today     = datetime.date.today().isoformat()
    year_ago  = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
    body = json.dumps({"parameters": [
        {"type":"date/single","value":year_ago,"target":["variable",["template-tag","Date_From"]]},
        {"type":"date/single","value":today,    "target":["variable",["template-tag","Date_To"]]}
    ]}).encode()
    req = urllib.request.Request(
        f"{METABASE_URL}/api/card/{CARD_ID}/query/json",
        data=body,
        headers={"Content-Type":"application/json","x-api-key":METABASE_KEY},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read())

def process(rows):
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
        o["usd"]     += float(r.get("USD_AMOUNT") or r.get("usd_amount") or 0)
        o["cases"]   += 1
        raw_dt = str(r.get("transactionDateTime","") or r.get("created_at","") or "")
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
        won      = o["statuses"].get("won",0)
        resolved = won + o["statuses"].get("lost",0) + o["statuses"].get("not_eligible",0)
        wr       = round(won/resolved*100,1) if resolved > 0 else None
        top_r    = sorted(o["reasons"].items(),   key=lambda x:-x[1])
        top_m    = sorted(o["merchants"].items(),  key=lambda x:-x[1])
        trend    = [{"month":m,"cases":o["months"].get(m,0),"usd":0} for m in all_months]
        active   = sum(1 for t in trend if t["cases"]>0)
        if active < 2: continue
        result.append({
            "org_id":       oid,
            "customer":     o["customer"],
            "total_cases":  total,
            "total_usd":    round(o["usd"],2),
            "avg_usd":      round(o["usd"]/total,2),
            "win_rate":     wr,
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
        "months_range": {"from": mwd[0] if mwd else "","to": mwd[-1] if mwd else ""},
        "orgs": result
    }

print("Fetching Metabase Q16937...")
rows = fetch_rows()
print(f"Rows fetched: {len(rows)}")
output = process(rows)
print(f"Orgs: {output['total_orgs']}, Cases: {output['total_cases']}")
with open("disputes.json","w") as f:
    json.dump(output, f, ensure_ascii=False)
print("disputes.json saved OK")
