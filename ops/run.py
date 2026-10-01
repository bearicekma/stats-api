# 目的：本番の /chizai/data（item・shihyo 指定）の動作確認（読み取りのみ）
import httpx

API = "https://stats-api-709252231118.asia-northeast1.run.app"
for h in ["04", "15", "37", "39"]:
    r = httpx.get(f"{API}/chizai/items", params={"hyo": h}, timeout=120).json()
    print(h, "items:", r["count"])
def show(params, years):
    r = httpx.get(f"{API}/chizai/data", params=params, timeout=300)
    j = r.json()
    print(params, r.status_code, j.get("count"))
    for x in j.get("data", []):
        if x["決算年度"] in years:
            print("  ", x["決算年度"], x["行番号"], x["列番号"], x["行名称"], "|", x["列名称"], "|", x["項目名"], "|", x.get("指標名"), "|", x["値"])
show({"hyo": "15", "dantai": "20000", "item": "公債費", "shihyo": "決算額"}, (2005, 2006, 2024))
show({"hyo": "37", "dantai": "00000", "item": "減収補てん債(昭和57・61・平成5~当年度分)|臨時財政対策債", "shihyo": "差引現在高"}, (2017, 2018, 2019, 2020, 2024))
show({"hyo": "39", "dantai": "00000", "item": "市中銀行", "shihyo": "差引現在高|差引現在高の利率別/0.5%以下"}, (2002, 2003, 2008, 2020, 2021, 2024))
