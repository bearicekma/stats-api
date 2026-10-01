# 目的：本番の /chizai/data（item 指定）と /chizai/items の動作確認（読み取りのみ）
import httpx

API = "https://stats-api-709252231118.asia-northeast1.run.app"
r = httpx.get(f"{API}/chizai/items", params={"hyo": "04"}, timeout=120).json()
print("items:", r["count"], [x["項目名"] for x in r["data"][:5]])
r = httpx.get(f"{API}/chizai/data", params={"hyo": "04", "dantai": "20000", "item": "歳入合計|国庫支出金/電源立地地域対策交付金"}, timeout=120).json()
print("data:", r["count"])
for x in r["data"]:
    if x["決算年度"] in (1989, 2002, 2003, 2017, 2018, 2024):
        print(" ", x["決算年度"], x["行番号"], x["列番号"], x["列名称"], x["項目名"], x["値"])
r = httpx.get(f"{API}/chizai/data", params={"hyo": "04", "item": "存在しない"}, timeout=120)
print("unknown:", r.status_code, r.text[:200])
