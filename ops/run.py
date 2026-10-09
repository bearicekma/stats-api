# 目的：gBizINFO API v2 の実際の応答（ベースURL・項目名・件数・ページング・充足率）を確認する（法人マスタへの属性付与の設計用）
# 内容：読み取りのみ。GCS には書き込まない。トークンは出力しない。出力は項目名・型・件数・充足率の集計だけ

import json
import os
import time

import httpx

TOKEN = os.environ.get("GBIZ_API_TOKEN", "")
print("トークン:", "あり" if TOKEN else "なし（ops.yml の env を確認）")
H = {"X-hojinInfo-api-token": TOKEN, "Accept": "application/json"}
c = httpx.Client(timeout=60, headers=H, follow_redirects=True)


def get(url, params=None):
    time.sleep(1.5)
    r = c.get(url, params=params)
    try:
        body = r.json()
    except Exception:
        body = None
    return r.status_code, body, r.text[:200]


def shape(x, depth=0, maxd=3):
    # 値を出さずに構造（キーと型）だけ返す
    if depth >= maxd:
        return type(x).__name__
    if isinstance(x, dict):
        return {k: shape(v, depth + 1, maxd) for k, v in x.items()}
    if isinstance(x, list):
        return [shape(x[0], depth + 1, maxd), f"len={len(x)}"] if x else ["(空)"]
    return type(x).__name__


# 1. ベースURLの確認
bases = ["https://api.info.gbiz.go.jp/hojin/v2", "https://api.info.gbiz.go.jp/v2", "https://info.gbiz.go.jp/hojin/v2"]
base = None
for b in bases:
    st, body, txt = get(f"{b}/hojin", {"prefecture": "20", "limit": "1"})
    print(f"[base] {b}/hojin → {st}", "" if body else txt.replace(chr(10), " ")[:120])
    if st == 200 and body and base is None:
        base = b
if base is None:
    raise SystemExit("v2 のベースURLが特定できませんでした")
print("採用するベースURL:", base)

# 2. 検索の応答の構造（長野県、1件）
st, body, _ = get(f"{base}/hojin", {"prefecture": "20", "limit": "1"})
print("\n[検索 limit=1] 最上位のキー:", list(body.keys()))
print(json.dumps(shape(body, maxd=4), ensure_ascii=False, indent=1)[:3000])
for k in ("totalCount", "total_count", "count", "pageNumber", "totalPage", "total_page"):
    if k in body:
        print(f"  {k} = {body[k]}")

# 3. 長野県1ページ目（1000件）で項目ごとの充足率
st, body, _ = get(f"{base}/hojin", {"prefecture": "20", "limit": "1000", "page": "1"})
rows = body.get("hojin-infos") or body.get("hojin_infos") or []
print(f"\n[検索 長野県 limit=1000 page=1] status={st} 件数={len(rows)} 最上位キー={list(body.keys())}")
for k in ("totalCount", "total_count", "totalPage", "total_page", "pageNumber"):
    if k in body:
        print(f"  {k} = {body[k]}")
if rows:
    keys = sorted({k for r in rows for k in r})
    print("  充足率（null・空文字・空配列以外の割合）:")
    for k in keys:
        n = sum(1 for r in rows if r.get(k) not in (None, "", [], {}))
        print(f"    {k}: {n}/{len(rows)} ({n / len(rows):.0%})")
    st_vals = {}
    for r in rows:
        st_vals[str(r.get("status"))] = st_vals.get(str(r.get("status")), 0) + 1
    print("  status の値:", st_vals)

# 4. 法人番号指定（資本金・従業員数が入っている法人を1件選ぶ）
num = next((r["corporate_number"] for r in rows if r.get("capital_stock") and r.get("employee_number")), rows[0]["corporate_number"] if rows else None)
if num:
    st, body, _ = get(f"{base}/hojin/{num}")
    print(f"\n[法人番号指定 /hojin/{{番号}}] status={st}")
    print(json.dumps(shape(body, maxd=5), ensure_ascii=False, indent=1)[:3000])
    for sub in ("finance", "subsidy", "procurement", "certification", "commendation", "patent", "workplace", "corporation"):
        st, b2, txt = get(f"{base}/hojin/{num}/{sub}")
        info = (b2 or {}).get("hojin-infos") or []
        inner = {k: shape(v, maxd=2) for k, v in (info[0] if info else {}).items() if k not in ("corporate_number", "name", "location", "status", "postal_code", "update_date", "kana", "name_en")}
        print(f"  /{sub} → {st} {json.dumps(inner, ensure_ascii=False)[:600] if b2 else txt[:120]}")

# 5. 差分（updateInfo）
for path in ("updateInfo/hojin", "updateInfo"):
    st, body, txt = get(f"{base}/{path}", {"from": "20261001", "to": "20261008", "page": "1"})
    print(f"\n[差分 /{path} 2026-10-01〜08] status={st}", "" if body else txt[:150])
    if body:
        print("  最上位キー:", list(body.keys()))
        for k in ("totalCount", "total_count", "totalPage", "total_page", "pageNumber"):
            if k in body:
                print(f"  {k} = {body[k]}")
        rows2 = body.get("hojin-infos") or []
        print("  件数(このページ):", len(rows2))
        break

print("\n完了")
