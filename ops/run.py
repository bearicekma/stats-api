# 目的：地方財政状況調査 表15・37・39の項目対応表づくり（読み取りのみ・GCSへの書き込みなし）
# 内容：e-Stat旧DB（〜2017年度）は「行の項目(cat03)×列の指標(cat01)」の2次元。CSVの各セルの
#       「全国＋47都道府県」の値の並びと一致する旧DBのセルを探し、行→cat03・列→cat01 の対応を年度ごとに多数決で決める。
#       あわせて各年度のCSVの行・列の名称と全国計の値を出力する。出力は公表済みの集計値・名称・コードだけ

import collections
import io
import os

import httpx
import pandas as pd
from google.cloud import storage

API = "https://stats-api-709252231118.asia-northeast1.run.app"
TARGETS = [("15", "0003173109"), ("37", "0003173057"), ("39", "0003173097")]
bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))

for hyo, dbid in TARGETS:
    meta = httpx.get(f"{API}/estat/meta/{dbid}", timeout=300).json()
    pname = {}
    for p in meta["parameters"]:
        if p["parameter"] in ("cdCat01", "cdCat03"):
            pname[p["parameter"]] = p["name"]
            for v in p["values"]:
                print(f"CAT,{hyo},{p['parameter']},{v['code']},{v.get('level')},{v.get('parent_code')},{v['name']}")
    r = httpx.get(f"{API}/estat/pass/{dbid}", params={"format": "csv", "with_code": "true"}, timeout=1500)
    r.raise_for_status()
    db = pd.read_csv(io.BytesIO(r.content), encoding="utf-8-sig", dtype=str)
    c1, c3 = pname["cdCat01"] + "_code", pname["cdCat03"] + "_code"
    area = [c for c in db.columns if c.endswith("_code") and "団体" in c][0]
    tcol = [c for c in db.columns if c.endswith("_code") and "時間" in c][0]
    db = db[db["値"].notna()]
    db["値"] = pd.to_numeric(db["値"], errors="coerce").fillna(0).round().astype("int64")
    db["地域"] = db[area].str.zfill(5)
    print(f"DB {hyo} {dbid} {len(db)}件 列={list(db.columns)}")

    csv = pd.read_parquet(io.BytesIO(bucket.blob(f"chizai/pref/{hyo}/data.parquet").download_as_bytes()))
    csv["値"] = pd.to_numeric(csv["値"], errors="coerce").fillna(0).round().astype("int64")
    areas = sorted(set(db["地域"]) & set(csv["市区町村コード"]))
    for tc in sorted(db[tcol].unique()):
        y = int(tc[:4])
        cy = csv[csv["決算年度"] == y]
        if cy.empty:
            continue
        dv = db[db[tcol] == tc].pivot_table(index=[c3, c1], columns="地域", values="値", aggfunc="sum").reindex(columns=areas).fillna(0).astype("int64")
        cv = cy.pivot_table(index=["行番号", "列番号"], columns="市区町村コード", values="値", aggfunc="sum").reindex(columns=areas).fillna(0).astype("int64")
        index = collections.defaultdict(list)
        for key, row in dv.iterrows():
            t = tuple(row.tolist())
            if any(t):
                index[t].append(key)
        rv, cvote = collections.defaultdict(collections.Counter), collections.defaultdict(collections.Counter)
        for (g, c), row in cv.iterrows():
            hits = index.get(tuple(row.tolist()), [])
            if len(hits) == 1:
                rv[g][hits[0][0]] += 1
                cvote[c][hits[0][1]] += 1
        for g, cnt in sorted(rv.items()):
            (best, n), total = cnt.most_common(1)[0], sum(cnt.values())
            print(f"ROWMAP,{hyo},{tc},{y},{g},{best},{n},{total}")
        for c, cnt in sorted(cvote.items()):
            (best, n), total = cnt.most_common(1)[0], sum(cnt.values())
            print(f"COLMAP,{hyo},{tc},{y},{c},{best},{n},{total}")

    z = csv[csv["市区町村コード"] == "00000"]
    for (y, g), gg in z.groupby(["決算年度", "行番号"]):
        first = gg.sort_values("列番号").iloc[0]
        print(f"ROWN,{hyo},{y},{g},{first['行名称']},{first['値']}")
    for (y, c), gg in z.groupby(["決算年度", "列番号"]):
        print(f"COLN,{hyo},{y},{c},{gg['列名称'].dropna().iloc[0] if gg['列名称'].notna().any() else ''}")
    # 合計の検算用に全国計の全セル
    for _, r0 in z.iterrows():
        print(f"CELL,{hyo},{r0['決算年度']},{r0['行番号']},{r0['列番号']},{r0['値']}")
