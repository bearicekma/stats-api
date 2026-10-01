# 目的：地方財政状況調査（都道府県分）表21〜24（普通建設事業費の状況）の項目対応表づくりの材料を出力する（読み取りのみ・GCSへの書き込みなし）
# 内容：e-Stat旧DB（〜2017年度）の各セル（分類コードの組）とCSVの各セル（行番号×列番号）を、団体ごとの値の並びで照合し、
#       一意に一致したものを出力する。あわせてCSVの行・列の名称と全国計の値、旧DBの分類の名称を出力する

import collections
import io
import os
import sys

import httpx
import pandas as pd
from google.cloud import storage

API = "https://stats-api-709252231118.asia-northeast1.run.app"
TARGETS = [("21", "0003173092"), ("22", "0003173092"), ("23", "0003173092"), ("24", "0003173092")]
AREAS = ["00000", "01000", "13000", "20000", "23000", "27000", "40000", "47000"]  # 旧DBが大きいため8団体で照合
bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))

_cache = {}
for hyo, dbid in TARGETS:
    meta = httpx.get(f"{API}/estat/meta/{dbid}", timeout=300).json()
    dims = []
    for p in meta["parameters"]:
        if p["parameter"].startswith("cdCat") or p["parameter"] == "cdTab":
            dims.append((p["parameter"], p["name"]))
            for v in p["values"]:
                print(f"CAT,{hyo},{p['parameter']},{v['code']},{v.get('level')},{v.get('parent_code')},{v['name']}")
    print(f"DIMS,{hyo},{dbid},{'|'.join(f'{a}:{b}' for a, b in dims)}")
    params = {"format": "csv", "with_code": "true"}
    if AREAS:
        params["cdArea"] = ",".join(AREAS)
    if dbid not in _cache:
        r = httpx.get(f"{API}/estat/pass/{dbid}", params=params, timeout=1500)
        r.raise_for_status()
        _cache[dbid] = r.content
    db = pd.read_csv(io.BytesIO(_cache[dbid]), encoding="utf-8-sig", dtype=str)
    area = [c for c in db.columns if c.endswith("_code") and ("団体" in c or "地域" in c)][0]
    tcol = [c for c in db.columns if c.endswith("_code") and "時間" in c][0]
    kcols = [b + "_code" for a, b in dims if b + "_code" in db.columns]
    db = db[db["値"].notna()]
    db["値"] = pd.to_numeric(db["値"], errors="coerce").fillna(0).round().astype("int64")
    db["地域"] = db[area].str.zfill(5)
    db["key"] = db[kcols].fillna("").agg("|".join, axis=1)
    print(f"DB,{hyo},{len(db)},{'|'.join(kcols)}")
    sys.stdout.flush()

    csv = pd.read_parquet(io.BytesIO(bucket.blob(f"chizai/pref/{hyo}/data.parquet").download_as_bytes()))
    csv["値"] = pd.to_numeric(csv["値"], errors="coerce").fillna(0).round().astype("int64")
    areas = sorted(set(db["地域"]) & set(csv["市区町村コード"]))
    for tc in sorted(db[tcol].unique()):
        y = int(tc[:4])
        cy = csv[csv["決算年度"] == y]
        if cy.empty:
            continue
        dv = db[db[tcol] == tc].pivot_table(index="key", columns="地域", values="値", aggfunc="sum").reindex(columns=areas).fillna(0).astype("int64")
        idx = collections.defaultdict(list)
        for key, row in dv.iterrows():
            t = tuple(row.tolist())
            if any(t):
                idx[t].append(key)
        cv = cy.pivot_table(index=["行番号", "列番号"], columns="市区町村コード", values="値", aggfunc="sum").reindex(columns=areas).fillna(0).astype("int64")
        for (g, c), row in cv.iterrows():
            hits = idx.get(tuple(row.tolist()), [])
            if len(hits) == 1:
                print(f"MATCH,{hyo},{tc},{y},{g},{c},{hits[0]}")
    z = csv[csv["市区町村コード"] == "00000"]
    for (y, g), gg in z.groupby(["決算年度", "行番号"]):
        print(f"ROWN,{hyo},{y},{g},{gg['行名称'].iloc[0]}")
    for (y, c), gg in z.groupby(["決算年度", "列番号"]):
        print(f"COLN,{hyo},{y},{c},{gg['列名称'].dropna().iloc[0] if gg['列名称'].notna().any() else ''}")
    for r0 in z.itertuples():
        print(f"CELL,{hyo},{r0.決算年度},{r0.行番号},{r0.列番号},{r0.値}")
    sys.stdout.flush()
