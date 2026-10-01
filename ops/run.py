# 目的：地方財政状況調査 表04（歳入内訳）の行02（用紙の続き）の項目特定（読み取りのみ・GCSへの書き込みなし）
# 内容：e-Stat旧DB（統計表 0003173301、〜2017年度）の項目と、CSV表04の各列を、
#       同じ年度の「全国＋47都道府県」の値の並びが一致するかで対応づけ、結果をログに出す。
#       旧DBは stats-api の /estat/pass 経由で取得する（appId不要）。出力は公表済みの集計値と名称・コードだけ

import io
import os

import httpx
import pandas as pd
from google.cloud import storage

API = "https://stats-api-709252231118.asia-northeast1.run.app"
DB_ID = "0003173301"
bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))

# ── 旧DBの項目一覧（階層つき） ──
meta = httpx.get(f"{API}/estat/meta/{DB_ID}", timeout=300).json()
for p in meta["parameters"]:
    if p["parameter"] in ("cdCat01", "cdTab"):
        for v in p["values"]:
            print(f"CAT,{p['parameter']},{v['code']},{v.get('level')},{v.get('parent_code')},{v['name']}")

# ── 旧DBの値（歳入額のみ・全団体・全年度） ──
r = httpx.get(f"{API}/estat/pass/{DB_ID}", params={"cdTab": "105900", "format": "csv", "with_code": "true"}, timeout=900)
r.raise_for_status()
db = pd.read_csv(io.BytesIO(r.content), encoding="utf-8-sig", dtype=str)
print("DB列:", list(db.columns), len(db))
cat_col = [c for c in db.columns if c.endswith("_code") and ("歳入" in c or "内訳" in c)][0]
area_col = [c for c in db.columns if c.endswith("_code") and "団体" in c][0]
time_col = [c for c in db.columns if c.endswith("_code") and "時間" in c][0]
db = db[db["値"].notna()]
db["年度"] = db[time_col].str[:4].astype(int)
print("DB時間軸コード:", sorted(db[time_col].unique()))
db["値"] = pd.to_numeric(db["値"], errors="coerce").fillna(0).round().astype("int64")
db["地域"] = db[area_col].str.zfill(5)

# ── CSV表04（行01）の値 ──
csv = pd.read_parquet(io.BytesIO(bucket.blob("chizai/pref/04/data.parquet").download_as_bytes()))
print("CSV 行の例:", csv[(csv["団体コード"] == "000000") & (csv["列番号"] == "001")].groupby(["決算年度"]).apply(lambda g: dict(zip(g["行番号"], g["値"]))).tail(3).to_dict())
c1 = csv[csv["行番号"] == "02"].copy()
c1 = c1[c1["値"].notna()]
c1["値"] = pd.to_numeric(c1["値"], errors="coerce").fillna(0).round().astype("int64")
c1["地域"] = c1["市区町村コード"]

areas = sorted(set(db["地域"]) & set(c1["地域"]))
print("照合に使う地域数:", len(areas))
for tc in sorted(db[time_col].unique()):
    y = int(tc[:4])
    if y not in set(c1["決算年度"]):
        continue
    dv = db[db[time_col] == tc].pivot_table(index=cat_col, columns="地域", values="値", aggfunc="sum").reindex(columns=areas).fillna(0).astype("int64")
    cv = c1[c1["決算年度"] == y].pivot_table(index="列番号", columns="地域", values="値", aggfunc="sum").reindex(columns=areas).fillna(0).astype("int64")
    if y in (2010, 2015):
        print("DBNAT", tc, {k: int(v) for k, v in dv["00000"].items() if k in ("1690", "1770", "1900", "1000", "1010")})
    names = c1[c1["決算年度"] == y].drop_duplicates("列番号").set_index("列番号")["列名称"]
    index = {}
    for code, row in dv.iterrows():
        index.setdefault(tuple(row.tolist()), []).append(code)
    n_one = n_multi = n_none = n_zero = 0
    for col, row in cv.iterrows():
        key = tuple(row.tolist())
        hit = index.get(key, [])
        if not any(key):
            n_zero += 1
            kind = "ZERO"
        elif len(hit) == 1:
            n_one += 1
            kind = "ONE"
        elif hit:
            n_multi += 1
            kind = "MULTI"
        else:
            n_none += 1
            kind = "NONE"
        print(f"MAP2,{tc},{y},{col},{kind},{'|'.join(hit)},{names.get(col)},{row.iloc[0]}")
    print(f"SUM,{tc},{y},一意{n_one},複数{n_multi},一致なし{n_none},全ゼロ{n_zero},DB項目数{len(dv)}")

# ── 各年度の列の並び（全国計の値つき）：2018年度以降の延長に使う ──
z = c1[c1["地域"] == "00000"].sort_values(["決算年度", "列番号"])
for _, r in z.iterrows():
    print(f"COL2,{r['決算年度']},{r['列番号']},{r['列名称']},{r['値']}")

t2 = pd.read_parquet(io.BytesIO(bucket.blob("chizai/pref/02/data.parquet").download_as_bytes()))
t2 = t2[(t2["団体コード"] == "000000") & (t2["行番号"] == "01") & (t2["列番号"] == "001")]
for _, r in t2.iterrows():
    print(f"TOTAL,{r['決算年度']},{r['値']}")
