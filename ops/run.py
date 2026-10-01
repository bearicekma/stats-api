# 目的：地方財政状況調査（都道府県分）で、用紙の「続きの行」がある表を洗い出す（読み取りのみ）
# 内容：全国計について、同じ年度に同じ行名称が複数の行番号で出る表を数え、
#       その行で値が入っている列の数（続きの行は途中の列までしか値がない）を表示する

import io
import os

import pandas as pd
from google.cloud import storage

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
paths = sorted(b.name for b in bucket.list_blobs(prefix="chizai/pref/") if b.name.endswith("/data.parquet"))
for p in paths:
    df = pd.read_parquet(io.BytesIO(bucket.blob(p).download_as_bytes()), columns=["決算年度", "団体コード", "表名称", "行番号", "行名称", "列番号", "値"])
    z = df[df["団体コード"] == "000000"]
    if z.empty:
        z = df[df["団体コード"] == df["団体コード"].iloc[0]]
    dup = z.groupby(["決算年度", "行名称"])["行番号"].nunique()
    dup = dup[dup > 1]
    filled = z[z["値"].notna()].groupby(["決算年度", "行番号"])["列番号"].nunique()
    ncols = z.groupby("決算年度")["列番号"].nunique()
    if len(dup):
        yrs = sorted(dup.index.get_level_values(0).unique())
        names = sorted(set(dup.index.get_level_values(1)))[:5]
        # 重複した行名称を持つ行のうち、行番号が最小でない行の「値あり列数 / 全列数」
        y = yrs[-1]
        rows = z[(z["決算年度"] == y) & z["行名称"].isin(dup.loc[y].index)].groupby("行名称")["行番号"].apply(sorted)
        ratio = []
        for nm, rs in rows.items():
            for r in rs[1:]:
                ratio.append(f"{r}:{filled.get((y, r), 0)}/{ncols[y]}")
        print(f"{p.split('/')[2]} | {z['表名称'].iloc[-1][:30]} | 重複のある年度 {yrs[0]}-{yrs[-1]}（{len(yrs)}年） | 行名称 {names} | {y}年度 続き行の値あり列 {ratio[:6]}")
    else:
        print(f"{p.split('/')[2]} | 重複なし")
