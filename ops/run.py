# 目的：市町村分（2011〜2013年度）の「全国（団体コード00〜）」でキー重複が出た原因と、団体の一覧（_index）が倍になった原因を調べる（読み取りのみ）
import io, os
import pandas as pd
from google.cloud import storage

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
rd = lambda p: pd.read_parquet(io.BytesIO(bucket.blob(p).download_as_bytes()))
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30); pd.set_option("display.max_colwidth", 40)

for y in (2014, 2013, 2012):
    ix = rd(f"chizai/city/_index/{y}.parquet")
    print(f"== _index {y}: {len(ix)}件 列={list(ix.columns)}")
    for c in ix.columns:
        if ix[c].nunique() < 60: print("  ", c, ix[c].value_counts(dropna=False).head(12).to_dict())
    print(ix.head(5).to_string())
    if "団体コード" in ix.columns:
        g = ix.groupby("団体コード").size()
        print("  団体コードあたりの件数", g.value_counts().head(5).to_dict())
        k = g[g > 1].index[:3]
        print(ix[ix.団体コード.isin(k)].to_string())

for y in (2012, 2014):
    d = rd(f"chizai/city/02/00/{y}.parquet")
    print(f"== 表02 全国 {y}: {len(d)}件 団体={d.groupby(['団体コード','団体名'], dropna=False).size().to_dict()}")
    dup = d[d.duplicated(subset=["決算年度", "団体コード", "行番号", "列番号"], keep=False)].sort_values(["団体コード", "行番号", "列番号"])
    print(dup.head(12).to_string())
    for c in d.columns:
        if c not in ("値",) and d[c].nunique() < 30: print("  ", c, d[c].value_counts(dropna=False).head(10).to_dict())
