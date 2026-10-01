# 目的：地方財政状況調査 表46・47（2011年度〜、歳入内訳の復旧・復興／全国防災分）と表16（人件費）の
#       折り返し行（行02）に名前を付けるための材料を出力する（読み取りのみ・GCSへの書き込みなし）
# 内容：表04・46・47・16 の年度・行・列ごとの列名称と、全団体の値（; 区切り）を出力する

import io
import os

import pandas as pd
from google.cloud import storage

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
for hyo, y0 in [("04", 2011), ("46", 2011), ("47", 2011), ("16", 2016)]:
    d = pd.read_parquet(io.BytesIO(bucket.blob(f"chizai/pref/{hyo}/data.parquet").download_as_bytes()))
    d = d[d["決算年度"] >= y0]
    d["値"] = pd.to_numeric(d["値"], errors="coerce").fillna(0).round().astype("int64")
    areas = sorted(d["団体コード"].unique())
    print(f"AREAS,{hyo},{';'.join(areas)}")
    for (y, g, c), gg in d.groupby(["決算年度", "行番号", "列番号"]):
        v = gg.set_index("団体コード")["値"].reindex(areas).fillna(0).astype("int64")
        nm = gg["列名称"].dropna()
        print(f"V,{hyo},{y},{g},{c},{nm.iloc[0] if len(nm) else ''},{gg['行名称'].iloc[0]},{';'.join(map(str, v.tolist()))}")
