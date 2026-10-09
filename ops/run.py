# 目的：法人マスタ（houjin/zenken/）のParquetのサイズを確認する（_M_houjin として master/ に置く設計の判断用）
# 内容：読み取りのみ。GCSのファイルサイズを集計して出力する。あわせて全国を1ファイルにした場合のサイズを試算する

import io
import os
import tempfile

import pandas as pd
from google.cloud import storage

b = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
blobs = sorted(b.list_blobs(prefix="houjin/zenken/"), key=lambda x: -x.size)
tot = sum(x.size for x in blobs)
print(f"ファイル数 {len(blobs)} / 合計 {tot / 1e6:.1f}MB")
for x in blobs[:6]:
    print(f"  {x.name}: {x.size / 1e6:.1f}MB")
print("  …長野県:", next((f"{x.size / 1e6:.1f}MB" for x in blobs if x.name.endswith("pref=20.parquet")), "なし"))

# 全国を1ファイルにまとめた場合（zstd圧縮・行グループ単位で都道府県がまとまるよう並べ替え）
tmp = tempfile.mkdtemp()
parts = []
for x in blobs:
    if x.name.endswith(".parquet"):
        p = os.path.join(tmp, os.path.basename(x.name))
        x.download_to_filename(p)
        parts.append(pd.read_parquet(p))
df = pd.concat(parts, ignore_index=True)
print(f"全国 {len(df):,}行 / メモリ上 {df.memory_usage(deep=True).sum() / 1e9:.2f}GB")
for comp in ("snappy", "zstd"):
    out = os.path.join(tmp, f"all_{comp}.parquet")
    df.sort_values(["都道府県コード", "団体コード", "法人番号"]).to_parquet(out, index=False, compression=comp, row_group_size=100_000)
    print(f"  1ファイル（{comp}）: {os.path.getsize(out) / 1e6:.1f}MB")
