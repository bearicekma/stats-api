# 目的：ops ワークフローの動作確認（GCS を読むだけで書き込まない）
# 内容：hellowork の保存済み月ごとの件数と、_M_sangyo の件数・大分類数を表示する

import io
import os

import pandas as pd
from google.cloud import storage

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))

# hellowork の月ファイルごとの件数と、産業分類コードの付与件数
for blob in sorted(bucket.list_blobs(prefix="hellowork/kyujin/"), key=lambda b: b.name):
    df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()), columns=["求人番号", "産業分類_小分類コード", "就業場所_市区町村コード"])
    print(f"{blob.name}: {len(df)}件 / 小分類コードあり {df['産業分類_小分類コード'].notna().sum()}件 / 市区町村コードあり {df['就業場所_市区町村コード'].notna().sum()}件")

# _M_sangyo の件数
m = pd.read_parquet(io.BytesIO(bucket.blob("master/_M_sangyo/data.parquet").download_as_bytes()))
print(f"_M_sangyo: {len(m)}件 / 大分類 {m['dai_code'].nunique()} / 中分類 {m['chu_code'].nunique()} / is_kanri {int(m['is_kanri'].sum())}件")
