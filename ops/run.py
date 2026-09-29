# 目的：資本金を円単位の整数に変換する（保存済みの全月ファイルが対象）
# 内容：月ファイルごとに、変換前の値あり件数・変換後の整数件数・変換できなかった件数を表示する
#       DRY_RUN=True のうちは保存しない。夜間収集（20:00〜20:50）と重ならない時間に実行すること

import io
import os

import pandas as pd
from google.cloud import storage

from app.collectors import hellowork as hw

DRY_RUN = True

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
for blob in sorted(bucket.list_blobs(prefix="hellowork/kyujin/"), key=lambda b: b.name):
    raw = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
    df = hw.normalize(raw)
    bad = raw["資本金"].notna() & df["資本金"].isna()
    print(f"{blob.name}: {len(df)}件 / 型 {raw['資本金'].dtype} → {df['資本金'].dtype} / 値あり {raw['資本金'].notna().sum()} → {df['資本金'].notna().sum()}件 / 変換できず {int(bad.sum())}件"
          f" / 最小 {df['資本金'].min()} / 中央値 {df['資本金'].median()} / 最大 {df['資本金'].max()}")
    if not DRY_RUN:
        hw._write_parquet(bucket, blob.name, df, schema=hw.SCHEMA)
        print(f"✅ {blob.name} に保存")
