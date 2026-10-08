# 目的：昨夜（2026-10-08）のハローワーク夜間収集分に職業分類コードが付いているかの確認（読み取りのみ・GCSへの書き込みなし）
# 内容：hellowork/kyujin/202610.parquet を読み、取得日ごとの件数と、職業分類コード3列の付与状況を集計して出力する
#       出力は件数だけ

import datetime

from google.cloud import storage

from app.collectors import hellowork as hw

bucket = storage.Client().bucket(hw.BUCKET_NAME)
df = hw.normalize(hw._read_parquet(bucket, f"{hw.DATA_PREFIX}/202610.parquet"))
day = datetime.date(2026, 10, 8)
new = df[df["取得日"] == day]
print(f"202610: 全 {len(df)}件 / 取得日=2026-10-08 {len(new)}件")
print("取得日=2026-10-08 のうち 小分類コードあり:", int(new["職業分類_小分類コード"].notna().sum()),
      "/ 中分類コードあり:", int(new["職業分類_中分類コード"].notna().sum()),
      "/ 大分類コードあり:", int(new["職業分類_大分類コード"].notna().sum()))
print("小分類はあるのに大分類が欠けている件数:", int((new["職業分類_小分類コード"].notna() & new["職業分類_大分類コード"].isna()).sum()))
print("取得日別 件数 / 小分類コードあり（直近7日）:")
g = df.groupby("取得日").agg(件数=("求人番号", "size"), コードあり=("職業分類_小分類コード", lambda s: int(s.notna().sum())))
print(g.tail(7).to_string())
print("取得日=2026-10-08 の大分類コード別件数:", new["職業分類_大分類コード"].value_counts(dropna=False).sort_index().to_dict())
