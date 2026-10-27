# 目的：医療・介護の求人の産業分類コードが 830 になっている原因を調べる（GCS を読むだけで書き込まない）
# 内容：月ファイルごとの 830 の件数、830 に付いた「産業分類」の文字列、中分類 83〜85 の分布、_M_sangyo の 83〜85 の行、
#       現在の照合ルールで付け直した場合のコードを表示する（求人の中身は出さない）

import io
import os

import pandas as pd
from google.cloud import storage

from app.collectors import hellowork as hw

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
m = hw.load_sangyo_master(bucket)
print("_M_sangyo:", len(m), "件 / 列:", list(m.columns))
print(m[m["chu_code"].isin(["83", "84", "85"])][["code", "name", "chu_code"]].to_string(index=False))

for b in sorted(bucket.list_blobs(prefix="hellowork/kyujin/"), key=lambda b: b.name):
    df = pd.read_parquet(io.BytesIO(b.download_as_bytes()), columns=["産業分類", "産業分類_中分類コード", "産業分類_小分類コード", "取得日"])
    print(f"\n== {b.name}: {len(df)}件 / 830 は {int((df['産業分類_小分類コード'] == '830').sum())}件")
    print("830 に付いた産業分類:", df.loc[df["産業分類_小分類コード"] == "830", "産業分類"].value_counts().head(15).to_dict())
    print("830 の取得日:", df.loc[df["産業分類_小分類コード"] == "830", "取得日"].astype(str).value_counts().sort_index().to_dict())
    med = df[df["産業分類_中分類コード"].isin(["83", "84", "85"])]
    print("中分類83〜85 の小分類:", med["産業分類_小分類コード"].value_counts().to_dict())
    print("中分類83〜85 の産業分類（上位）:", med["産業分類"].value_counts().head(15).to_dict())
    re_ = [hw.match_sangyo(n, m)[2] for n in df["産業分類"]]
    diff = (pd.Series(re_, index=df.index).fillna("") != df["産業分類_小分類コード"].fillna(""))
    print("現在のルールで付け直すと変わる件数:", int(diff.sum()), "/ 例:", pd.DataFrame({"産業分類": df.loc[diff, "産業分類"], "今": df.loc[diff, "産業分類_小分類コード"], "再": pd.Series(re_, index=df.index)[diff]}).drop_duplicates().head(10).to_dict("records"))
