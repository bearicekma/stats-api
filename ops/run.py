# 目的：10月1日の取得件数（約700件）が多い理由を調べる（GCS を読むだけで書き込まない）
# 内容：9月・10月ファイルの件数、10月分と9月分の求人番号の重複、受付年月日・取得日・求人種別の分布、当日の一覧キャッシュの件数を表示する

import io
import os
import re

import pandas as pd
from google.cloud import storage

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
read = lambda p: pd.read_parquet(io.BytesIO(bucket.blob(p).download_as_bytes()))
digits = lambda s: re.sub(r"\D", "", str(s))

sep, oct_ = read("hellowork/kyujin/202609.parquet"), read("hellowork/kyujin/202610.parquet")
print(f"9月 {len(sep)}件 / 10月 {len(oct_)}件 / 10月のうち9月にもある求人番号 {oct_['求人番号'].map(digits).isin(set(sep['求人番号'].map(digits))).sum()}件")
print("9月 取得日別:", sep["取得日"].astype(str).value_counts().sort_index().to_dict())
print("10月 取得日別:", oct_["取得日"].astype(str).value_counts().sort_index().to_dict())
print("10月 受付年月日別:", oct_["受付年月日"].astype(str).value_counts().sort_index().to_dict())
print("10月 求人種別×就業形態:", oct_.groupby(["求人種別", "就業形態"]).size().to_dict())

for b in sorted(bucket.list_blobs(prefix="hellowork/_list/"), key=lambda b: b.name)[-5:]:
    l = pd.read_parquet(io.BytesIO(b.download_as_bytes()))
    print(f"{b.name}: 一覧 {len(l)}件 / 種別 {l['kind'].value_counts().to_dict()}")
