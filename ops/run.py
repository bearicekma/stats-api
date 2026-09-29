# 目的：資本金列の表記パターンを調べる（GCS を読むだけで書き込まない）
# 内容：全月ファイルの資本金を、数字を「9」に置き換えた形に集計して件数を表示する（公開ログのため値そのものは出さない）

import io
import os
import re

import pandas as pd
from google.cloud import storage

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
s = pd.concat([pd.read_parquet(io.BytesIO(b.download_as_bytes()), columns=["資本金"])["資本金"] for b in bucket.list_blobs(prefix="hellowork/kyujin/")])
print(f"件数 {len(s)} / 空欄 {s.isna().sum()}")
print(s.dropna().map(lambda v: re.sub(r"[0-9０-９]", "9", str(v))).value_counts().to_string())
