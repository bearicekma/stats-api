# 目的：法人マスタを master/_M_houjin に移したため、旧保存先 houjin/zenken/（都道府県別48ファイル＋_meta.json）を削除する
# 内容：master/_M_houjin/data.parquet があり、件数が合うことを確かめてから、houjin/zenken/ 以下を削除する

import io
import json
import os

import pyarrow.parquet as pq
from google.cloud import storage

DRY_RUN = False

b = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
new = b.get_blob("master/_M_houjin/data.parquet")
meta = json.loads(b.blob("master/_M_houjin/_meta.json").download_as_text())
assert new is not None, "master/_M_houjin/data.parquet がありません"
rows = pq.ParquetFile(io.BytesIO(new.download_as_bytes())).metadata.num_rows
print(f"新: master/_M_houjin/data.parquet {new.size / 1e6:.1f}MB / {rows:,}行 / 基準日 {meta['基準日']}（_meta の件数 {meta['件数']:,}）")
assert rows == meta["件数"], "行数が _meta.json と合いません"

old = list(b.list_blobs(prefix="houjin/zenken/"))
print(f"旧: houjin/zenken/ {len(old)}ファイル / {sum(x.size for x in old) / 1e6:.1f}MB")
if DRY_RUN:
    print("DRY_RUN のため削除しません")
else:
    for x in old:
        x.delete()
    print(f"削除しました: {len(old)}ファイル / 残り {len(list(b.list_blobs(prefix='houjin/')))}ファイル（houjin/ 以下）")
