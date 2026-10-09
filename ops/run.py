# 目的：推定判定用に _M_shokugyo_hw の小分類一覧（コード・名称・例示職業名の先頭）を出力する（読み取りのみ）
# 内容：GCS の master/_M_shokugyo_hw/data.parquet を読み、1行1小分類で出力する（公開されている分類表の内容のみ）

from google.cloud import storage

from app.collectors import hellowork as hw

m = hw._read_parquet(storage.Client().bucket(hw.BUCKET_NAME), hw.SHOKUGYO_PATH)
for r in m.sort_values("code").itertuples():
    ex = (r.examples or "")[:90]
    print(f"{r.code}\t{r.name}\t{r.chu_name}\t{ex}")
