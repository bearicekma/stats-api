# 目的：hellowork 9月分で市区町村コードが付かなかった求人の原因を調べる（GCS を読むだけで書き込まない）
# 内容：未付与の求人について、就業場所住所・所在地の「先頭12文字」（市区町村レベルまで）と補足欄を表示する
#       公開ログのため、番地以降や事業所名は出力しない

import io
import os

import pandas as pd
from google.cloud import storage

from app.collectors import hellowork as hw

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
df = pd.read_parquet(io.BytesIO(bucket.blob("hellowork/kyujin/202609.parquet").download_as_bytes()))
miss = df[df["就業場所_市区町村コード"].isna()]
print(f"未付与 {len(miss)}件 / 全 {len(df)}件")

head = lambda s: None if pd.isna(s) else str(s).replace(" ", "")[:12]
out = pd.DataFrame({
    "就業場所_住所(先頭)": miss["就業場所_住所"].map(head),
    "所在地(先頭)": miss["所在地"].map(head),
    "就業場所_補足": miss["就業場所_補足"],
    "求人種別": miss["求人種別"],
})
print(out.to_string(index=False))

# 現在の照合ルールで所在地を使った場合に付くかどうか
cities = hw.load_city_master(bucket)
print("所在地で照合した場合:", [hw.match_city(a, cities)[0] for a in miss["所在地"]])
print("_M_city 長野県の件数:", len(cities), "/ 例:", [c[1] for c in cities[:5]])
