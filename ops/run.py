# 目的：職業分類マスタ（_M_shokugyo / _M_shokugyo_hw）を GCS に保存する（対応表は確認済み）
# 内容：対応表 app/data/shokugyo_hw_jsco.csv を付けた _M_shokugyo_hw と _M_shokugyo を組み立てて保存し、
#       対応先の中分類コード・名称が _M_shokugyo と一致しているか、対応のない小分類がないかを確認する

import pandas as pd

from app.collectors import shokugyo as sk

pd.set_option("display.width", 200)

DRY_RUN = False

j = sk.save_jsco_master(dry_run=DRY_RUN)
h = sk.save_hw_master(dry_run=DRY_RUN)

jsco_chu = j.drop_duplicates("chu_code").set_index("chu_code")["chu_name"]
print("対応先のない小分類:", h.loc[h["jsco_chu_code"].isna(), "code"].tolist())
print("標準分類にない対応先コード:", sorted(set(h["jsco_chu_code"].dropna()) - set(jsco_chu.index)))
print("対応先の名称が標準分類と違う:", h.loc[h["jsco_chu_name"] != h["jsco_chu_code"].map(jsco_chu), ["code", "jsco_chu_code", "jsco_chu_name"]].values.tolist())
print("小分類単位で上書きした行:", h.loc[h["code"].isin(["003-01", "029-02", "029-03", "087-01", "088-04"]), ["code", "name", "jsco_chu_code"]].values.tolist())
print("\n標準中分類ごとの厚労省編小分類の件数:")
print(h.groupby(["jsco_chu_code", "jsco_chu_name"]).size().to_string())
