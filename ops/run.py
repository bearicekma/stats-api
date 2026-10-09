# 目的：_M_shokugyo_hw の作り直し（ページの「0045-08」誤記で 045-08 医薬品販売店員が抜け、045-07 の例示職業名が上書きされていた不具合の修正）
# 内容：修正後の app/collectors/shokugyo.py で _M_shokugyo_hw を組み立て、件数と 045-07 / 045-08 の行を確認する
#       DRY_RUN = True なら保存しない（公開されている分類表の内容のみ出力）

import pandas as pd

from app.collectors import shokugyo as sk

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)

DRY_RUN = False

h = sk.save_hw_master(dry_run=DRY_RUN)
print(h[h["code"].isin(["045-06", "045-07", "045-08", "045-09"])][["code", "name", "jsco_chu_code", "examples"]].to_string(index=False))
print("対応先のない小分類:", h.loc[h["jsco_chu_code"].isna(), "code"].tolist())
print("例示に別の小分類の見出しが混ざる件数:", int(h["examples"].fillna("").str.contains("例示職業名").sum()))
