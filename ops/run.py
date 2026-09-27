# 目的：hellowork 9月分の市区町村コードを、改善した照合ルールで付け直す
# 内容：refresh_codes('202609') を実行する。DRY_RUN=True のうちは件数の変化を表示するだけで保存しない

from app.collectors import hellowork as hw

DRY_RUN = False

df = hw.refresh_codes("202609", dry_run=DRY_RUN)
print("未付与の就業場所（先頭12文字）:", [str(a).replace(" ", "")[:12] for a in df.loc[df["就業場所_市区町村コード"].isna(), "就業場所_住所"]])
