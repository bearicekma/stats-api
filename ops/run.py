# 目的：在留外国人統計（市区町村別テーブルデータ t2）の初回取込（full）
# 内容：app/collectors/zairyu.py の run("full") を実行し、5時点（2023-12〜2025-12）を zairyu/t2/ に保存する
#       ops.yml には e-Stat の appId がないため、カタログ検索の代わりに e-Stat で確認済みのファイル番号を渡す
#       （以後の定期更新は collect-zairyu ワークフローが appId 付きでカタログから探す）
#       Power Pivot の明細を読む pbixray は requirements.txt にないので、ここで入れる
#       書き込み先は新しいパス（zairyu/）のみ。出力は件数・総数などの集計だけ

import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pbixray==0.15.5"], check=True)

from app.collectors import zairyu as z

FILES = [  # (調査年月, 表番号, statInfId, 更新日) … e-Stat getDataCatalog（statsCode=00250012）で確認
    ("202312", "23-12-t2", "000040186957", "2025-05-09"),
    ("202406", "24-06-t2", "000040228086", "2024-12-13"),
    ("202412", "24-12-t2", "000040292373", "2025-07-28"),
    ("202506", "25-06-t2", "000040379766", "2025-12-12"),
    ("202512", "25-12-t2", "000040472266", "2026-07-10"),
]
z.fetch_catalog = lambda: [{
    "period": p, "table": t, "statInfId": s, "modified": m,
    "name": f"{t}_在留外国人統計テーブルデータ（国籍・地域別　在留資格別　市区町村別）",
    "url": f"https://www.e-stat.go.jp/stat-search/file-download?&statInfId={s}&fileKind=0",
} for p, t, s, m in FILES]

n = z.run("full")

periods = z._load_df(z.PERIODS_PATH)
print(periods.to_string(index=False))
data = z._load_df(z.DATA_PATH)
print("時点別 合計:", data.groupby("調査年月")["人数"].sum().to_dict())
print("地域区分別 行数:", data["地域区分"].value_counts().to_dict())
print("列:", list(data.columns))
print("取込時点数:", n)
