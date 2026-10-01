# 目的：地方財政状況調査（都道府県分）の項目対応表づくりのための調査（読み取りのみ・GCSへの書き込みなし）
# 内容：指定した表の全国計（団体コード000000）について、行・列の名称がいつ切り替わったかと、
#       切り替わり前後の値を表示する。出力は公表済みの全国計の集計値と名称だけ

import io
import os
import sys

import pandas as pd
from google.cloud import storage

from app.collectors.chizai import _norm

TABLES = os.environ.get("CHIZAI_TABLES", "04,15,37,39").split(",")
bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 60)


def ranges(years):
    ys, out, s = sorted(set(years)), [], None
    for i, y in enumerate(ys):
        if s is None:
            s = y
        if i == len(ys) - 1 or ys[i + 1] != y + 1:
            out.append(f"{s}" if s == y else f"{s}-{y}")
            s = None
    return ",".join(out)


for hyo in TABLES:
    blob = bucket.blob(f"chizai/pref/{hyo}/data.parquet")
    df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
    z = df[df["団体コード"] == "000000"].copy()
    print(f"\n######## 表{hyo} 全国計 {len(z)}件 年度 {z['決算年度'].min()}-{z['決算年度'].max()}")
    for kind, no, name, other_no in [("行", "行番号", "行名称", "列番号"), ("列", "列番号", "列名称", "行番号")]:
        z["キー"] = z[name].map(_norm)
        print(f"\n==== 表{hyo} {kind}：名称ごとの年度と番号（名称は最新年度の表記）")
        rows = []
        for key, g in z.groupby("キー"):
            g = g.sort_values("決算年度")
            nums = "; ".join(f"{n}:{ranges(gg['決算年度'])}" for n, gg in g.groupby(no))
            rows.append((min(g["決算年度"]), g[name].iloc[-1], ranges(g["決算年度"]), nums))
        for r in sorted(rows):
            print(f"  {r[1]} | 年度 {r[2]} | 番号 {r[3]}")

        # 名称の切り替わり：Y年度にあってY+1年度にない名称／Y+1年度に初めて出る名称
        print(f"\n==== 表{hyo} {kind}：名称の切り替わりと前後の値（相手側は先頭の{'列' if kind == '行' else '行'}番号の値）")
        first_other = sorted(z[other_no].unique())[0]
        val = z[z[other_no] == first_other].groupby(["決算年度", "キー"])["値"].sum()
        names = z.drop_duplicates(["キー"], keep="last").set_index("キー")[name]
        years = sorted(z["決算年度"].unique())
        by_year = {y: set(z.loc[z["決算年度"] == y, "キー"]) for y in years}
        for a, b in zip(years, years[1:]):
            gone, new = by_year[a] - by_year[b], by_year[b] - by_year[a]
            if not gone and not new:
                continue
            print(f"  -- {a}→{b}")
            for k in sorted(gone):
                print(f"     消えた: {names[k]} ({a}年度値 {val.get((a, k))})")
            for k in sorted(new):
                print(f"     現れた: {names[k]} ({b}年度値 {val.get((b, k))})")
    sys.stdout.flush()
