# 目的：表48・49（繰越額等の復旧・復興分／全国防災分）が表03と、表95・96（基金の復旧・復興分／全国防災分）が表31と同じ様式かを確かめる（読み取りのみ）
# あわせて表69（国民健康保険事業会計、2018年度〜）の名称と全国計を出力する
# 内容：①年度ごとに行・列の番号と名称を表07〜13と比べ、違いを出力する
#       ②表07〜13の項目対応表を当てはめ、親＝子の合計などの検算が表07〜13と同じように成り立つかを全団体で確かめる

import collections
import io
import os

import pandas as pd
from google.cloud import storage

bucket = storage.Client().bucket(os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data"))
items = pd.read_csv("app/data/chizai_items.csv", dtype=str).fillna("")


def load(hyo):
    d = pd.read_parquet(io.BytesIO(bucket.blob(f"chizai/pref/{hyo}/data.parquet").download_as_bytes()))
    d = d[d["決算年度"] >= 2012].copy()
    d["値"] = pd.to_numeric(d["値"], errors="coerce").fillna(0)
    return d


def label(d, base):
    # 表07〜13の対応表（軸=行・列）を年度範囲つきで当てはめる
    x = items[items["表番号"] == base]
    r, c = {}, {}
    for t in x.itertuples():
        for y in range(max(int(t.決算年度_開始), 2012), int(t.決算年度_終了) + 1):
            if t.軸 == "行":
                r[(y, t.行番号)] = (t.項目名, t.項目コード)
            else:
                c[(y, t.列番号)] = t.項目名
    d["行項目"] = [r.get((y, g), ("?", ""))[0] for y, g in zip(d["決算年度"], d["行番号"])]
    d["行コード"] = [r.get((y, g), ("?", ""))[1] for y, g in zip(d["決算年度"], d["行番号"])]
    d["列項目"] = [c.get((y, k), "?") for y, k in zip(d["決算年度"], d["列番号"])]
    return d


def checks(d):
    # 戻り値: {(検算の種類, 親): 合わなかったセル数}
    bad = collections.Counter()
    d = d[(d["行項目"] != "（空欄）") & (d["列項目"] != "（空欄）")]
    s = d.groupby(["決算年度", "団体コード", "行項目", "列項目"])["値"].sum()
    code = dict(zip(d["行項目"], d["行コード"]))
    for (y, a), g in s.groupby(level=[0, 1]):
        m = g.droplevel([0, 1]).unstack(fill_value=0)  # 行項目 × 列項目
        for axis, names in (("行", list(m.index)), ("列", list(m.columns))):
            mm = m if axis == "行" else m.T
            for p in names:
                kids = [k for k in names if k.startswith(p + "/") and "/" not in k[len(p) + 1:]]
                if kids:
                    diff = (mm.loc[kids].sum() - mm.loc[p]).abs() > 2
                    bad[(axis, p)] += int(diff.sum())
        if "歳出合計" in m.index:
            top = [k for k in m.index if "/" not in k and k != "歳出合計" and code.get(k, "").isdigit()]
            exp = [k for k in top if int(code[k]) < 700]
            fin = [k for k in top if int(code[k]) >= 700]
            for nm, ks in (("歳出合計=性質別", exp), ("歳出合計=財源", fin)):
                bad[("行", nm)] += int(((m.loc[ks].sum() - m.loc["歳出合計"]).abs() > 2).sum())
    return bad


for base, tgts in (("03", ("48", "49")), ("31", ("95", "96"))):
    b = label(load(base), base)
    bb = checks(b)
    ok = {key for key, n in bb.items() if n == 0}
    print(f"BASE,{base},検算で常に成り立つもの{len(ok)}/{len(bb)}")
    for tgt in tgts:
        t = load(tgt)
        t = t[t["表名称"].fillna("").str.contains("復旧|防災|減災")] if tgt == "49" else t
        for ax, no, nm in (("行", "行番号", "行名称"), ("列", "列番号", "列名称")):
            for y in sorted(t["決算年度"].unique()):
                A = set(map(tuple, b[b["決算年度"] == y][[no, nm]].drop_duplicates().fillna("").values))
                B = set(map(tuple, t[t["決算年度"] == y][[no, nm]].drop_duplicates().fillna("").values))
                if A != B:
                    print(f"DIFF,{tgt},{base},{y},{ax},表{base}のみ={sorted(A - B)[:6]},表{tgt}のみ={sorted(B - A)[:6]}")
        t = label(t, base)
        unm = t[((t["行項目"] == "?") | (t["列項目"] == "?")) & (t["値"] != 0)]
        blank = t[((t["行項目"] == "（空欄）") | (t["列項目"] == "（空欄）")) & (t["値"] != 0)]
        tb = checks(t)
        ng = {key: n for key, n in tb.items() if key in ok and n}
        print(f"RES,{tgt},{base},年度{t['決算年度'].min()}-{t['決算年度'].max()},件数{len(t)},対応なし非ゼロ{len(unm)},空欄に値{len(blank)},検算不一致{len(ng)}")
        for key, n in list(ng.items())[:10]:
            print(f"NG,{tgt},{key[0]},{key[1]},{n}")
# 表69：名称と全国計
d = pd.read_parquet(io.BytesIO(bucket.blob("chizai/pref/69/data.parquet").download_as_bytes()))
z = d[d["市区町村コード"] == "00000"]
for (y, g), gg in z.groupby(["決算年度", "行番号"]):
    print(f"ROWN,69,{y},{g},{gg['行名称'].iloc[0]}")
for (y, c), gg in z.groupby(["決算年度", "列番号"]):
    print(f"COLN,69,{y},{c},{gg['列名称'].dropna().iloc[0] if gg['列名称'].notna().any() else ''}")
for r0 in z.itertuples():
    print(f"CELL,69,{r0.決算年度},{r0.行番号},{r0.列番号},{0 if pd.isna(r0.値) else r0.値}")
