# 一時的な確認スクリプト：ハローワーク産業分類コード表と保存済み求人の「産業分類」の照合率を調べる（書き込みなし）
import io, re, unicodedata, requests, pandas as pd

UA = {"User-Agent": "Mozilla/5.0"}
norm = lambda s: re.sub(r"[\s、,]", "", unicodedata.normalize("NFKC", str(s)))
out = []

# コード表（大・中・小分類）を取得し、ページ内の全表を縦に連結して保存する
for lv in ("01", "02", "03"):
    html = requests.get(f"https://www.hellowork.mhlw.go.jp/info/industry_list{lv}.html", headers=UA, timeout=60).content
    tabs = pd.read_html(io.BytesIO(html))
    out.append(f"list{lv}: {len(tabs)} tables, shapes={[t.shape for t in tabs[:6]]}")
    out.append(tabs[0].head(4).to_string())
    pd.concat([t.astype(str).assign(_table=i) for i, t in enumerate(tabs)], ignore_index=True).to_csv(f"tmp/industry_list{lv}.csv", index=False)

tabs = pd.read_html(io.BytesIO(requests.get("https://www.hellowork.mhlw.go.jp/info/industry_list03.html", headers=UA, timeout=60).content))
sho = pd.concat([t.iloc[:, :2].set_axis(["code", "name"], axis=1) for t in tabs if t.shape[1] >= 2], ignore_index=True).dropna()
sho["code"] = sho["code"].map(lambda c: unicodedata.normalize("NFKC", str(c)).split(".")[0].strip().zfill(3))
sho = sho[sho["code"].str.fullmatch(r"\d{3}")].drop_duplicates("code")

# 保存済み求人（産業分類の列だけ使う。求人の中身はコミットしない）
kj = pd.read_csv(io.BytesIO(requests.get("https://stats-api-709252231118.asia-northeast1.run.app/hellowork/kyujin", params={"from": "2026-09", "format": "csv"}, timeout=300).content), dtype=str)
lk = sho.assign(key=sho["name"].map(norm)).drop_duplicates("key", keep=False).set_index("key")["code"]
kj["小分類コード"] = kj["産業分類"].map(norm).map(lk)

out.append(f"小分類 {len(sho)}行 / 求人 {len(kj)}件 / 産業分類 {kj['産業分類'].nunique()}種類 / 一致率 {kj['小分類コード'].notna().mean():.3f}")
out.append("--- 一致しなかった文字列 ---")
out.append(kj.loc[kj["小分類コード"].isna(), "産業分類"].value_counts(dropna=False).to_string())
kj.groupby(["産業分類", "小分類コード"], dropna=False).size().rename("件数").reset_index().sort_values("件数", ascending=False).to_csv("tmp/sangyo_values.csv", index=False)
open("tmp/sangyo_result.txt", "w").write("\n".join(out) + "\n")
print("\n".join(out))
