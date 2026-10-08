# 目的：職業分類マスタ（_M_shokugyo / _M_shokugyo_hw）の作成確認（dry run・GCSへの書き込みなし）
# 内容：
#   1. app/collectors/shokugyo.py で両マスタを組み立て、件数・中分類一覧・先頭行を出力する（対応表の下書きに使う）
#   2. 求人詳細ページ1件の「職業情報」（ID_shokugyojohou）の中身と、求人票PDFなどのリンクを確認する
#      （出力は要素のタグ・属性名・リンクのパスとパラメータ名だけ。求人の中身は出さない）

import re
import time
from urllib.parse import parse_qs, urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage

from app.collectors import hellowork as hw
from app.collectors import shokugyo as sk

pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 60)

# ---------- 1. マスタの dry run ----------
print("===== _M_shokugyo（日本標準職業分類）=====")
j = sk.save_jsco_master(dry_run=True)
print("列:", list(j.columns))
print(j.drop_duplicates("chu_code")[["dai_code", "dai_name", "chu_code", "chu_name"]].to_string(index=False))

print("\n===== _M_shokugyo_hw（厚労省編職業分類）=====")
h = sk.save_hw_master(dry_run=True)
print("列:", list(h.columns))
print(h.drop_duplicates("chu_code")[["dai_code", "dai_name", "chu_code", "chu_name"]].to_string(index=False))
print("\n先頭5行:")
print(h.head(5).to_string(index=False))
print("例示なしの小分類:", h.loc[h["examples"].isna(), "code"].tolist())
print("☓例示に〇が混ざる・〇例示に☓が混ざる件数:", int(h["examples"].fillna("").str.contains("☓").sum()), int(h["not_examples"].fillna("").str.contains("〇").sum()))

# ---------- 2. 求人詳細ページの「職業情報」 ----------
print("\n===== 求人詳細ページの職業情報 =====")
bucket = storage.Client().bucket(hw.BUCKET_NAME)
lists = sorted(b.name for b in bucket.list_blobs(prefix=hw.LIST_PREFIX + "/") if b.name.endswith(".parquet"))
links = hw._read_parquet(bucket, lists[-1])
client = httpx.Client(headers={"User-Agent": hw.USER_AGENT}, timeout=60, follow_redirects=True)
checked = 0
for url in links["url"].head(8):
    time.sleep(hw.DETAIL_INTERVAL)
    r = client.get(url)
    if r.status_code != 200 or "kjNo" not in r.text:
        continue
    soup = BeautifulSoup(r.text, "html.parser")
    for e in soup.find_all(id="ID_shokugyojohou"):
        print("ID_shokugyojohou:", e.name, "属性:", sorted(e.attrs.keys()), "文字数:", len(e.get_text(strip=True)))
        for a in e.find_all("a", href=True):
            u = urlparse(a["href"])
            print("  リンク:", u.netloc, re.sub(r"\d", "9", u.path), "パラメータ:", sorted(parse_qs(u.query).keys()), "リンク文字:", a.get_text(strip=True)[:30])
        lab = e.find_previous(["th", "dt", "label", "span"])
        print("  直前の見出し:", lab.get_text(strip=True)[:30] if lab else None)
    pdf_links = [a for a in soup.find_all("a", href=True) if re.search(r"(?i)pdf|kjhyo|求人票", a["href"] + a.get_text())]
    print("求人票・PDF関連のリンク:", [(urlparse(a["href"]).path, sorted(parse_qs(urlparse(a["href"]).query).keys()), a.get_text(strip=True)[:20]) for a in pdf_links][:5])
    buttons = [(b.get("id"), b.get("name"), (b.get("value") or b.get_text(strip=True))[:20]) for b in soup.find_all(["input", "button"]) if b.get("type") in ("button", "submit") or b.name == "button"]
    print("ボタン:", buttons[:15])
    checked += 1
    if checked >= 2:
        break
client.close()
print("確認した詳細ページ:", checked)
