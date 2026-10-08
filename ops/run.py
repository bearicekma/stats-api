# 目的：職業分類マスタ作成の事前確認（読み取りのみ・GCSへの書き込みなし）
# 内容：
#   A. 保存済みのハローワーク求人に職業分類のコードがあるか（列名・「その他項目」のキー・職種列のコード有無を集計）
#   B. 求人詳細ページ1件・検索ページに「職業分類」の項目があるか（要素IDと周辺の見出しだけを出力。求人の中身は出さない）
#   C. 厚労省編職業分類（令和4年）のハローワーク掲載ページ15枚の構造と件数
#   D. 日本標準職業分類（平成21年）分類項目名CSV（総務省）の形式と件数

import io
import json
import re
import time
import unicodedata
from collections import Counter

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage

from app.collectors import hellowork as hw

bucket = storage.Client().bucket(hw.BUCKET_NAME)
client = httpx.Client(headers={"User-Agent": hw.USER_AGENT}, timeout=60, follow_redirects=True)
CODE_PAT = re.compile(r"\d{3}-\d{2}")

# ---------- A. 保存済みデータ ----------
print("===== A. 保存済みのハローワーク求人 =====")
months = sorted(b.name for b in bucket.list_blobs(prefix=hw.DATA_PREFIX + "/") if b.name.endswith(".parquet"))
print("月ファイル:", months)
df = pd.concat([hw._read_parquet(bucket, p) for p in months[-2:]], ignore_index=True)
print(f"直近2か月の件数: {len(df)}")
print("「職業」「分類」を含む列:", [c for c in df.columns if "職業" in c or "分類" in c])
keys = Counter(k for s in df["その他項目"].dropna() for k in json.loads(s).keys())
print("「その他項目」のキー（件数順）:", keys.most_common(80))
print("職種列に NNN-NN 形式のコードを含む件数:", int(df["職種"].fillna("").str.contains(CODE_PAT).sum()))
code_keys = [k for k in keys if any(CODE_PAT.search(str(json.loads(s).get(k, ""))) for s in df["その他項目"].dropna().head(300))]
print("値に NNN-NN 形式のコードを含む「その他項目」のキー:", code_keys)

# ---------- B. 詳細ページ・検索ページ ----------
print("\n===== B. 詳細ページ・検索ページ =====")
lists = sorted(b.name for b in bucket.list_blobs(prefix=hw.LIST_PREFIX + "/") if b.name.endswith(".parquet"))
links = hw._read_parquet(bucket, lists[-1])
found = False
for url in links["url"].head(5):
    time.sleep(hw.DETAIL_INTERVAL)
    r = client.get(url)
    if r.status_code != 200 or "kjNo" not in r.text:
        continue
    soup = BeautifulSoup(r.text, "html.parser")
    ids = [e["id"] for e in soup.find_all(id=True)]
    print(f"詳細ページ: 要素ID {len(ids)} 個 / 'ID_' 始まり {sum(i.startswith('ID_') for i in ids)} 個")
    print("「職業」を含む箇所の数:", r.text.count("職業"), "/「職業分類」:", r.text.count("職業分類"))
    # 「職業分類」を含む見出し要素と、その次の要素のタグ・ID だけを出す（値は出さない）
    for e in soup.find_all(string=re.compile("職業分類")):
        nxt = e.parent.find_next(id=True)
        print("  見出し:", e.parent.name, e.parent.get("id"), e.parent.get("class"), str(e).strip()[:20], "→ 次の要素:", nxt.name if nxt else None, nxt.get("id") if nxt else None)
    print("職業・分類らしい要素ID:", [i for i in ids if re.search(r"(?i)brui|bunrui|sks|shkg|shokugyo", i)])
    found = True
    break
print("詳細ページ取得:", "成功" if found else "失敗（掲載終了など）")

time.sleep(hw.LIST_INTERVAL)
sr = client.get(hw.SEARCH_URL)
print(f"\n検索ページ: HTTP {sr.status_code} / 「職業分類」出現 {sr.text.count('職業分類')} 回 / 「職種」出現 {sr.text.count('職種')} 回")
ssoup = BeautifulSoup(sr.text, "html.parser")
print("職業・職種らしい入力欄:", [(e.name, e.get("id"), e.get("name")) for e in ssoup.find_all(["input", "select", "button", "a"]) if re.search(r"(?i)sksu|shkg|shokugyo|brui|職業|職種", " ".join(str(e.get(a, "")) for a in ("id", "name", "value", "onclick", "href")) + e.get_text())][:30])

# ---------- C. 厚労省編職業分類（ハローワーク掲載ページ） ----------
print("\n===== C. 厚労省編職業分類（令和4年）ハローワーク掲載ページ =====")
top = client.get("https://www.hellowork.mhlw.go.jp/info/mhlw_job_dictionary.html").content.decode("utf-8")
pages = sorted(set(re.findall(r"mhlw_job_dictionary_(\d{2})\.html", top)))
print("大分類ページ番号:", pages)
total_chu, total_sho, sample_done = set(), set(), False
for p in pages:
    time.sleep(hw.LIST_INTERVAL)
    html = client.get(f"https://www.hellowork.mhlw.go.jp/info/mhlw_job_dictionary_{p}.html").content.decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "?"
    text = unicodedata.normalize("NFKC", soup.get_text("\n", strip=True))
    chu = set(re.findall(r"^(\d{3}) \S", text, flags=re.M))
    sho = set(re.findall(r"^(\d{3}-\d{2})", text, flags=re.M))
    total_chu |= chu
    total_sho |= sho
    print(f"  page {p}: {title} / 中分類 {len(chu)} / 小分類 {len(sho)}")
    if not sample_done:
        print("  表の構造（先頭12行）:")
        for tr in soup.find_all("tr")[:12]:
            print("   ", [c.get_text(" ", strip=True)[:50] for c in tr.find_all(["th", "td"])])
        sample_done = True
print(f"合計: 中分類 {len(total_chu)} / 小分類 {len(total_sho)}")

# ---------- D. 日本標準職業分類 CSV（総務省） ----------
print("\n===== D. 日本標準職業分類（平成21年）分類項目名CSV =====")
time.sleep(1)
raw = client.get("https://www.soumu.go.jp/main_content/000587432.csv").content
for enc in ("utf-8-sig", "cp932"):
    try:
        txt = raw.decode(enc)
        print("文字コード:", enc)
        break
    except UnicodeDecodeError:
        continue
lines = txt.splitlines()
print("行数:", len(lines))
print("先頭25行:")
for ln in lines[:25]:
    print("   ", ln[:120])
print("末尾5行:")
for ln in lines[-5:]:
    print("   ", ln[:120])

client.close()
