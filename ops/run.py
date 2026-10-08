# 目的：職業分類マスタの dry run（2回目）と、求人の「職業情報」リンク・求人票にある職業コードの確認（GCSへの書き込みなし）
# 内容：
#   1. _M_shokugyo_hw を組み立て直し、例示職業名の取り込み（見出し行の読み飛ばし）を確認する
#   2. 求人詳細ページの「職業情報」リンク（ID_shokugyojohou）のリンク先とパラメータを確認する
#   3. 「求人票を表示」（GECA110020）の応答の形式を確認し、PDFなら「職業分類」を含む行だけを出力する
#      （求人番号などの値は伏せ、出力は分類コード・分類名の行だけにする）

import io
import re
import time
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup
from google.cloud import storage

from app.collectors import hellowork as hw
from app.collectors import shokugyo as sk

pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 50)

# ---------- 1. _M_shokugyo_hw の dry run ----------
h = sk.save_hw_master(dry_run=True)
print("例示なし:", h.loc[h["examples"].isna(), "code"].tolist())
print("☓例示なし:", len(h[h["not_examples"].isna()]), "件")
print("〇例示に「例示職業名」「☓」が混ざる件数:", int(h["examples"].fillna("").str.contains("例示職業名|☓").sum()))
print(h[h["code"].isin(["001-99", "002-99", "067-01", "098-01"])][["code", "name", "examples", "not_examples"]].to_string(index=False))


def mask(name, values):
    # 求人番号などの値は伏せ、それ以外は先頭20字だけ出す
    return [("***" if re.search(r"(?i)kj|no", name) else v[:20]) for v in values]


# ---------- 2・3. 求人詳細ページ ----------
bucket = storage.Client().bucket(hw.BUCKET_NAME)
lists = sorted(b.name for b in bucket.list_blobs(prefix=hw.LIST_PREFIX + "/") if b.name.endswith(".parquet"))
links = hw._read_parquet(bucket, lists[-1])
client = httpx.Client(headers={"User-Agent": hw.USER_AGENT}, timeout=60, follow_redirects=True)
checked = 0
for url in links["url"].sample(10, random_state=1):
    time.sleep(hw.DETAIL_INTERVAL)
    r = client.get(url)
    if r.status_code != 200 or "kjNo" not in r.text:
        continue
    checked += 1
    soup = BeautifulSoup(r.text, "html.parser")
    print(f"\n----- 詳細ページ {checked} -----")
    a = soup.find(id="ID_shokugyojohou")
    if a is not None:
        u = urlparse(urljoin(str(r.url), a["href"]))
        print("職業情報リンク:", u.netloc, u.path, {k: mask(k, v) for k, v in parse_qs(u.query).items()}, "文字:", a.get_text(strip=True))
    btn = soup.find("a", href=re.compile("GECA110020"))
    if btn is not None and checked <= 2:
        time.sleep(hw.DETAIL_INTERVAL)
        pr = client.get(urljoin(str(r.url), btn["href"]))
        ctype = pr.headers.get("content-type", "")
        print("求人票を表示:", pr.status_code, ctype, len(pr.content), "bytes")
        if "pdf" in ctype or pr.content[:4] == b"%PDF":
            with pdfplumber.open(io.BytesIO(pr.content)) as pdf:
                text = "\n".join(p.extract_text() or "" for p in pdf.pages)
            print("  PDF頁数:", len(pdf.pages), "/「職業分類」出現:", text.count("職業分類"))
            for ln in text.splitlines():
                if "職業分類" in ln or re.search(r"\b\d{3}-\d{2}\b", ln):
                    print("   ", re.sub(r"\d{5}-\d{6,}", "*****-********", ln)[:80])
        else:
            ps = BeautifulSoup(pr.text, "html.parser")
            print("  HTML: 「職業分類」出現", pr.text.count("職業分類"), "/ iframe・embed:", [(e.name, urlparse(e.get("src", "")).path) for e in ps.find_all(["iframe", "embed", "object"])])
    if checked >= 4:
        break
client.close()
