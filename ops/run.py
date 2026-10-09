# 目的：Gビズインフォ「データダウンロード」の基本情報（Kihonjoho）を実際に取得し、列・件数・充足率を確認する（法人マスタへの属性付与の設計用）
# 内容：読み取りのみ。GCS には書き込まない。トークンは出力しない。出力は列名・件数・充足率・値の種類数の集計だけ

import io
import os
import re
import tempfile
import time
import zipfile

import httpx
import pandas as pd
from bs4 import BeautifulSoup

TOKEN = os.environ.get("GBIZ_API_TOKEN", "")
print("トークン:", "あり" if TOKEN else "なし")
BASE = "https://info.gbiz.go.jp"
c = httpx.Client(timeout=httpx.Timeout(60.0, read=1800.0), follow_redirects=True,
                 headers={"User-Agent": "Mozilla/5.0 (stats-api ops)"})

# セッションを作り、フォームの送信先（jsessionid付き）を得る
r = c.get(f"{BASE}/hojin/DownloadTop")
form = BeautifulSoup(r.text, "html.parser").find("form", id="down")
action = form.get("action")
url = action if action.startswith("http") else BASE + action
print("送信先:", re.sub(r";jsessionid=[^?]*", ";jsessionid=…", url))

data = {"downfile": "Kihonjoho", "meta": "", "downenc": "UTF-8", "apiToken": TOKEN, "isZip": "on", "downtype": "zip"}
t0 = time.time()
path = os.path.join(tempfile.mkdtemp(), "kihon.bin")
with c.stream("POST", url, data=data) as resp:
    print("status:", resp.status_code, "content-type:", resp.headers.get("content-type"),
          "disposition:", resp.headers.get("content-disposition"), "length:", resp.headers.get("content-length"))
    with open(path, "wb") as f:
        for chunk in resp.iter_bytes(1 << 20):
            f.write(chunk)
size = os.path.getsize(path)
print(f"取得 {size / 1e6:.1f}MB / {time.time() - t0:.0f}秒")
head = open(path, "rb").read(4)
if head[:2] != b"PK":
    txt = open(path, "rb").read(600).decode("utf-8", "replace")
    print("ZIPではありません。先頭:", re.sub(r"\s+", " ", txt)[:400])
    raise SystemExit(0)

z = zipfile.ZipFile(path)
for i in z.infolist():
    print(f"  ZIP内: {i.filename} {i.file_size / 1e6:.1f}MB")
name = max(z.infolist(), key=lambda i: i.file_size).filename

# 先頭行（ヘッダー）と、全体の充足率をチャンクで集計する
with z.open(name) as fh:
    first = fh.readline().decode("utf-8-sig")
print("\nヘッダー:", first.strip()[:2000])

counts, total = None, 0
pref_counts = {}
nagano = None
with z.open(name) as fh:
    for ch in pd.read_csv(io.TextIOWrapper(fh, encoding="utf-8-sig"), dtype=str, keep_default_na=False, chunksize=200_000):
        filled = (ch != "").sum()
        counts = filled if counts is None else counts + filled
        total += len(ch)
        loc = next((col for col in ch.columns if "所在地" in col or col.lower() == "location"), None)
        if loc:
            p = ch[loc].str.extract(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")[0].fillna("?")
            for k, v in p.value_counts().items():
                pref_counts[k] = pref_counts.get(k, 0) + int(v)
            nag = ch[ch[loc].str.startswith("長野県")]
            nf = (nag != "").sum()
            nagano = nf if nagano is None else nagano + nf
print(f"\n総行数: {total:,}")
print("列ごとの充足率（空でない割合）: 全国 / 長野県")
nn = pref_counts.get("長野県", 0)
for col in counts.index:
    a = counts[col] / total if total else 0
    b = (nagano[col] / nn) if nagano is not None and nn else float("nan")
    print(f"  {col}: {a:.1%} / {b:.1%}")
print("\n都道府県別の行数（上位と長野県）:", dict(sorted(pref_counts.items(), key=lambda x: -x[1])[:8]), "長野県:", nn)
print("\n完了")
