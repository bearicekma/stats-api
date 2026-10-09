# 目的：厚労省編職業分類ページ（大分類07 販売・営業）で 045-07 / 045-08 がどう書かれているかを確認する（読み取りのみ）
# 内容：ハローワークの mhlw_job_dictionary_04.html を取得し、045-06〜045-10 付近の表の行を、セルの構造が分かる形で出力する
#       （公開されている分類表の内容のみ）

import re

import httpx
from bs4 import BeautifulSoup

from app.collectors import hellowork as hw
from app.collectors import shokugyo as sk

html = httpx.get(sk.HW_PAGE_URL.format("04"), headers={"User-Agent": hw.USER_AGENT}, timeout=60, follow_redirects=True).content.decode("utf-8")
soup = BeautifulSoup(html, "html.parser")
rows = soup.find_all("tr")
idx = [i for i, tr in enumerate(rows) if re.search(r"045-0[6-9]", tr.get_text())]
print("該当行番号:", idx)
for i in range(max(min(idx) - 2, 0), min(max(idx) + 4, len(rows))):
    tr = rows[i]
    cells = tr.find_all(["td", "th"])
    print(f"--- tr[{i}] セル{len(cells)}個")
    for c in cells:
        attrs = {k: v for k, v in c.attrs.items() if k in ("rowspan", "colspan", "class")}
        print(f"   <{c.name} {attrs}> {c.get_text(' | ', strip=True)[:160]}")
# 045-08 を含む生のHTML断片
m = re.search(r"045-08", html)
print("\n045-08 の前後の生HTML:")
print(html[max(m.start() - 600, 0): m.end() + 300] if m else "（045-08 なし）")
