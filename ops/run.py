# 目的：地方財政状況調査（都道府県分）の調査表様式Excelから、表の行見出しを確認する（読み取りのみ）
# 内容：e-Statの様式ファイル（01〜22表）を取得し、シート名の一覧と、04表シートの左上の文字を表示する

import io

import httpx
import openpyxl

URL = "https://www.e-stat.go.jp/stat-search/file-download?statInfId=000040374254&fileKind=0"
r = httpx.get(URL, timeout=120, follow_redirects=True)
print("status", r.status_code, "bytes", len(r.content), r.headers.get("content-type"))
wb = openpyxl.load_workbook(io.BytesIO(r.content), data_only=True)
print("シート:", wb.sheetnames)
for name in wb.sheetnames:
    if "04" in name or "４" in name:
        ws = wb[name]
        print(f"===== {name} {ws.max_row}x{ws.max_column}")
        for row in ws.iter_rows(min_row=1, max_row=min(ws.max_row, 80), max_col=min(ws.max_column, 40)):
            texts = [f"{c.coordinate}={str(c.value).strip()}" for c in row if c.value not in (None, "")]
            if texts:
                print(" | ".join(texts)[:600])
