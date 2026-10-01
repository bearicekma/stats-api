# 目的：地方財政状況調査（都道府県分）の調査表様式（Excel）から、表16・46・47の見出しを読み取る（読み取りのみ・GCSへの書き込みなし）
# 内容：e-Statの調査表様式ファイルをダウンロードし、表16・46・47のシートの空でないセルを「シート,行,列,文字」で出力する

import io
import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "xlrd", "openpyxl"], check=True)
import httpx
import pandas as pd

URL = "https://www.e-stat.go.jp/stat-search/file-download?statInfId={}&fileKind=0"
FILES = {  # 調査年: (01表～22表, 23表～51表)
    "2012": ("000031398452", "000031398453"),
    "2019": ("000031756946", "000031756947"),
    "2021": ("000032091860", "000032091861"),
    "2025": ("000040374254", "000040374255"),
}
for year, ids in FILES.items():
    for sid in ids:
        b = httpx.get(URL.format(sid), timeout=120, follow_redirects=True).content
        try:
            book = pd.read_excel(io.BytesIO(b), sheet_name=None, header=None, dtype=str)
        except Exception as e:
            print(f"ERR,{year},{sid},{e}")
            continue
        print(f"BOOK,{year},{sid},{'|'.join(book)}")
        for sh, df in book.items():
            if not any(k in sh for k in ("16", "46", "47")):
                continue
            for i, row in df.iterrows():
                for j, v in row.items():
                    if isinstance(v, str) and v.strip():
                        print(f"C,{year},{sh},{i},{j},{' '.join(v.split())}")
