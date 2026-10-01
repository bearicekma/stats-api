# 目的：地方財政状況調査（都道府県分）表16（人件費の状況）の行02の見出しを、調査表様式と作成要領から読み取る（読み取りのみ・GCSへの書き込みなし）
# 内容：①調査表様式（Excel）の表16シートについて、セルの文字に加え、図形（テキストボックス）の文字と位置を出力する
#       ②作成要領（PDF）の表16に関するページの文字を出力する

import io
import re
import subprocess
import sys
import zipfile

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "openpyxl", "xlrd", "pypdf"], check=True)
import httpx
from pypdf import PdfReader

URL = "https://www.e-stat.go.jp/stat-search/file-download?statInfId={}&fileKind={}"
FORMS = {2020: "000032188822", 2021: "000040044816", 2022: "000040171230", 2023: "000040231574", 2024: "000040374254"}  # 決算年度: 様式（01表～22表）
GUIDES = {2020: "000032188821", 2024: "000040374253"}  # 決算年度: 作成要領


def get(sid, kind):
    return httpx.get(URL.format(sid, kind), timeout=300, follow_redirects=True).content


for y, sid in FORMS.items():
    b = get(sid, 0)
    print(f"FORM,{y},{sid},{'xlsx' if b[:2] == b'PK' else 'xls'},{len(b)}")
    if b[:2] != b"PK":
        continue
    z = zipfile.ZipFile(io.BytesIO(b))
    # シート名→シートXML→図形XML の対応をたどる
    wb = z.read("xl/workbook.xml").decode("utf-8")
    rels = z.read("xl/_rels/workbook.xml.rels").decode("utf-8")
    rid = dict(re.findall(r'<sheet[^>]*name="([^"]+)"[^>]*r:id="([^"]+)"', wb))
    tgt = dict(re.findall(r'Id="([^"]+)"[^>]*Target="([^"]+)"', rels)) | {a: b for b, a in re.findall(r'Target="([^"]+)"[^>]*Id="([^"]+)"', rels)}
    if "16" not in rid:
        continue
    sheet = "xl/" + tgt[rid["16"]].lstrip("/").replace("xl/", "")
    srels = sheet.replace("worksheets/", "worksheets/_rels/") + ".rels"
    if srels not in z.namelist():
        print(f"NODRAW,{y}")
        continue
    for d in re.findall(r'Target="([^"]*drawing[^"]*)"', z.read(srels).decode("utf-8")):
        dx = z.read("xl/drawings/" + d.split("/")[-1]).decode("utf-8")
        for anc in re.findall(r"<xdr:(?:twoCellAnchor|oneCellAnchor)[\s\S]*?</xdr:(?:twoCellAnchor|oneCellAnchor)>", dx):
            fr = re.search(r"<xdr:from><xdr:col>(\d+)</xdr:col>[\s\S]*?<xdr:row>(\d+)</xdr:row>", anc)
            to = re.search(r"<xdr:to><xdr:col>(\d+)</xdr:col>[\s\S]*?<xdr:row>(\d+)</xdr:row>", anc)
            txt = "".join(re.findall(r"<a:t>([^<]*)</a:t>", anc))
            if txt.strip():
                print(f"TB,{y},{fr.group(2) if fr else ''},{fr.group(1) if fr else ''},{to.group(2) if to else ''},{to.group(1) if to else ''},{' '.join(txt.split())}")

for y, sid in GUIDES.items():
    r = PdfReader(io.BytesIO(get(sid, 2)))
    print(f"GUIDE,{y},{sid},{len(r.pages)}")
    for i, p in enumerate(r.pages):
        t = p.extract_text() or ""
        if re.search(r"人\s*件\s*費\s*の\s*状\s*況", t) or re.search(r"表\s*１６|16\s*表", t):
            for line in t.splitlines():
                if line.strip():
                    print(f"G,{y},{i},{line.strip()}")
