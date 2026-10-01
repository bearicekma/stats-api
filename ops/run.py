<<<<<<< HEAD
# 目的：地方財政状況調査（都道府県分）表16（人件費の状況）の様式から、項目番号ごとの見出しを組み立てる材料を出力する（読み取りのみ）
# 内容：決算年度2020〜2024の様式（Excel）の表16シートについて、セルの文字（数式の結果を含む）と図形の文字を「行,列,文字」で出力する

import io
import re
import subprocess
import sys
import zipfile

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "openpyxl"], check=True)
import httpx
import openpyxl

URL = "https://www.e-stat.go.jp/stat-search/file-download?statInfId={}&fileKind=0"
FORMS = {2020: "000032188822", 2021: "000040044816", 2022: "000040171230", 2023: "000040231574", 2024: "000040374254"}

for y, sid in FORMS.items():
    b = httpx.get(URL.format(sid), timeout=300, follow_redirects=True).content
    ws = openpyxl.load_workbook(io.BytesIO(b), data_only=True)["16"]
    for row in ws.iter_rows():
        for c in row:
            if c.value is not None and str(c.value).strip():
                print(f"C,{y},{c.row - 1},{c.column - 1},{' '.join(str(c.value).split())}")
    z = zipfile.ZipFile(io.BytesIO(b))
    wb = z.read("xl/workbook.xml").decode("utf-8")
    rels = z.read("xl/_rels/workbook.xml.rels").decode("utf-8")
    rid = dict(re.findall(r'<sheet[^>]*name="([^"]+)"[^>]*r:id="([^"]+)"', wb))
    tgt = {i: t for i, t in re.findall(r'<Relationship[^>]*Id="([^"]+)"[^>]*Target="([^"]+)"', rels)}
    tgt.update({i: t for t, i in re.findall(r'<Relationship[^>]*Target="([^"]+)"[^>]*Id="([^"]+)"', rels)})
    sheet = "xl/" + tgt[rid["16"]].split("xl/")[-1].lstrip("/")
    srels = sheet.replace("worksheets/", "worksheets/_rels/") + ".rels"
    for d in re.findall(r'Target="([^"]*drawing[^"]*)"', z.read(srels).decode("utf-8")):
        dx = z.read("xl/drawings/" + d.split("/")[-1]).decode("utf-8")
        for anc in re.findall(r"<xdr:(?:twoCellAnchor|oneCellAnchor)[\s\S]*?</xdr:(?:twoCellAnchor|oneCellAnchor)>", dx):
            fr = re.search(r"<xdr:from><xdr:col>(\d+)</xdr:col>[\s\S]*?<xdr:row>(\d+)</xdr:row>", anc)
            to = re.search(r"<xdr:to><xdr:col>(\d+)</xdr:col>[\s\S]*?<xdr:row>(\d+)</xdr:row>", anc)
            txt = "".join(re.findall(r"<a:t>([^<]*)</a:t>", anc))
            if txt.strip() and fr:
                print(f"T,{y},{fr.group(2)},{fr.group(1)},{to.group(2) if to else fr.group(2)},{to.group(1) if to else fr.group(1)},{' '.join(txt.split())}")
=======
# 目的：ハローワーク夜間収集の実行回数を 4回（20:00〜20:45）→ 8回（20:00〜21:45、15分おき）に増やす
# 内容：Cloud Scheduler のジョブ hellowork-collect のスケジュールを gcloud で更新し、更新後の設定を表示する
#       （サービスアカウントに Cloud Scheduler の権限がなければ失敗する → その場合は Colab から更新する）

import os
import subprocess

key = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
run = lambda *args: print(subprocess.run(["gcloud", *args], capture_output=True, text=True).__getattribute__("stdout") or "", end="")

subprocess.run(["gcloud", "auth", "activate-service-account", f"--key-file={key}", "--quiet"], check=True, capture_output=True)
r = subprocess.run(["gcloud", "scheduler", "jobs", "update", "http", "hellowork-collect",
                    "--project=stats-api-491107", "--location=asia-northeast1",
                    "--schedule=0,15,30,45 20-21 * * *", "--time-zone=Asia/Tokyo", "--quiet"],
                   capture_output=True, text=True)
print("update exit:", r.returncode)
print((r.stderr or "").strip()[-600:])
d = subprocess.run(["gcloud", "scheduler", "jobs", "describe", "hellowork-collect", "--project=stats-api-491107",
                    "--location=asia-northeast1", "--format=value(schedule,timeZone,state,attemptDeadline,retryConfig.retryCount)"],
                   capture_output=True, text=True)
print("describe:", d.stdout.strip(), (d.stderr or "").strip()[-300:])
>>>>>>> origin/main
