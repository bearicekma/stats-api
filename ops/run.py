# 目的：2026/11/2 朝の定期実行で「白黒2値」で圧縮され文字がかすれたPDFを、グレースケール方式で処理し直す
# 内容：対象フォルダで optimized=1 かつ 11/1 21:30(JST)以降に更新されたPDFを探し、
#       ① 元の版（Driveの版の履歴の最初の版）をダウンロード
#       ② 今の版に埋め込み済みの全文テキストを取り出す（Geminiは使わない＝無料枠を消費しない）
#       ③ 元の版 ＋ 全文 から compress_with_text（グレースケール方式）で作り直して差し替える
#       ログは公開されるため、ファイル名・IDは出さずサイズと方式だけ表示する

import io
import fitz
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from app.collectors import drive_rename as dr
from app.pdf_optimize import compress_with_text, verify_text_layer

SINCE = "2026-11-01T12:30:00Z"   # 白黒2値版のデプロイ(11/1 21:30 JST頃)以降


def download(req):
    # files().get_media / revisions().get_media のリクエストをバイト列にする
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue()


service = dr._get_drive_service()
q = (f"'{dr.TARGET_FOLDER_ID}' in parents and trashed = false and mimeType = 'application/pdf' "
     f"and modifiedTime > '{SINCE}' and appProperties has {{ key='optimized' and value='1' }}")
files = service.files().list(q=q, fields="files(id, name, size)", pageSize=100).execute().get("files", [])
print(f"対象: {len(files)}件")

for n, f in enumerate(files, 1):
    try:
        revs = service.revisions().list(fileId=f["id"], fields="revisions(id, modifiedTime, size)").execute().get("revisions", [])
        if len(revs) < 2:
            print(f"[{n}] スキップ：元の版が見つからない（版 {len(revs)}件）")
            continue
        orig = download(service.revisions().get_media(fileId=f["id"], revisionId=revs[0]["id"]))
        cur = download(service.files().get_media(fileId=f["id"]))

        # 今の版に埋め込まれている全文（不可視テキスト層）を取り出す。折り返しの改行は除く
        doc = fitz.open(stream=cur, filetype="pdf")
        text = "".join(p.get_text("text") for p in doc).replace("\n", "")
        doc.close()
        if len(text) < dr.OPT_MIN_TEXT_CHARS:
            print(f"[{n}] スキップ：埋め込み全文を取り出せない（{len(text)}文字）")
            continue

        new_bytes, mode = compress_with_text(orig, text)
        if not verify_text_layer(new_bytes):
            print(f"[{n}] スキップ：検証NG")
            continue

        media = MediaIoBaseUpload(io.BytesIO(new_bytes), mimetype="application/pdf", resumable=True)
        service.files().update(fileId=f["id"], body={"appProperties": {"renamed": "1", "optimized": "1"}},
                               media_body=media).execute()
        print(f"[{n}] 差し替え：元 {len(orig):,} → 白黒2値 {len(cur):,} → 新 {len(new_bytes):,} bytes"
              f"（元の{len(new_bytes)/len(orig):.0%}） {mode} / 全文 {len(text)}文字")
    except Exception as e:
        print(f"[{n}] 失敗：{type(e).__name__}: {str(e)[:200]}")
