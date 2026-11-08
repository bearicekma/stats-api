# 目的：11/8 の再処理で「検証NG（テキスト層を抽出できず）」となり、リネームのみだったPDFを処理し直す
# 内容：対象フォルダで renamed=1 だが optimized=1 が無く、11/8 09:55(JST)以降に更新されたPDFを探し、
#       Geminiで全文を取り直して（1件につき無料枠1回）、文字サイズ自動調整版で圧縮＋全文埋め込みして差し替える
#       ファイル名はそのまま。ログは公開されるため、ファイル名・IDは出さない

from app.collectors import drive_rename as dr

SINCE = "2026-11-08T00:55:00Z"

service = dr._get_drive_service()
q = (f"'{dr.TARGET_FOLDER_ID}' in parents and trashed = false and mimeType = 'application/pdf' "
     f"and modifiedTime > '{SINCE}' and appProperties has {{ key='renamed' and value='1' }}")
files = service.files().list(q=q, fields="files(id, name, appProperties)", pageSize=100).execute().get("files", [])
files = [f for f in files if (f.get("appProperties") or {}).get("optimized") != "1"]
print(f"対象: {len(files)}件")

for n, f in enumerate(files, 1):
    try:
        pdf = dr._download_pdf_bytes(service, f["id"])
        _, text, _ = dr._analyze_pdf(pdf)
        new_bytes, note = dr._try_optimize(pdf, text)
        if not new_bytes:
            print(f"[{n}] 差し替えなし：{note}（全文 {len(text)}文字）")
            continue
        fail = dr._update_file(service, f["id"], f["name"], new_bytes)
        print(f"[{n}] {'失敗: ' + fail if fail else '差し替え'}：{len(pdf):,} → {len(new_bytes):,} bytes {note}（全文 {len(text)}文字）")
    except Exception as e:
        print(f"[{n}] 失敗：{type(e).__name__}: {str(e)[:200]}")
