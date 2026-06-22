# Google Drive PDF 自動リネーム処理
#  - 個人Drive(マイドライブ)の対象フォルダ内PDFを走査
#  - 数日以内アップロード & 未処理(YYYYMMDD形式)のものを抽出
#  - PDFを直接Geminiに渡して見出しを生成し「YYYYMMDD_見出し」にリネーム
#  - 認証は OAuth 2.0 ユーザー委任(refresh_token)、値は環境変数から取得
#  - Geminiは候補モデルを順に試すフォールバック方式（廃止/日次上限は即次へ）

import os
import re
import io
import time
from datetime import datetime, timezone, timedelta

# スキャンPDFの軽量化＋不可視テキスト層（PyMuPDFのみ・依存追加なし）
from app.pdf_optimize import is_scanned_pdf, optimize_scanned_pdf, verify_text_layer

# ---- 設定値 ----
TARGET_FOLDER_ID  = "141cGbdt8MalPRPP15tHjlED7DlZd24z9"  # 対象フォルダ
RECENT_DAYS       = 3       # アップロードからこの日数以内のみ対象
MAX_HEADLINES     = 3       # 連結する見出しの最大本数
MAX_NAME_LEN      = 100     # ファイル名(日付含む)のおおよその上限
MAX_FILES_PER_RUN = 20      # 1回のrunで処理する最大件数（無料枠20/日の安全弁）

# Geminiの候補モデル（無料枠で使える順。先頭から試す）
GEMINI_MODELS  = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-3-flash"]
MAX_RPM_RETRY  = 2          # 分次(RPM)一時超過時のリトライ回数
RPM_RETRY_WAIT = 8          # リトライ前の待機秒数

# 未処理ファイル名の判定：YYYYMMDD または YYYYMMDD (n)
_NAME_PATTERN = re.compile(r"^(\d{8})(?:\s*\(\d+\))?$")


def _get_drive_service():
    # 環境変数のOAuth資格情報からDriveクライアントを構築して返す
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    creds = Credentials(
        token=None,  # アクセストークンは無し→refresh_tokenから自動取得
        refresh_token=os.environ["DRIVE_OAUTH_REFRESH_TOKEN"],
        client_id=os.environ["DRIVE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["DRIVE_OAUTH_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_target_pdfs(service):
    # フォルダ内のPDFを全件取得
    q = (
        f"'{TARGET_FOLDER_ID}' in parents "
        f"and mimeType = 'application/pdf' and trashed = false"
    )
    files = []
    page_token = None
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, createdTime, size, appProperties)",
            pageSize=100,
            pageToken=page_token,
            orderBy="createdTime desc",
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def _is_unprocessed(name):
    # 拡張子を除いた名前が YYYYMMDD / YYYYMMDD (n) なら未処理→日付部分を返す
    base = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
    m = _NAME_PATTERN.match(base)
    return (m.group(1) if m else None)


def _within_recent(created_time):
    # createdTime(RFC3339, UTC)が RECENT_DAYS 以内か
    dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
    threshold = datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)
    return dt >= threshold


def _download_pdf_bytes(service, file_id):
    # PDF本体をバイト列でダウンロード
    from googleapiclient.http import MediaIoBaseDownload
    buf = io.BytesIO()
    req = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()


def _generate_headline_and_text(pdf_bytes):
    # PDFを直接Geminiに渡し、「見出し（最大MAX_HEADLINES本）」と「本文全文」を
    # 1回の呼び出しでJSON取得（OCR用の追加呼び出しをしない＝無料枠を温存）。
    import json
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    prompt = (
        "これは新聞紙面のPDFです。次の2つをJSONで返してください。\n"
        f"1) headlines: 主要な記事の見出しを重要な順に最大{MAX_HEADLINES}本の配列。"
        "各20文字程度、記号・番号は付けない。\n"
        "2) text: 紙面の本文をできるだけ忠実に書き起こした全文（検索用）。\n"
        "縦書きの段組みです。各段を上から下へ読み切り、右の段から\n"
        "左の段の順に1段ずつ処理し、段の途中で隣の段に移らないこと。\n"
        '出力は {"headlines": [...], "text": "..."} のJSONのみ。'
    )
    parts = [
        types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        prompt,
    ]
    config = types.GenerateContentConfig(response_mime_type="application/json")

    last_error = None

    for model in GEMINI_MODELS:
        attempt = 0
        while True:
            try:
                resp = client.models.generate_content(
                    model=model, contents=parts, config=config
                )
                raw = (resp.text or "").strip()
                if not raw:
                    last_error = f"{model}: 空応答"
                    break
                raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
                data = json.loads(raw)

                heads = [str(h).strip(" 　・-*")
                         for h in (data.get("headlines") or []) if str(h).strip()]
                heads = [h for h in heads if h][:MAX_HEADLINES]
                if not heads:
                    last_error = f"{model}: 見出し抽出できず"
                    break
                headline  = "・".join(heads)
                full_text = str(data.get("text") or "").strip()
                return headline, full_text

            except Exception as e:
                msg = str(e)
                last_error = f"{model}: {msg}"
                if "429" not in msg and "RESOURCE_EXHAUSTED" not in msg:
                    break
                if "limit: 0" in msg or "limit:0" in msg:
                    break
                if "PerDay" in msg:
                    break
                if attempt < MAX_RPM_RETRY:
                    attempt += 1
                    time.sleep(RPM_RETRY_WAIT)
                    continue
                else:
                    break

    raise RuntimeError(last_error or "見出し・全文の生成失敗（全候補）")


def _sanitize(title):
    # ファイル名に使えない文字を除去
    title = re.sub(r'[\\/:*?"<>|]', "", title)
    title = re.sub(r"[\r\n\t]", "", title)
    return title.strip()


def _unique_name(service, base):
    # 同名が既にあれば (2)(3)... を付けて衝突回避
    candidate = base
    n = 2
    while True:
        safe = candidate.replace("'", "\\'")
        q = (
            f"'{TARGET_FOLDER_ID}' in parents and trashed = false "
            f"and name = '{safe}'"
        )
        resp = service.files().list(q=q, fields="files(id)", pageSize=1).execute()
        if not resp.get("files"):
            return candidate
        candidate = f"{base}({n})"
        n += 1


def run_drive_rename():
    # メイン処理：対象PDFを走査してリネーム。結果をdictで返す
    service = _get_drive_service()
    files = _list_target_pdfs(service)

    renamed, skipped, failed = [], 0, []
    processed = 0  # 実際にAPIを消費した件数（成功・失敗とも）

    for f in files:
        name = f["name"]

        # 条件1: アップロードが数日以内
        if not _within_recent(f["createdTime"]):
            skipped += 1
            continue

        # 条件2: 未処理（YYYYMMDD形式）か
        date_part = _is_unprocessed(name)
        if not date_part:
            skipped += 1
            continue

        # 既に最適化済み（appProperties）なら念のためスキップ（二重ガード）
        if (f.get("appProperties") or {}).get("optimized") == "v1":
            skipped += 1
            continue

        # 件数上限に達したら、残りは未処理のまま次回へ
        if processed >= MAX_FILES_PER_RUN:
            skipped += 1
            continue

        try:
            pdf_bytes = _download_pdf_bytes(service, f["id"])
            headline, full_text = _generate_headline_and_text(pdf_bytes)
            if not headline:
                failed.append({"name": name, "reason": "見出し生成失敗"})
                processed += 1
                continue

            new_base = f"{date_part}_{_sanitize(headline)}"[:MAX_NAME_LEN]
            new_name = _unique_name(service, new_base)

            # スキャンPDFのみ：軽量化＋不可視テキスト層を埋め込み、
            # 検証OKなら「中身＋名前＋マーカー」を一括で上書き更新（ファイルIDは不変）
            if is_scanned_pdf(pdf_bytes):
                optimized = optimize_scanned_pdf(pdf_bytes, full_text)
                if verify_text_layer(optimized):
                    from googleapiclient.http import MediaIoBaseUpload
                    media = MediaIoBaseUpload(
                        io.BytesIO(optimized),
                        mimetype="application/pdf", resumable=False,
                    )
                    service.files().update(
                        fileId=f["id"],
                        body={"name": new_name, "appProperties": {"optimized": "v1"}},
                        media_body=media,
                    ).execute()
                    renamed.append({"from": name, "to": new_name,
                                    "optimized": True, "bytes": len(optimized)})
                else:
                    # 検証失敗→中身は触らず名前だけ変更（原本を壊さない）
                    service.files().update(
                        fileId=f["id"], body={"name": new_name}
                    ).execute()
                    renamed.append({"from": name, "to": new_name, "optimized": False})
            else:
                # 非スキャン（テキストPDF等）→ 従来どおり名前だけ変更（画質を保つ）
                service.files().update(
                    fileId=f["id"], body={"name": new_name}
                ).execute()
                renamed.append({"from": name, "to": new_name, "optimized": False})

            processed += 1
        except Exception as e:
            failed.append({"name": name, "reason": str(e)})
            processed += 1  # 失敗もAPI消費とみなしカウント

    return {"renamed": renamed, "skipped": skipped, "failed": failed}


def list_drive_pdfs():
    # 一覧表示用：フォルダ内PDFをJSON向けに整形して返す
    service = _get_drive_service()
    files = _list_target_pdfs(service)
    out = []
    for f in files:
        name = f["name"]
        base = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
        out.append({
            "id":           f["id"],
            "name":         name,
            "created_time": f["createdTime"],
            "size":         int(f["size"]) if f.get("size") else None,
            "renamed":      _NAME_PATTERN.match(base) is None,
        })
    return out
