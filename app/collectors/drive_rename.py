# Google Drive PDF 自動リネーム処理
#  - 個人Drive(マイドライブ)の対象フォルダ内PDFを走査
#  - 数日以内アップロード & 未処理(YYYYMMDD形式)のものを抽出
#  - PDFを直接Geminiに渡して見出しを生成し「YYYYMMDD_見出し」にリネーム
#  - 認証は OAuth 2.0 ユーザー委任(refresh_token)、値は環境変数から取得

import os
import re
import io
from datetime import datetime, timezone, timedelta

# ---- 設定値 ----
TARGET_FOLDER_ID = "141cGbdt8MalPRPP15tHjlED7DlZd24z9"  # 対象フォルダ
RECENT_DAYS      = 3            # アップロードからこの日数以内のみ対象
MAX_HEADLINES    = 3            # 連結する見出しの最大本数
MAX_NAME_LEN     = 100          # ファイル名(日付含む)のおおよその上限
GEMINI_MODEL     = "gemini-2.5-flash"

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
            fields="nextPageToken, files(id, name, createdTime, size)",
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


def _generate_headline(pdf_bytes):
    # PDFを直接Geminiに渡して主要見出しを生成する
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    prompt = (
        "これは日本経済新聞の紙面PDFです。"
        f"この紙面に含まれる主要な記事の見出しを、重要な順に最大{MAX_HEADLINES}本、"
        "それぞれ簡潔に作ってください。\n"
        "条件:\n"
        "- 各見出しは20文字程度まで\n"
        "- 記号・改行・番号は付けない\n"
        "- 1行に1見出しだけを出力（説明文は不要）"
    )

    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
            prompt,
        ],
    )
    text = (resp.text or "").strip()
    if not text:
        return None

    # 行ごとに分解→先頭の記号/番号を除去→最大本数で「・」連結
    lines = [ln.strip(" 　・-*0123456789.") for ln in text.splitlines() if ln.strip()]
    lines = [ln for ln in lines if ln][:MAX_HEADLINES]
    if not lines:
        return None
    return "・".join(lines)


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
        # name内のシングルクォートはエスケープ
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

        try:
            pdf_bytes = _download_pdf_bytes(service, f["id"])
            headline = _generate_headline(pdf_bytes)
            if not headline:
                failed.append({"name": name, "reason": "見出し生成失敗"})
                continue

            new_base = f"{date_part}_{_sanitize(headline)}"[:MAX_NAME_LEN]
            new_name = _unique_name(service, new_base)

            service.files().update(
                fileId=f["id"],
                body={"name": new_name},
            ).execute()
            renamed.append({"from": name, "to": new_name})
        except Exception as e:
            failed.append({"name": name, "reason": str(e)})

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
            "renamed":      _NAME_PATTERN.match(base) is None,  # 未処理パターンに合致しない=処理済み
        })
    return out
