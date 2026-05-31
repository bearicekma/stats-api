# Google Drive PDF 自動リネーム エンドポイント
#  POST /drive_rename/run   : 対象フォルダのPDFを走査してリネーム実行
#  GET  /drive_rename/files : フォルダ内PDF一覧をJSONで返す
#  GET  /drive_rename/view  : 年月別グルーピングのHTML一覧ページ

import asyncio
from pathlib           import Path
from datetime          import datetime
from fastapi           import APIRouter
from fastapi.responses import JSONResponse, HTMLResponse

from app.collectors.drive_rename import run_drive_rename, list_drive_pdfs

router = APIRouter(prefix="/drive_rename", tags=["Drive Rename"])

# HTMLテンプレートのパス（/ocr, /guide と同じ静的配信パターン）
FILES_HTML = Path(__file__).parent.parent / "templates" / "drive_files.html"


@router.post("/run", summary="Drive PDF 自動リネーム実行")
async def drive_rename_run():
    """
    対象フォルダ内のPDFを走査し、未処理ファイルをリネームします。

    対象条件:
    - アップロードが数日以内（既定3日）
    - ファイル名が YYYYMMDD または YYYYMMDD (n) 形式（未処理）

    処理:
    - PDFを直接Geminiに渡して主要見出しを生成（最大3本を「・」連結）
    - 「YYYYMMDD_見出し」にリネーム（名前衝突は連番で回避）
    - 失敗があればGmailで通知

    レスポンス:
    - renamed (array) リネームしたファイル: from(str) / to(str)
    - skipped (int)   対象外でスキップした件数
    - failed  (array) 失敗ファイル: name(str) / reason(str)
    """
    result = await asyncio.to_thread(run_drive_rename)

    # 失敗時のみGmail通知（既存のsend_gmailを利用）
    if result["failed"]:
        from app.notifier import send_gmail
        lines = "\n".join(f"・{x['name']}: {x['reason']}" for x in result["failed"])
        send_gmail(
            subject="【Stats API】drive_rename 一部失敗",
            body=(
                f"PDFリネームで {len(result['failed'])} 件失敗しました。\n\n"
                f"{lines}\n\n"
                f"成功: {len(result['renamed'])}件 / スキップ: {result['skipped']}件"
            ),
        )

    return {"updated_at": str(datetime.now()), **result}


@router.get("/files", summary="Drive PDF 一覧取得")
async def drive_rename_files():
    """
    対象フォルダ内のPDF一覧をJSONで返します。

    各要素:
    - id (str)           DriveファイルID
    - name (str)         ファイル名
    - created_time (str) アップロード日時(RFC3339)
    - size (int|null)    バイト数
    - renamed (bool)     リネーム済みか（YYYYMMDD未処理形式でなければtrue）
    """
    try:
        files = await asyncio.to_thread(list_drive_pdfs)
        return {"updated_at": str(datetime.now()), "count": len(files), "files": files}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error": "一覧取得に失敗しました",
            "detail": str(e),
        })


@router.get("/view", response_class=HTMLResponse, summary="Drive PDF 一覧ページ")
def drive_rename_view():
    # 年月別グルーピングで閲覧するHTMLページ（ページ側が /files のJSONを取得して描画）
    return HTMLResponse(FILES_HTML.read_text(encoding="utf-8"))
