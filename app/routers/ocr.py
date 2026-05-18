# 汎用OCRエンドポイント（Cloud Vision API）
#  GET  /ocr         : アップロード用WebUI(HTML)を返す
#  POST /ocr         : PDF/画像を受け取りOCR、JSONを返す
#  POST /ocr/to_xlsx : OCR結果JSONをExcel(.xlsx)に変換して返す（Vision再実行なし）

import io
from pathlib           import Path
from datetime          import datetime
from fastapi           import APIRouter, UploadFile, File, Body
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
import pandas as pd

router = APIRouter(tags=["OCR"])

# ---- 設定値 ----
MAX_BYTES      = 10 * 1024 * 1024          # アップロード上限 10MB
MAX_PAGES      = 20                        # PDF最大ページ数（Cloud Runタイムアウト対策）
PDF_DPI        = 300                       # PDFページ画像化のDPI
LOW_CONF       = 0.85                      # これ未満を「低信頼ページ」とする
TEXT_LAYER_MIN = 20                        # この文字数以上抽出できれば埋め込みテキスト層とみなす
ALLOWED_EXT    = {".pdf", ".png", ".jpg", ".jpeg"}

OCR_HTML = Path(__file__).parent.parent / "templates" / "ocr.html"

# Visionクライアントは遅延生成（importや認証失敗でアプリ全体が落ちないように）
_vision_client = None


def _ocr_image(content: bytes):
    # 画像バイト列をVision APIのDOCUMENT_TEXT_DETECTIONでOCR
    # 戻り値: (テキスト, ページ信頼度 or None)
    global _vision_client
    from google.cloud import vision
    if _vision_client is None:
        _vision_client = vision.ImageAnnotatorClient()

    image = vision.Image(content=content)
    ctx   = vision.ImageContext(language_hints=["ja", "en"])  # 和欧混在想定
    resp  = _vision_client.document_text_detection(image=image, image_context=ctx)
    if resp.error.message:
        raise RuntimeError(resp.error.message)

    fa   = resp.full_text_annotation
    text = fa.text if fa else ""
    conf = None
    if fa and fa.pages:
        c = fa.pages[0].confidence
        conf = round(float(c), 4) if c else None
    return text, conf


@router.get("/ocr", response_class=HTMLResponse, summary="OCR WebUI")
def ocr_page():
    # アップロード用のWebUIページ（/guideと同じ静的HTML配信パターン）
    return HTMLResponse(OCR_HTML.read_text(encoding="utf-8"))


@router.post("/ocr", summary="汎用OCR実行")
async def ocr_run(file: UploadFile = File(...)):
    """
    PDF/画像をOCRしてテキストを返します（個人利用前提・認証なし）。

    対応形式:
    - .pdf / .png / .jpg / .jpeg（上限10MB、PDFは最大20ページ）

    処理ロジック:
    - PDF: ページに埋め込みテキスト層があれば pdfplumber で抽出（無料・高精度）
    - テキスト層が無いページのみ Cloud Vision でOCR（課金ページ単位で節約）
    - 画像: そのまま Cloud Vision でOCR

    レスポンスフィールド:
    - filename (string) アップロードファイル名
    - page_count (int) 処理ページ数
    - text (string) 全ページ連結テキスト
    - pages (array) ページ毎: page(int) / text(string) / source(string: pdf_text|ocr) / confidence(float|null)
    - low_confidence_pages (array[int]) 信頼度が0.85未満のページ番号（要目視確認）
    """
    # 拡張子チェック
    name = (file.filename or "").lower()
    ext  = "." + name.rsplit(".", 1)[-1] if "." in name else ""
    if ext not in ALLOWED_EXT:
        return JSONResponse(status_code=400, content={
            "error": f"非対応の形式です: {ext or '不明'}",
            "hint":  "対応形式: .pdf / .png / .jpg / .jpeg",
        })

    data = await file.read()
    if len(data) > MAX_BYTES:
        return JSONResponse(status_code=400, content={
            "error": f"ファイルが大きすぎます（{len(data)//1024//1024}MB）",
            "hint":  "上限は10MBです",
        })

    pages_out = []
    try:
        if ext == ".pdf":
            import pdfplumber, fitz  # fitz = PyMuPDF
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                n = len(pdf.pages)
                if n > MAX_PAGES:
                    return JSONResponse(status_code=400, content={
                        "error": f"ページ数が上限を超えています（{n}ページ）",
                        "hint":  f"上限は{MAX_PAGES}ページです",
                    })
                fdoc = fitz.open(stream=data, filetype="pdf")
                for i in range(n):
                    # まず埋め込みテキスト層を試す（無料・高精度）
                    ptxt = (pdf.pages[i].extract_text() or "").strip()
                    if len(ptxt) >= TEXT_LAYER_MIN:
                        pages_out.append({"page": i + 1, "text": ptxt,
                                          "source": "pdf_text", "confidence": None})
                    else:
                        # 画像ページのみVisionでOCR
                        pix = fdoc[i].get_pixmap(dpi=PDF_DPI)
                        t, c = _ocr_image(pix.tobytes("png"))
                        pages_out.append({"page": i + 1, "text": t,
                                          "source": "ocr", "confidence": c})
                fdoc.close()
        else:
            # 画像はそのままOCR
            t, c = _ocr_image(data)
            pages_out = [{"page": 1, "text": t, "source": "ocr", "confidence": c}]
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error": "OCR処理に失敗しました",
            "detail": str(e),
        })

    combined = "\n".join(p["text"] for p in pages_out)
    low_conf = [p["page"] for p in pages_out
                if p["source"] == "ocr" and p["confidence"] is not None
                and p["confidence"] < LOW_CONF]

    return {
        "filename":             file.filename,
        "page_count":           len(pages_out),
        "text":                 combined,
        "pages":                pages_out,
        "low_confidence_pages": low_conf,
        "updated_at":           str(datetime.now()),
    }


@router.post("/ocr/to_xlsx", summary="OCR結果をExcel変換")
def ocr_to_xlsx(payload: dict = Body(...)):
    # POST /ocr のレスポンスJSONをそのまま受け取り、xlsxに変換して返す
    # （Visionを再実行しないので追加コストなし）
    pages = payload.get("pages", [])
    df = pd.DataFrame(pages, columns=["page", "source", "confidence", "text"])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="OCR")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="ocr_result.xlsx"'},
    )
