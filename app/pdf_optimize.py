# スキャンPDFの軽量化＋不可視テキスト層の埋め込み（PyMuPDF=fitz のみ使用・依存追加なし）
#  - is_scanned_pdf       : 画像主体でテキスト層が無い「スキャンPDF」かを判定
#  - optimize_scanned_pdf : 各ページを指定dpiで画像化(=軽量化)し、OCR全文を不可視層として重ねる
#  - verify_text_layer    : 上書き前チェック（テキストが抽出できるか）

import fitz  # PyMuPDF

_TEXT_THRESHOLD = 50  # 抽出テキストがこの文字数未満なら「テキスト層なし」とみなす


def is_scanned_pdf(pdf_bytes: bytes) -> bool:
    # テキストがほぼ無く、画像が1枚以上あればスキャンPDFと判定
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        total_text = sum(len(p.get_text("text").strip()) for p in doc)
        total_imgs = sum(len(p.get_images(full=True)) for p in doc)
    finally:
        doc.close()
    return (total_text < _TEXT_THRESHOLD) and (total_imgs >= 1)


def _pick_fontsize(n_chars: int) -> float:
    # 文字数に応じて1ページに収まりやすいフォントサイズを選ぶ（不可視なので見た目は不問）
    if n_chars <= 3000:  return 6.0
    if n_chars <= 6000:  return 5.0
    if n_chars <= 10000: return 4.0
    return 3.0


def optimize_scanned_pdf(pdf_bytes: bytes, text: str,
                         dpi: int = 200, quality: int = 82) -> bytes:
    # 各ページを画像化してJPEG再エンコード（軽量化）し、OCR全文を不可視テキスト層として重ねる
    src = fitz.open(stream=pdf_bytes, filetype="pdf")
    out = fitz.open()
    try:
        text = (text or "").strip()
        n_pages = src.page_count
        chunk = max(1, -(-len(text) // n_pages)) if text else 0  # ページ数で切り上げ分割
        for i, page in enumerate(src):
            # 軽量化：ページを画像化してJPEGで貼り直す
            pix = page.get_pixmap(dpi=dpi)
            jpg = pix.tobytes("jpeg", jpg_quality=quality)
            newp = out.new_page(width=page.rect.width, height=page.rect.height)
            newp.insert_image(newp.rect, stream=jpg)
            # 不可視テキスト層：このページぶんのテキストを描画モード3（不可視）で重ねる
            part = text[i * chunk:(i + 1) * chunk] if text else ""
            if part:
                newp.insert_textbox(newp.rect, part,
                                    fontsize=_pick_fontsize(len(part)),
                                    fontname="japan", render_mode=3)
        return out.tobytes(deflate=True, garbage=4)
    finally:
        src.close()
        out.close()


def verify_text_layer(pdf_bytes: bytes, min_chars: int = 20) -> bool:
    # 上書き前チェック：開けて、テキストが min_chars 以上抽出できればOK
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            got = sum(len(p.get_text("text").strip()) for p in doc)
            ok = doc.page_count >= 1
        finally:
            doc.close()
        return ok and got >= min_chars
    except Exception:
        return False
