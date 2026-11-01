# スキャンPDFの軽量化＋不可視テキスト層の埋め込み（PyMuPDF=fitz と Pillow を使用）
#  - is_scanned_pdf       : 画像主体でテキスト層が無い「スキャンPDF」かを判定
#  - compress_with_text   : 【現行】ページごとに自動判定して圧縮し、全文を不可視層として重ねる
#                             文字主体のページ → グレースケールで描き直す（地色を白に飛ばし文字を濃くしてJPEG化）
#                             写真入りのページ → 元の画像をその場で再圧縮（解像度上限＋JPEG画質）
#                             それでも元より大きければ → 元のまま全文だけ重ねる
#  - optimize_scanned_pdf : 【旧方式・未使用】各ページを画像化し直してJPEG化（縮まないことが多い）
#  - verify_text_layer    : 上書き前チェック（テキストが抽出できるか）

import io
import fitz  # PyMuPDF

_TEXT_THRESHOLD = 50  # 抽出テキストがこの文字数未満なら「テキスト層なし」とみなす

# ---- compress_with_text の設定 ----
TEXT_MAX_DPI      = 200    # 文字ページ：元画像の解像度で描き直す（ただしこの解像度が上限）
TEXT_MIN_DPI      = 100    #   …元画像の解像度が分からない・低すぎる場合の下限
TEXT_WHITE_POINT  = 225    # 文字ページ：これより明るい画素（紙の地色）は真っ白にする
TEXT_BLACK_POINT  = 40     #   …これより暗い画素は真っ黒にし、その間を引き伸ばして文字を濃くする
TEXT_JPEG_QUALITY = 60     # 文字ページ：JPEG画質
CLASSIFY_DPI      = 100    # ページ判定用に描画する解像度
PHOTO_AREA_RATIO  = 0.01   # 中間調または彩色の「面」がページのこの割合を超えたら写真入りとみなす
REWRITE_DPI_OVER  = 220    # 写真入りページ：これを超える解像度の画像だけ…
REWRITE_DPI_TO    = 200    #   …この解像度に下げる
REWRITE_QUALITY   = 75     # 写真入りページ：JPEG画質


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
                         dpi: int = 250, quality: int = 88) -> bytes:
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


def _add_invisible_text(doc, text: str):
    # 全文をページ数で均等に分け、各ページに不可視テキスト(描画モード3)として重ねる
    text = (text or "").strip()
    if not text or doc.page_count == 0:
        return
    chunk = max(1, -(-len(text) // doc.page_count))  # 切り上げ分割
    for i, page in enumerate(doc):
        part = text[i * chunk:(i + 1) * chunk]
        if part:
            page.insert_textbox(page.rect, part,
                                fontsize=_pick_fontsize(len(part)),
                                fontname="japan", render_mode=3)


def _is_photo_page(page) -> bool:
    # 低解像度で描画し、中間調(灰色)または彩色の「面」があるかで写真入りページかを判定。
    # 細い文字の縁にも中間調は出るため、収縮フィルタで細い部分を消してから面積を測る。
    from PIL import Image, ImageFilter
    pix = page.get_pixmap(dpi=CLASSIFY_DPI)
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    n = img.width * img.height
    mid = img.convert("L").point(lambda v: 255 if 60 < v < 200 else 0).filter(ImageFilter.MinFilter(5))
    col = img.convert("HSV").getchannel("S").point(lambda s: 255 if s > 60 else 0).filter(ImageFilter.MinFilter(5))
    return (mid.histogram()[255] / n > PHOTO_AREA_RATIO) or (col.histogram()[255] / n > PHOTO_AREA_RATIO)


def _native_dpi(page) -> int:
    # ページ内の画像の実解像度(dpi)を推定（最も高いもの）。分からなければ上限値
    best = 0
    for info in page.get_image_info():
        w_pt = info["bbox"][2] - info["bbox"][0]
        if w_pt > 0 and info.get("width"):
            best = max(best, info["width"] / (w_pt / 72))
    if best <= 0:
        return TEXT_MAX_DPI
    return int(max(TEXT_MIN_DPI, min(TEXT_MAX_DPI, round(best))))


def _text_gray_jpeg(page) -> bytes:
    # 文字主体のページを元画像の解像度(上限あり)でグレー描画し、
    # 地色を白に飛ばし・文字を濃くしてからJPEG化する（輪郭の濃淡を残すのでかすれない）
    from PIL import Image
    pix = page.get_pixmap(dpi=_native_dpi(page), colorspace=fitz.csGRAY)
    img = Image.frombytes("L", (pix.width, pix.height), pix.samples)
    span = TEXT_WHITE_POINT - TEXT_BLACK_POINT
    img = img.point(lambda v: 255 if v > TEXT_WHITE_POINT else max(0, int((v - TEXT_BLACK_POINT) * 255 / span)))
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=TEXT_JPEG_QUALITY, optimize=True)
    return buf.getvalue()


def compress_with_text(pdf_bytes: bytes, text: str):
    # ページごとに自動判定して圧縮し、全文を不可視テキスト層として重ねる。
    # 戻り値: (新しいPDFバイト列, 方式の説明)  例: "グレー3p・写真1p" / "元のまま文字のみ（縮まないため）"
    src = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        photo_flags = [_is_photo_page(p) for p in src]

        # 文字ページ用：再圧縮で画質が落ちる前の元画像からグレーJPEGを作っておく
        gray_jpegs = {i: _text_gray_jpeg(p) for i, p in enumerate(src) if not photo_flags[i]}

        # 写真入りページ用：元の画像をその場で再圧縮（解像度上限＋JPEG画質）
        src.rewrite_images(dpi_threshold=REWRITE_DPI_OVER, dpi_target=REWRITE_DPI_TO,
                           quality=REWRITE_QUALITY)

        out = fitz.open()
        try:
            for i, page in enumerate(src):
                if photo_flags[i]:
                    out.insert_pdf(src, from_page=i, to_page=i)   # 再圧縮済みのページをそのまま使う
                else:
                    newp = out.new_page(width=page.rect.width, height=page.rect.height)
                    newp.insert_image(newp.rect, stream=gray_jpegs[i])
            _add_invisible_text(out, text)
            compressed = out.tobytes(deflate=True, garbage=4)
        finally:
            out.close()
    finally:
        src.close()

    n_photo = sum(photo_flags)
    n_gray = len(photo_flags) - n_photo
    mode = "・".join(s for s in (f"グレー{n_gray}p" if n_gray else "", f"写真{n_photo}p" if n_photo else "") if s)

    if len(compressed) < len(pdf_bytes):
        return compressed, mode

    # 元より大きくなった → 画像は元のまま、全文だけ重ねる（検索はできるようにする）
    orig = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        _add_invisible_text(orig, text)
        return orig.tobytes(deflate=True, garbage=4), "元のまま文字のみ（縮まないため）"
    finally:
        orig.close()
