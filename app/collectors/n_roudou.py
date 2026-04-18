# 長野労働局「最近の雇用情勢」月次PDFから、受理地別・産業別新規求人数を収集する
# PDFのP2④「産業別新規求人の状況」をパースし、大分類レベルでparquetに保存する
# GCS保存先: n_roudou/juri_sangyo/data.parquet

from bs4              import BeautifulSoup
from datetime         import date
from google.cloud     import storage
from io               import BytesIO
import httpx
import os
import pandas as pd
import pdfplumber
import re
import tempfile
import unicodedata


# ── 定数 ──────────────────────────────────────────────

BUCKET_NAME   = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH      = "n_roudou/juri_sangyo/data.parquet"
LIST_PAGE_URL = "https://jsite.mhlw.go.jp/nagano-roudoukyoku/jirei_toukei/kyujin_kyushoku/roudoushijyou_jyouhou/koyoujyousei_itiran.html"
SITE_BASE_URL = "https://jsite.mhlw.go.jp"

# 和暦→西暦変換用（令和元年=2019）
REIWA_BASE = 2018

# 抽出対象の大分類: (行番号, カラム側, 産業コード, 産業分類名)
# 検証の結果、表2の行番号は4つの年度PDFで完全に固定されていたため、行番号直接マッピング方式を採用する
TARGET_MAJORS = [
    ( 2, "L", "all",   "全数"),
    ( 2, "R", "G",     "情報通信業"),
    ( 4, "L", "D",     "建設業"),
    ( 4, "R", "H",     "運輸業、郵便業"),
    ( 6, "L", "E",     "製造業"),
    ( 6, "R", "I",     "卸売業、小売業"),
    ( 8, "R", "J",     "金融業、保険業"),
    (10, "R", "K",     "不動産業、物品賃貸業"),
    (12, "R", "M",     "宿泊業、飲食サービス業"),
    (16, "R", "N",     "生活関連サービス業、娯楽業"),
    (20, "R", "O",     "教育、学習支援業"),
    (22, "R", "P",     "医療、福祉"),
    (26, "R", "R",     "サービス業（他に分類されないもの）"),
    (30, "R", "other", "その他の産業"),
]


# ── ユーティリティ関数 ───────────────────────────────────

def _parse_int(value):
    # カンマ付き数値文字列をintに変換する（"14,967" → 14967）
    # 空文字やNoneはNoneを返す
    if value is None or value == "":
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned == "":
            return None
        return int(cleaned)
    except (ValueError, TypeError):
        return None


def _parse_percent(value):
    # 前月比・前年同月比をfloatに変換する
    # ▲→マイナス、カッコ除去、空文字はNone
    if value is None or value == "":
        return None
    try:
        s = str(value).strip()
        # カッコ除去（令和6年度の一部データでカッコ付き表記があったため）
        s = s.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
        # ▲・△をマイナス符号に変換
        s = s.replace("▲", "-").replace("△", "-").strip()
        if s == "" or s == "-":
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def _normalize_industry_name(name):
    # 産業名を正規化する（全角→半角、空白除去、改行除去）
    # 例: "Ｄ 建 設 業" → "D建設業"
    if not name:
        return ""
    normalized = unicodedata.normalize("NFKC", name)
    return normalized.replace(" ", "").replace("　", "").replace("\n", "")


def _parse_publish_date(pdf):
    # PDF1ページ目から公表日を抽出する（例: "令和８年３月31日" → date(2026, 3, 31)）
    try:
        text = pdf.pages[0].extract_text() or ""
        text_norm = unicodedata.normalize("NFKC", text)
        m = re.search(r"令和(\d+)年(\d+)月(\d+)日", text_norm)
        if m:
            return date(REIWA_BASE + int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception as e:
        print(f"  ⚠️ 公表日の抽出に失敗: {e}")
    return None


def _parse_target_year_month(pdf):
    # PDF1ページ目から対象年月を抽出する（例: "令和８年２月分" → (2026, 2)）
    try:
        text = pdf.pages[0].extract_text() or ""
        text_norm = unicodedata.normalize("NFKC", text)
        m = re.search(r"令和(\d+)年(\d+)月分", text_norm)
        if m:
            return (REIWA_BASE + int(m.group(1)), int(m.group(2)))
    except Exception as e:
        print(f"  ⚠️ 対象年月の抽出に失敗: {e}")
    return None


# ── PDFパース関数 ──────────────────────────────────────

def parse_pdf(pdf_url: str) -> list[dict]:
    # 1つのPDF URLから14レコード（大分類14産業分）を抽出して返す
    # 失敗時は空リストを返す

    print(f"  📥 取得中: {pdf_url}")

    try:
        r = httpx.get(pdf_url, timeout=60.0, follow_redirects=True,
                      headers={"Accept-Encoding": "gzip"})
        r.raise_for_status()
    except Exception as e:
        print(f"  ❌ PDF取得失敗: {e}")
        return []

    try:
        with pdfplumber.open(BytesIO(r.content)) as pdf:

            # 対象年月・公表日を抽出する
            ym = _parse_target_year_month(pdf)
            publish_dt = _parse_publish_date(pdf)

            if ym is None:
                print(f"  ❌ 対象年月の抽出に失敗")
                return []

            target_date = date(ym[0], ym[1], 1)

            # 「産業別新規求人の状況」を含むページを検索する
            target_idx = None
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                if "産業別新規求人の状況" in text:
                    target_idx = i
                    break

            if target_idx is None:
                print(f"  ❌ 『産業別新規求人の状況』が見つからない")
                return []

            # 該当ページから表を抽出する
            tables = pdf.pages[target_idx].extract_tables()
            if len(tables) < 2:
                print(f"  ❌ 表が{len(tables)}個のみ（表2が必要）")
                return []

            table2 = tables[1]
            rows   = len(table2)
            cols   = len(table2[0]) if table2 else 0

            # 構造チェック（32行×12列が期待値だが、ズレていてもパース試行する）
            if rows != 32 or cols != 12:
                print(f"  ⚠️ 表2の構造が想定と異なる: {rows}行×{cols}列 → パース試行する")

            # 各大分類をパースする
            records = []
            for row_idx, col_side, code, name in TARGET_MAJORS:

                # 行数不足チェック（パート行＝+1行にアクセスするため）
                if row_idx + 1 >= rows:
                    print(f"  ⚠️ 行{row_idx}にアクセスできない: {code}")
                    continue

                # 左カラム vs 右カラムでセル位置を切り替える
                if col_side == "L":
                    c_name, c_count, c_mom, c_yoy = 0, 2, 4, 5
                else:
                    c_name, c_count, c_mom, c_yoy = 6, 8, 10, 11

                total_row = table2[row_idx]
                part_row  = table2[row_idx + 1]

                # 産業名の妥当性チェック（期待コードと先頭一致しているか）
                raw_name  = total_row[c_name] if c_name < len(total_row) else None
                norm_name = _normalize_industry_name(raw_name)

                if code == "all":
                    ok = norm_name.startswith("全数")
                elif code == "other":
                    ok = norm_name.startswith("その他")
                else:
                    ok = norm_name.startswith(code)

                if not ok:
                    print(f"  ⚠️ 産業名が期待と不一致: 行{row_idx} 期待={code} 実際={norm_name[:20]}")
                    continue

                # 新規求人数セルは '14,967\n5,955' のように全体値とパート値が \n で2値結合されている
                count_cell = total_row[c_count] if c_count < len(total_row) else None
                count_parts = str(count_cell).split("\n") if count_cell else []
                new_job_offers = _parse_int(count_parts[0]) if len(count_parts) >= 1 else None
                uchi_part      = _parse_int(count_parts[1]) if len(count_parts) >= 2 else None

                # 全体の前月比・前年同月比（総行）
                mom_total = _parse_percent(total_row[c_mom] if c_mom < len(total_row) else None)
                yoy_total = _parse_percent(total_row[c_yoy] if c_yoy < len(total_row) else None)

                # パートの前月比・前年同月比（パート行）
                mom_part = _parse_percent(part_row[c_mom] if c_mom < len(part_row) else None)
                yoy_part = _parse_percent(part_row[c_yoy] if c_yoy < len(part_row) else None)

                records.append({
                    "DATE":              target_date,
                    "公表日":             publish_dt,
                    "産業コード":          code,
                    "産業分類":           name,
                    "新規求人数":          new_job_offers,
                    "前月比":             mom_total,
                    "前年同月比":          yoy_total,
                    "うちパート":          uchi_part,
                    "うちパート前月比":      mom_part,
                    "うちパート前年同月比":   yoy_part,
                    "PDF_URL":           pdf_url,
                })

            print(f"  ✅ {len(records)}件抽出（対象月: {target_date}）")
            return records

    except Exception as e:
        print(f"  ❌ パース失敗: {type(e).__name__}: {e}")
        return []


# ── PDF URL一覧取得関数 ──────────────────────────────────

def fetch_pdf_url_list() -> list[str]:
    # 一覧ページをスクレイプし、月次雇用情勢PDFのURL一覧を返す
    # 令和6年度4月分以降のみ対象とする（P2④のフォーマットが安定しているため）

    print(f"📋 一覧ページ取得: {LIST_PAGE_URL}")

    try:
        r = httpx.get(LIST_PAGE_URL, timeout=30.0, follow_redirects=True,
                      headers={"Accept-Encoding": "gzip"})
        r.raise_for_status()
    except Exception as e:
        print(f"❌ 一覧ページ取得失敗: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # 全PDFリンクを取得する
    pdf_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            # 相対URLを絶対URLに変換する
            if href.startswith("http"):
                pdf_urls.append(href)
            elif href.startswith("/"):
                pdf_urls.append(SITE_BASE_URL + href)

    # 重複除去（同一PDFが複数箇所にリンクされるケースがあるため）
    pdf_urls = list(dict.fromkeys(pdf_urls))

    print(f"📋 {len(pdf_urls)}件のPDFリンクを検出")
    return pdf_urls


# ── GCS読み書き関数 ─────────────────────────────────────

def _load_existing() -> pd.DataFrame:
    # 既存parquetをGCSから取得する（存在しなければ空DataFrame）
    gcs    = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    blob   = bucket.blob(GCS_PATH)

    if not blob.exists():
        return pd.DataFrame()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        blob.download_to_filename(tmp_path)
        return pd.read_parquet(tmp_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _save_parquet(df: pd.DataFrame):
    # DataFrameをGCSにParquet形式で保存する
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, index=False)
        gcs    = storage.Client()
        bucket = gcs.bucket(BUCKET_NAME)
        bucket.blob(GCS_PATH).upload_from_filename(tmp_path)
        print(f"✅ GCS保存完了: gs://{BUCKET_NAME}/{GCS_PATH} ({len(df):,}件)")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ── 収集関数（外部呼び出し用） ────────────────────────────

def collect_n_roudou_initial():
    # 初回一括収集: 一覧ページ上の全PDFを対象にパースしてGCSに保存する
    # 既存データがあれば PDF_URL で重複判定してマージする

    pdf_urls = fetch_pdf_url_list()
    if not pdf_urls:
        print("❌ PDF URLが取得できなかった")
        return 0

    # 既存データから取得済みPDF_URLを抽出する
    existing_df = _load_existing()
    existing_urls = set()
    if not existing_df.empty and "PDF_URL" in existing_df.columns:
        existing_urls = set(existing_df["PDF_URL"].dropna().unique())
        print(f"📊 既存データ: {len(existing_df)}件 / 取得済みPDF: {len(existing_urls)}件")

    # 未取得PDFのみを対象にする
    new_urls = [u for u in pdf_urls if u not in existing_urls]
    print(f"📥 新規パース対象: {len(new_urls)}件")

    # 各PDFをパースする（1月の失敗で全体を止めない）
    new_records = []
    for url in new_urls:
        try:
            records = parse_pdf(url)
            new_records.extend(records)
        except Exception as e:
            print(f"  ❌ {url}: {type(e).__name__}: {e}")
            continue

    if not new_records:
        print("ℹ️  新規に追加するレコードなし")
        return 0

    new_df = pd.DataFrame(new_records)

    # 既存とマージする（DATE+産業コードで重複排除、新しい方を優先）
    if existing_df.empty:
        combined = new_df
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["DATE", "産業コード"], keep="last"
        )

    # DATEでソートする
    combined = combined.sort_values(["DATE", "産業コード"]).reset_index(drop=True)

    _save_parquet(combined)
    return len(new_records)


def collect_n_roudou_monthly():
    # 定期収集: 一覧ページから最新のPDFを確認し、既存になければ追加する
    # 処理内容は initial と同じ（差分のみ取得するため冪等）

    return collect_n_roudou_initial()
