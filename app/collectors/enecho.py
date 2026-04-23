# 資源エネルギー庁 給油所小売価格調査 収集スクリプト
# 毎週水曜14時公表のExcelファイル（全履歴入り）をダウンロードしてGCSに保存する

import io
import os
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH    = "enecho/gasoline/data.parquet"
ENECHO_BASE = "https://www.enecho.meti.go.jp/statistics/petroleum_and_lpgas/pl007/xlsx"

# 取得する地域の列インデックスと名称（局を除外、全国+47都道府県のみ）
REGION_COLS = [
    (2,  "全国"),
    (3,  "北海道"),
    (4,  "青森"),  (5,  "岩手"),  (6,  "宮城"),  (7,  "秋田"),  (8,  "山形"),  (9,  "福島"),
    (11, "茨城"),  (12, "栃木"),  (13, "群馬"),  (14, "埼玉"),  (15, "千葉"),
    (16, "東京"),  (17, "神奈川"),
    (18, "新潟"),  (19, "長野"),  (20, "山梨"),  (21, "静岡"),
    (23, "愛知"),  (24, "岐阜"),  (25, "三重"),  (26, "富山"),  (27, "石川"),
    (29, "福井"),  (30, "滋賀"),  (31, "京都"),  (32, "奈良"),
    (33, "大阪"),  (34, "兵庫"),  (35, "和歌山"),
    (37, "鳥取"),  (38, "島根"),  (39, "岡山"),  (40, "広島"),  (41, "山口"),
    (43, "徳島"),  (44, "香川"),  (45, "愛媛"),  (46, "高知"),
    (48, "福岡"),  (49, "佐賀"),  (50, "長崎"),  (51, "熊本"),
    (52, "大分"),  (53, "宮崎"),  (54, "鹿児島"),
    (56, "沖縄"),
]

SHEETS = ["ハイオク", "レギュラー", "軽油", "灯油店頭", "灯油配達"]

JST = timezone(timedelta(hours=9))


def _build_url() -> str:
    # 直近の公表日（水曜）に基づいてダウンロードURLを組み立てる
    # URLの形式: {西暦下2桁}{月2桁}{日2桁}s5.xlsx（例: 260422s5.xlsx）
    today = datetime.now(JST).date()
    # 直近の水曜日を求める（weekday: 月=0, 水=2）
    days_since_wed = (today.weekday() - 2) % 7
    # 当日が水曜かつ14時未満の場合は1週前にフォールバックする
    if days_since_wed == 0 and datetime.now(JST).hour < 14:
        days_since_wed = 7
    latest_wed = today - timedelta(days=days_since_wed)
    # 西暦の下2桁を使う（令和年ではない）
    yy   = str(latest_wed.year)[-2:]
    mmdd = latest_wed.strftime("%m%d")
    return f"{ENECHO_BASE}/{yy}{mmdd}s5.xlsx"


def _download_xlsx(url: str) -> bytes:
    # ExcelファイルをHTTPでダウンロードしてバイトデータとして返す
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; stats-api/1.0)",
        "Referer":    "https://www.enecho.meti.go.jp/statistics/petroleum_and_lpgas/pl007/results.html",
    }
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        res = client.get(url, headers=headers)
    res.raise_for_status()
    return res.content


def _parse_xlsx(content: bytes) -> pd.DataFrame:
    # 全シートを縦持ち（long format）に変換して1つのDataFrameに結合する
    xl     = pd.ExcelFile(io.BytesIO(content))
    frames = []

    for sheet in SHEETS:
        df = xl.parse(sheet, header=None)

        tax_series = pd.to_numeric(df.iloc[1:, -1], errors="coerce").reset_index(drop=True)
        dates      = pd.to_datetime(df.iloc[1:, 1]).reset_index(drop=True)

        for col_idx, region_name in REGION_COLS:
            prices = pd.to_numeric(df.iloc[1:, col_idx], errors="coerce").reset_index(drop=True)
            tmp = pd.DataFrame({
                "date":    dates,
                "品目":    sheet,
                "地域":    region_name,
                "価格":    prices,
                "消費税率": tax_series,
            })
            frames.append(tmp)

    result = pd.concat(frames, ignore_index=True)
    result = result.dropna(subset=["date", "価格"])
    result["date"] = result["date"].dt.date
    return result


def _save_to_gcs(df: pd.DataFrame) -> int:
    # DataFrameをParquetに変換してGCSに保存する（全件置き換え）
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    gcs    = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    bucket.blob(GCS_PATH).upload_from_file(buf, content_type="application/octet-stream")
    return len(df)


def collect_enecho_gasoline() -> int:
    # 給油所小売価格調査データを収集してGCSに保存するメイン関数
    url     = _build_url()
    print(f"📥 ダウンロード: {url}")
    content = _download_xlsx(url)
    df      = _parse_xlsx(content)
    count   = _save_to_gcs(df)
    print(f"✅ GCS保存完了: {count}件 → {GCS_PATH}")
    return count
