# デジタル観光統計オープンデータの収集・GCS保存
# 公益社団法人日本観光振興協会が毎月第2木曜に公開するCSVを取得する

import httpx
import pandas as pd
import io
import tempfile
import os
from datetime               import date
from dateutil.relativedelta import relativedelta
from google.cloud           import storage

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH    = "d_kanko/d_kanko.parquet"
BASE_URL    = "https://d2eveo6c5xeu3l.cloudfront.net"
START_YEAR  = 2021   # データ公開開始年


def _fetch_csv(url: str) -> pd.DataFrame | None:
    # 指定URLからCSVを取得してDataFrameで返す（取得失敗時はNone）
    try:
        res = httpx.get(url, timeout=30, follow_redirects=True)
        res.raise_for_status()
        return pd.read_csv(io.BytesIO(res.content), encoding="shift-jis")
    except Exception as e:
        print(f"⚠️ CSV取得スキップ: {url} ({e})")
        return None


def _normalize_pref(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    # 都道府県CSVを統合テーブル形式に変換する
    # dfをベースに列を追加・変換することでインデックスを正しく引き継ぐ
    result = df.copy()
    result["date"]          = pd.to_datetime(f"{year}-{month:02d}-01")
    result["区分"]          = "pref"
    result["都道府県コード"] = df["地域コード"].astype(str).str.zfill(2)
    result["都道府県名"]     = df["地域名称"]
    # 都道府県の地域コードは5桁ゼロ埋めに統一する（例: "20" → "20000"）
    result["地域コード"]     = df["地域コード"].astype(str).str.zfill(2) + "000"
    result["地域名称"]       = df["地域名称"]
    result["人数"]           = df["人数"].astype(int)

    return result[[
        "date", "区分", "都道府県コード", "都道府県名",
        "地域コード", "地域名称", "人数"
    ]].reset_index(drop=True)


def _normalize_city(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    # 市区町村CSVを統合テーブル形式に変換する
    # dfをベースに列を追加・変換することでインデックスを正しく引き継ぐ
    result = df.copy()
    result["date"]          = pd.to_datetime(f"{year}-{month:02d}-01")
    result["区分"]          = "city"
    result["都道府県コード"] = df["都道府県コード"].astype(str).str.zfill(2)
    result["都道府県名"]     = df["都道府県名"]
    # 市区町村の地域コードは5桁ゼロ埋め
    result["地域コード"]     = df["地域コード"].astype(str).str.zfill(5)
    result["地域名称"]       = df["地域名称"]
    result["人数"]           = df["人数"].astype(int)

    return result[[
        "date", "区分", "都道府県コード", "都道府県名",
        "地域コード", "地域名称", "人数"
    ]].reset_index(drop=True)


def _fetch_year(year: int) -> pd.DataFrame:
    # 年まとめCSV（過去年）を取得して統合形式のDataFrameで返す
    frames = []
    for kind, norm_fn in [("pref", _normalize_pref), ("city", _normalize_city)]:
        url = f"{BASE_URL}/{kind}/{kind}{year}.csv"
        df  = _fetch_csv(url)
        if df is None:
            continue
        # 年まとめCSVは月ごとに複数行あるため月ごとに分割して変換する
        for month, group in df.groupby("月"):
            frames.append(norm_fn(group.reset_index(drop=True), year, int(month)))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_month(year: int, month: int) -> pd.DataFrame:
    # 月別CSV（当年）を取得して統合形式のDataFrameで返す
    frames = []
    for kind, norm_fn in [("pref", _normalize_pref), ("city", _normalize_city)]:
        url = f"{BASE_URL}/{kind}/{kind}{year}{month:02d}.csv"
        df  = _fetch_csv(url)
        if df is None:
            continue
        frames.append(norm_fn(df, year, month))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _save_parquet(df: pd.DataFrame):
    # DataFrameをGCSにParquet形式で保存する
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(
            ["date", "区分", "都道府県コード", "地域コード"]
        ).reset_index(drop=True)
        df.to_parquet(tmp_path, index=False)
        gcs    = storage.Client()
        bucket = gcs.bucket(BUCKET_NAME)
        bucket.blob(GCS_PATH).upload_from_filename(tmp_path)
        print(f"✅ GCS保存完了: gs://{BUCKET_NAME}/{GCS_PATH} ({len(df):,}件)")
    finally:
        os.remove(tmp_path)


def collect_d_kanko_initial() -> int:
    # 初回一括収集：2021年〜先月分を全件取得してParquetに保存する
    today    = date.today()
    cur_year = today.year
    frames   = []

    # 過去年分（年まとめCSV）
    for year in range(START_YEAR, cur_year):
        print(f"📥 {year}年（年まとめ）取得中...")
        df = _fetch_year(year)
        if not df.empty:
            frames.append(df)

    # 当年分（月別CSV・1月〜先月）
    last_month = today - relativedelta(months=1)
    for month in range(1, last_month.month + 1):
        print(f"📥 {cur_year}年{month:02d}月取得中...")
        df = _fetch_month(cur_year, month)
        if not df.empty:
            frames.append(df)

    if not frames:
        print("❌ データが取得できませんでした")
        return 0

    result = pd.concat(frames, ignore_index=True)
    _save_parquet(result)
    return len(result)


def collect_d_kanko_monthly() -> int:
    # 定期収集：先月分のCSVを既存Parquetに追記して上書き保存する
    # 毎月第2木曜に実行（Cloud Scheduler）
    today      = date.today()
    last_month = today - relativedelta(months=1)
    year       = last_month.year
    month      = last_month.month

    print(f"📥 {year}年{month:02d}月（定期収集）取得中...")
    new_df = _fetch_month(year, month)

    if new_df.empty:
        print("⚠️ 新規データなし")
        return 0

    # 既存Parquetを読み込んで追記する
    try:
        gcs    = storage.Client()
        bucket = gcs.bucket(BUCKET_NAME)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            existing_path = tmp.name
        bucket.blob(GCS_PATH).download_to_filename(existing_path)
        existing_df = pd.read_parquet(existing_path)
        os.remove(existing_path)

        # 重複除去（同じdate・区分・地域コードは上書き）
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["date", "区分", "地域コード"], keep="last"
        )
    except Exception:
        # 既存ファイルがない場合はそのまま保存する
        combined = new_df

    # 1月収集時は前年分を年まとめCSVに差し替える
    if month == 1:
        prev_year = year - 1
        print(f"🔄 {prev_year}年分を年まとめCSVに差し替え中...")
        prev_df = _fetch_year(prev_year)
        if not prev_df.empty:
            combined = combined[combined["date"].dt.year != prev_year]
            combined = pd.concat([combined, prev_df], ignore_index=True)

    _save_parquet(combined)
    return len(new_df)
