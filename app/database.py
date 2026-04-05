
# GCS（Parquet形式）へのデータ保存・読み込みを担当

from google.cloud import storage
import pandas as pd
import duckdb
import tempfile
import os

# ==============================
# 設定
# ==============================
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")

# GCSクライアントを初期化（モジュール読み込み時に1回だけ作成）
gcs    = storage.Client()
bucket = gcs.bucket(BUCKET_NAME)


def _download_parquet(collection: str, category: str = "estat") -> str:
    """
    GCSからParquetファイルを一時ファイルにダウンロードしてパスを返す
    内部共通処理（get_stats・query_statsで使用）
    """

    gcs_path = f"{category}/{collection}/data.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    blob = bucket.blob(gcs_path)
    blob.download_to_filename(tmp_path)

    return tmp_path


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """
    DataFrameを辞書リストに変換する
    NaN・inf等のJSON非対応の値をNoneに変換する
    """

    # NaN・inf を None に変換（JSONシリアライズエラー対策）
    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient="records")


def save_stats(collection: str, records: list[dict], category: str = "estat"):
    """
    GCSにParquet形式でデータを保存する

    collection : データの種類（例："population", "cpi"）
    records    : 保存する辞書のリスト（全件まとめて渡す）
    category   : 保存先カテゴリ（"estat" or "master"）
    """

    if not records:
        print(f"⚠️ 保存するデータがありません: {collection}")
        return

    # 辞書のリストをDataFrameに変換
    df = pd.DataFrame(records)

    # 一時ファイルにParquet形式で書き出す
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        df.to_parquet(tmp_path, index=False)

    # GCSにアップロード
    gcs_path = f"{category}/{collection}/data.parquet"
    blob     = bucket.blob(gcs_path)
    blob.upload_from_filename(tmp_path)

    # 一時ファイルを削除
    os.remove(tmp_path)

    print(f"✅ GCS に保存しました: gs://{BUCKET_NAME}/{gcs_path} ({len(records)}件)")


def get_stats(collection: str, category: str = "estat") -> list[dict]:
    """
    GCSからParquetファイルを読み込んで全データを返す

    collection : データの種類（例："population", "cpi"）
    category   : 取得元カテゴリ（"estat" or "master"）
    """

    tmp_path = None
    try:
        tmp_path = _download_parquet(collection, category)
        df       = pd.read_parquet(tmp_path)
        return _df_to_records(df)

    except Exception as e:
        print(f"❌ データ取得エラー: {collection}: {e}")
        return []

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


def query_stats(collection: str, where: str, category: str = "estat") -> list[dict]:
    """
    DuckDBでParquetファイルをSQLクエリして返す
    ?sql= パラメータのWHERE句をそのまま渡す

    例: query_stats("cpi", "年='2024年' AND 地域='全国'")
    """

    tmp_path = None
    try:
        tmp_path = _download_parquet(collection, category)
        sql      = f"SELECT * FROM read_parquet('{tmp_path}') WHERE {where}"
        result   = duckdb.query(sql).to_df()
        return _df_to_records(result)

    except Exception as e:
        print(f"❌ クエリエラー: {collection}: {e}")
        return []

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
