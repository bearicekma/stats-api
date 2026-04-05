# GCS（Parquet形式）へのデータ保存・読み込みを担当

from google.cloud import storage
import pandas as pd
import duckdb
import tempfile
import math
import os

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")

gcs    = storage.Client()
bucket = gcs.bucket(BUCKET_NAME)


def _download_parquet(collection: str, category: str = "estat") -> str:
    # GCSからParquetファイルを一時ファイルにダウンロードしてパスを返す
    gcs_path = f"{category}/{collection}/data.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    blob = bucket.blob(gcs_path)
    blob.download_to_filename(tmp_path)

    return tmp_path


def _clean_value(v):
    # float型のNaN・inf・-infをNoneに変換する
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    # DataFrameを辞書リストに変換しNaN等をNoneに変換する
    records = df.to_dict(orient="records")
    return [
        {k: _clean_value(v) for k, v in record.items()}
        for record in records
    ]


def save_stats(collection: str, records: list[dict], category: str = "estat"):
    # GCSにParquet形式でデータを保存する
    if not records:
        print(f"⚠️ 保存するデータがありません: {collection}")
        return

    df = pd.DataFrame(records)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        df.to_parquet(tmp_path, index=False)

    gcs_path = f"{category}/{collection}/data.parquet"
    blob     = bucket.blob(gcs_path)
    blob.upload_from_filename(tmp_path)

    os.remove(tmp_path)

    print(f"✅ GCS に保存しました: gs://{BUCKET_NAME}/{gcs_path} ({len(records)}件)")


def get_stats(collection: str, category: str = "estat") -> list[dict]:
    # GCSからParquetファイルを読み込んで全データを返す
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
    # DuckDBでParquetファイルをSQLクエリして返す
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
