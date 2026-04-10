
import json
import os
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict
import httpx
from mcp.server.fastmcp import FastMCP

# --- 定数 ---
# 自分自身のベースURL（Cloud Run環境変数から取得、ローカルはlocalhost）
BASE_URL = os.getenv("STATS_API_BASE_URL", "http://localhost:8080")
HTTP_TIMEOUT = 30.0

# FastMCPサーバー初期化
mcp = FastMCP("stats_api_mcp", stateless_http=True)


# --- 共通HTTPクライアント ---
async def _get(path: str, params: dict = None) -> dict:
    """stats-apiの内部エンドポイントをGETで呼び出す共通関数"""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(f"{BASE_URL}{path}", params=params)
        resp.raise_for_status()
        return resp.json()


def _handle_error(e: Exception) -> str:
    """エラーをClaudeが理解しやすいメッセージに変換"""
    if isinstance(e, httpx.HTTPStatusError):
        return f"Error {e.response.status_code}: {e.response.text}"
    if isinstance(e, httpx.TimeoutException):
        return "Error: リクエストがタイムアウトしました。期間を絞るか、再試行してください。"
    return f"Error: {type(e).__name__}: {e}"


# =============================================================================
# ツール定義
# =============================================================================

# --- GCS+DuckDB 保存データ ---

class StatsGetCollectionInput(BaseModel):
    """stats_get_collection のパラメータ"""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    collection_name: str = Field(
        ..., description="コレクション名。例: 'cpi_nagano'、'boj_exchange'"
    )

@mcp.tool(
    name="stats_get_collection",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
)
async def stats_get_collection(params: StatsGetCollectionInput) -> str:
    """GCS+DuckDBに保存された統計コレクションデータを取得する。

    定期収集・蓄積済みのデータ（e-Stat・日銀等）を返す。
    利用可能なコレクション名は事前に確認が必要。

    Args:
        params: collection_name (str) - 取得するコレクション名

    Returns:
        str: JSON形式のデータ配列
    """
    try:
        data = await _get(f"/stats/{params.collection_name}")
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception as e:
        return _handle_error(e)


# --- マスタデータ ---

class StatsGetMasterInput(BaseModel):
    """stats_get_master のパラメータ"""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    collection_name: str = Field(
        ..., description="マスタコレクション名。例: '_M_calendar'、'_M_prefecture'"
    )

@mcp.tool(
    name="stats_get_master",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
)
async def stats_get_master(params: StatsGetMasterInput) -> str:
    """マスタデータ（カレンダー・都道府県等）を取得する。

    Args:
        params: collection_name (str) - マスタコレクション名

    Returns:
        str: JSON形式のマスタデータ
    """
    try:
        data = await _get(f"/master/{params.collection_name}")
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception as e:
        return _handle_error(e)


# --- e-Stat ---

class EstatGetMetadataInput(BaseModel):
    """estat_get_metadata のパラメータ"""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    stats_data_id: str = Field(
        ..., description="e-Stat統計表ID。例: '0003427113'（消費者物価指数）"
    )

@mcp.tool(
    name="estat_get_metadata",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
)
async def estat_get_metadata(params: EstatGetMetadataInput) -> str:
    """e-Statの統計表メタ情報（分類・地域・時期の選択肢と総件数）を取得する。

    estat_get_data を呼ぶ前に、利用可能なパラメータコードを確認するために使う。

    Args:
        params: stats_data_id (str) - e-Stat統計表ID

    Returns:
        str: JSON形式のメタ情報（classifications, total_count等）
    """
    try:
        data = await _get(f"/estat/meta/{params.stats_data_id}")
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception as e:
        return _handle_error(e)


class EstatGetDataInput(BaseModel):
    """estat_get_data のパラメータ"""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    stats_data_id: str = Field(
        ..., description="e-Stat統計表ID。例: '0003427113'"
    )
    cd_time_from: Optional[str] = Field(
        None, description="開始時期コード。例: '2024000000'（2024年）、'2024001201'（2024年12月）"
    )
    cd_time_to: Optional[str] = Field(
        None, description="終了時期コード。例: '2024999999'（2024年末）"
    )
    cd_area: Optional[str] = Field(
        None, description="地域コード（カンマ区切り可）。例: '00000'（全国）、'20A01'（長野市）"
    )
    cd_cat01: Optional[str] = Field(
        None, description="分類1コード。例: '0001'（総合）"
    )
    extra_params: Optional[str] = Field(
        None, description="追加パラメータをJSON文字列で指定。例: '{\"cdTab\": \"1\"}'"
    )

@mcp.tool(
    name="estat_get_data",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
)
async def estat_get_data(params: EstatGetDataInput) -> str:
    """e-Stat統計データをパススルー取得する（保存なし）。

    大量データがある場合は cd_time_from/cd_time_to で年単位に絞ること。
    利用可能なコードは estat_get_metadata で事前確認すること。

    Args:
        params:
            stats_data_id (str): e-Stat統計表ID
            cd_time_from (str, optional): 開始時期コード
            cd_time_to (str, optional): 終了時期コード
            cd_area (str, optional): 地域コード
            cd_cat01 (str, optional): 分類1コード
            extra_params (str, optional): 追加パラメータJSON文字列

    Returns:
        str: JSON形式の統計データ（value配列）
    """
    try:
        query: dict = {}
        if params.cd_time_from:
            query["cdTimeFrom"] = params.cd_time_from
        if params.cd_time_to:
            query["cdTimeTo"] = params.cd_time_to
        if params.cd_area:
            query["cdArea"] = params.cd_area
        if params.cd_cat01:
            query["cdCat01"] = params.cd_cat01
        if params.extra_params:
            query.update(json.loads(params.extra_params))

        data = await _get(f"/estat/pass/{params.stats_data_id}", params=query)
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception as e:
        return _handle_error(e)


# --- 日銀BOJ ---

class BojGetDataInput(BaseModel):
    """boj_get_data のパラメータ"""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    db: str = Field(
        ..., description="DB名。例: 'FM08'（外国為替）、'CO'（短観）、'PR01'（企業物価）"
    )
    code: str = Field(
        ..., description="系列コード（カンマ区切り、同一期種のみ）。例: 'FXERM07'（ドル円月次）"
    )
    start_date: Optional[str] = Field(
        None, description="開始期。月次: 'YYYYMM'、四半期: 'YYYYQQ'、暦年: 'YYYY'。例: '202401'"
    )
    end_date: Optional[str] = Field(
        None, description="終了期（start_dateと同形式）。例: '202412'"
    )

@mcp.tool(
    name="boj_get_data",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
)
async def boj_get_data(params: BojGetDataInput) -> str:
    """日銀時系列統計データを系列コード指定で取得する（コードAPI）。

    系列コードは boj_get_metadata で確認できる。
    同一期種（月次・四半期等）のコードのみ混在可能。

    Args:
        params:
            db (str): DB名（例: FM08, CO, PR01）
            code (str): 系列コード（カンマ区切り）
            start_date (str, optional): 開始期
            end_date (str, optional): 終了期

    Returns:
        str: JSON形式の時系列データ
    """
    try:
        query = {"db": params.db, "code": params.code}
        if params.start_date:
            query["startDate"] = params.start_date
        if params.end_date:
            query["endDate"] = params.end_date
        data = await _get("/boj/pass", params=query)
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception as e:
        return _handle_error(e)


class BojGetLayerInput(BaseModel):
    """boj_get_layer のパラメータ"""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    db: str = Field(..., description="DB名。例: 'FF'（資金循環）、'BP01'（国際収支）")
    frequency: str = Field(
        ..., description="期種。'M'（月次）、'Q'（四半期）、'CY'（暦年）、'FY'（年度）等"
    )
    layer: str = Field(
        ..., description="階層情報（カンマ区切り）。例: '1,1,1'、'*'（全件）"
    )
    start_date: Optional[str] = Field(None, description="開始期（期種に合わせた形式）")
    end_date: Optional[str] = Field(None, description="終了期")

@mcp.tool(
    name="boj_get_layer",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
)
async def boj_get_layer(params: BojGetLayerInput) -> str:
    """日銀時系列統計データを階層指定で取得する（階層API）。

    特定DBの階層ツリー配下のデータを一括取得する。
    階層構造は boj_get_metadata で確認する。
    1回のリクエストで最大250系列・6万データ点まで取得可能。

    Args:
        params:
            db (str): DB名
            frequency (str): 期種
            layer (str): 階層情報
            start_date (str, optional): 開始期
            end_date (str, optional): 終了期

    Returns:
        str: JSON形式の時系列データ（NEXTPOSITION含む）
    """
    try:
        query = {
            "db": params.db,
            "frequency": params.frequency,
            "layer": params.layer,
        }
        if params.start_date:
            query["startDate"] = params.start_date
        if params.end_date:
            query["endDate"] = params.end_date
        data = await _get("/boj/layer", params=query)
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception as e:
        return _handle_error(e)


class BojGetMetadataInput(BaseModel):
    """boj_get_metadata のパラメータ"""
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    db: str = Field(
        ..., description="DB名。例: 'FM08'（外国為替市況）、'PR01'（企業物価指数）"
    )

@mcp.tool(
    name="boj_get_metadata",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
)
async def boj_get_metadata(params: BojGetMetadataInput) -> str:
    """日銀DBのメタ情報（系列コード・系列名・収録期間・階層情報）を取得する。

    boj_get_data や boj_get_layer で使用するコードを調べるために使う。

    Args:
        params: db (str) - DB名

    Returns:
        str: JSON形式のメタ情報（系列コード・名称・期種・収録期間等）
    """
    try:
        data = await _get("/boj/meta", params={"db": params.db})
        return json.dumps(data, ensure_ascii=False, indent=2)
    except Exception as e:
        return _handle_error(e)
