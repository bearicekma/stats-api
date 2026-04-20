# FRED（米連邦準備銀行 経済データ）APIエンドポイント
# /fred/pass   : 系列データ取得（時系列の数値）
# /fred/meta   : 系列のメタ情報（名称・単位・期間など）
# /fred/search : キーワードで系列を検索

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
import httpx
import os

router = APIRouter(prefix="/fred", tags=["FRED 米連邦準備銀行"])

# FRED API のベースURL
FRED_BASE_URL = "https://api.stlouisfed.org/fred"


@router.get(
    "/pass",
    summary="時系列データ取得",
)
async def fred_pass(request: Request):
    """
    指定した系列IDの時系列データを取得します。
    APIキーは不要（サーバー側で付与）。

    **クエリパラメータ:**
    - `series_id` (必須) FRED系列ID（`/fred/search` で検索可）
    - `observation_start` (任意) 開始日（YYYY-MM-DD）
    - `observation_end` (任意) 終了日（YYYY-MM-DD）
    - `frequency` (任意) 集計頻度変換: `d`=日次 / `w`=週次 / `m`=月次 / `q`=四半期 / `a`=年次

    **よく使う系列ID:**
    - `DEXJPUS` USD/JPY 為替レート（日次）
    - `FEDFUNDS` FF金利（月次）
    - `CPIAUCSL` 米国CPI 季節調整済（月次）
    - `GDP` 米国GDP（四半期）
    - `UNRATE` 米国失業率（月次）
    - `DGS10` 10年物国債利回り（日次）

    **レスポンスフィールド:**
    - `series_id` 系列ID
    - `count` 件数
    - `data[].date` 日付（YYYY-MM-DD）
    - `data[].value` 値（欠損値はnull）

    **URL例:**
    - `/fred/pass?series_id=DEXJPUS&observation_start=2024-01-01`
    - `/fred/pass?series_id=FEDFUNDS`
    """
    params = dict(request.query_params)

    # series_idは必須
    if "series_id" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error":   "series_idパラメータは必須です",
                "hint":    "/fred/search で系列IDを検索できます",
                "example": "/fred/pass?series_id=DEXJPUS&observation_start=2024-01-01",
            }
        )

    # APIキーとJSON形式を付加する
    params["api_key"]   = os.environ["FRED_API_KEY"]
    params["file_type"] = "json"

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            f"{FRED_BASE_URL}/series/observations",
            params=params,
        )
    res.raise_for_status()
    data = res.json()

    # observationsを整形して返す
    observations = data.get("observations", [])
    cleaned = [
        {
            "date":  obs["date"],
            # 欠損値（"."）はNoneに変換する
            "value": None if obs["value"] == "." else float(obs["value"])
        }
        for obs in observations
    ]

    return {
        "series_id": params["series_id"],
        "count":     len(cleaned),
        "data":      cleaned,
    }


@router.get(
    "/meta",
    summary="系列のメタ情報取得",
)
async def fred_meta(request: Request):
    """
    指定した系列IDのメタ情報を取得します。

    **クエリパラメータ:**
    - `series_id` (必須) FRED系列ID

    **レスポンスフィールド:**
    - `series_id` 系列ID
    - `title` 系列名称
    - `units` 単位
    - `frequency` 更新頻度
    - `seasonal_adj` 季節調整の有無
    - `observation_start` 収録開始日
    - `observation_end` 収録終了日
    - `last_updated` 最終更新日時
    - `notes` 備考

    **URL例:**
    - `/fred/meta?series_id=DEXJPUS`
    """
    params = dict(request.query_params)

    if "series_id" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error":   "series_idパラメータは必須です",
                "example": "/fred/meta?series_id=DEXJPUS",
            }
        )

    params["api_key"]   = os.environ["FRED_API_KEY"]
    params["file_type"] = "json"

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            f"{FRED_BASE_URL}/series",
            params=params,
        )
    res.raise_for_status()
    data = res.json()

    # seriesリストの先頭1件を整形して返す
    series_list = data.get("seriess", [])
    if not series_list:
        return JSONResponse(status_code=404, content={"error": "系列が見つかりません"})

    s = series_list[0]
    return {
        "series_id":         s.get("id"),
        "title":             s.get("title"),
        "units":             s.get("units"),
        "frequency":         s.get("frequency"),
        "seasonal_adj":      s.get("seasonal_adjustment"),
        "observation_start": s.get("observation_start"),
        "observation_end":   s.get("observation_end"),
        "last_updated":      s.get("last_updated"),
        "notes":             s.get("notes"),
    }


@router.get(
    "/search",
    summary="キーワードで系列を検索",
)
async def fred_search(request: Request):
    """
    キーワードで系列を検索します。
    `/fred/pass` に使う系列IDを調べるのに使います。

    **クエリパラメータ:**
    - `search_text` (必須) 検索キーワード（スペース区切りでAND検索）
    - `limit` (任意) 取得件数（デフォルト1000、最大1000）

    **レスポンスフィールド:**
    - `data[].series_id` 系列ID
    - `data[].title` 系列名称
    - `data[].units` 単位
    - `data[].frequency` 更新頻度
    - `data[].observation_start` 収録開始日
    - `data[].observation_end` 収録終了日

    **URL例:**
    - `/fred/search?search_text=japan+yen&limit=10`
    - `/fred/search?search_text=consumer+price+index+japan`
    """
    params = dict(request.query_params)

    if "search_text" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error":   "search_textパラメータは必須です",
                "example": "/fred/search?search_text=japan+yen&limit=10",
            }
        )

    params["api_key"]     = os.environ["FRED_API_KEY"]
    params["file_type"]   = "json"
    params.setdefault("search_type", "full_text")

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            f"{FRED_BASE_URL}/series/search",
            params=params,
        )
    res.raise_for_status()
    data = res.json()

    series_list = data.get("seriess", [])
    return {
        "count": len(series_list),
        "data": [
            {
                "series_id":         s.get("id"),
                "title":             s.get("title"),
                "units":             s.get("units"),
                "frequency":         s.get("frequency"),
                "observation_start": s.get("observation_start"),
                "observation_end":   s.get("observation_end"),
            }
            for s in series_list
        ]
    }
