# EIA（米エネルギー省）APIエンドポイント
# /eia/pass : 原油価格などのデータをパススルーで取得する

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
import httpx
import os

router = APIRouter(prefix="/eia", tags=["EIA 米エネルギー省"])

# EIA API v2 のベースURL
EIA_BASE_URL = "https://api.eia.gov/v2"


@router.get(
    "/pass",
    summary="EIA API パススルー",
)
async def eia_pass(request: Request):
    """
    米国エネルギー情報局（EIA）のデータをパススルーで取得します。
    APIキーは不要（サーバー側で付与）。

    **クエリパラメータ:**
    - `route` (必須) EIA APIのエンドポイントパス（下記参照）
    - `data[]` (任意) 取得するデータ列（例: `value`）
    - `facets[series][]` (任意) 系列フィルター（例: `RWTC`=WTI原油）
    - `frequency` (任意) 集計頻度: `daily` / `weekly` / `monthly` / `annual`
    - `start` (任意) 開始日（YYYY-MM-DD）
    - `end` (任意) 終了日（YYYY-MM-DD）
    - `length` (任意) 取得件数（最大5000）
    - `offset` (任意) ページネーション用オフセット

    **主なrouteパス:**
    - `petroleum/pri/spt/data/` WTI・Brent原油スポット価格
    - `petroleum/pri/gnd/dcus/nus/data/` 米国ガソリン小売価格
    - `natural-gas/pri/sum/dcus/nus/data/` 米国天然ガス価格

    **URL例:**
    - `/eia/pass?route=petroleum/pri/spt/data/&data[]=value&facets[series][]=RWTC&frequency=monthly`

    詳細なrouteパスは EIA API Explorer を参照してください:
    https://www.eia.gov/opendata/browser/
    """
    params = dict(request.query_params)

    # routeパラメータは必須のため確認する
    if "route" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error":   "routeパラメータは必須です",
                "hint":    "取得したいEIAのエンドポイントパスを指定してください",
                "example": "/eia/pass?route=petroleum/pri/spt/data/&data[]=value&facets[series][]=RWTC&frequency=monthly",
            }
        )

    # routeをURLに組み込み、残りのパラメータをクエリに渡す
    route = params.pop("route")
    params["api_key"] = os.environ["EIA_API_KEY"]

    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.get(
            f"{EIA_BASE_URL}/{route}",
            params=params,
        )
    res.raise_for_status()
    return res.json()
