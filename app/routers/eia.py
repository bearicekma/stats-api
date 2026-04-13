# EIA（米エネルギー省）APIエンドポイント
# /eia/pass : 原油価格などのデータをパススルーで取得する

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
import httpx
import os

router = APIRouter(prefix="/eia", tags=["eia"])

# EIA API v2 のベースURL
EIA_BASE_URL = "https://api.eia.gov/v2"


@router.get("/pass")
async def eia_pass(request: Request):
    # EIA APIへのパススルー
    # routeパラメータでエンドポイントを指定する
    # 例: /eia/pass?route=petroleum/pri/spt/data/&data[]=value&facets[series][]=RWTC&frequency=monthly
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
