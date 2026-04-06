# 日銀（BOJ）APIエンドポイント
# /boj/pass  : コードAPIパススルー（ページネーション自動処理）
# /boj/layer : 階層APIパススルー（ページネーション自動処理）
# /boj/meta  : メタデータAPIパススルー

from fastapi import APIRouter, Request
import httpx

router = APIRouter(prefix="/boj", tags=["boj"])

# 日銀APIのベースURL
BOJ_BASE_URL = "https://www.stat-search.boj.or.jp/api/v1"


async def boj_fetch_all(endpoint: str, params: dict) -> dict:
    # 日銀APIをページネーションで全件取得する共通関数
    # NEXTPOSITIONがなくなるまでリクエストを繰り返す
    all_series    = []
    next_position = None

    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            # 続きがある場合は開始位置を指定する
            if next_position:
                params["startPosition"] = next_position

            res = await client.get(
                f"{BOJ_BASE_URL}/{endpoint}",
                params=params,
                headers={"Accept-Encoding": "gzip"},  # 通信量削減のためgzip圧縮を要求する
            )
            res.raise_for_status()
            data = res.json()

            # RESULTSET（系列データ）を蓄積する
            result_set = data.get("RESULTSET", [])
            if isinstance(result_set, list):
                all_series.extend(result_set)
            elif result_set:
                all_series.append(result_set)

            # NEXTPOSITIONがなければ全件取得完了
            next_position = data.get("NEXTPOSITION")
            if not next_position:
                break

    # 全件取得後にRESULTSETを差し替えて返す
    data["RESULTSET"]    = all_series
    data["NEXTPOSITION"] = None
    return data


@router.get("/pass")
async def boj_pass(request: Request):
    # 日銀コードAPIへのパススルー（系列コード指定でデータ取得）
    # 例: /boj/pass?db=FM08&code=FXUSDM&startDate=202401&endDate=202503
    params = dict(request.query_params)
    params.setdefault("format", "json")
    params.setdefault("lang",   "jp")

    return await boj_fetch_all("getDataCode", params)


@router.get("/layer")
async def boj_layer(request: Request):
    # 日銀階層APIへのパススルー（階層情報でデータ取得）
    # 例: /boj/layer?db=FF&frequency=Q&layer=1,1,1
    params = dict(request.query_params)
    params.setdefault("format", "json")
    params.setdefault("lang",   "jp")

    return await boj_fetch_all("getDataLayer", params)


@router.get("/meta")
async def boj_meta(request: Request):
    # 日銀メタデータAPIへのパススルー（DB内の系列コード一覧取得）
    # 例: /boj/meta?db=FM08
    params = dict(request.query_params)
    params.setdefault("format", "json")
    params.setdefault("lang",   "jp")

    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.get(
            f"{BOJ_BASE_URL}/getMetadata",
            params=params,
            headers={"Accept-Encoding": "gzip"},
        )
    res.raise_for_status()
    return res.json()
