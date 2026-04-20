# 日銀（BOJ）APIエンドポイント
# /boj/pass  : コードAPIパススルー（ページネーション自動処理）
# /boj/layer : 階層APIパススルー（ページネーション自動処理）
# /boj/meta  : メタデータAPIパススルー

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
import httpx

router = APIRouter(prefix="/boj", tags=["日本銀行"])

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


@router.get(
    "/pass",
    summary="コードAPI — 系列コードで時系列データを取得",
)
async def boj_pass(request: Request):
    """
    系列コードを指定して時系列統計データを取得します（日銀コードAPI）。
    上限を超える場合は自動でページネーション処理します。

    **クエリパラメータ:**
    - `db` (必須) DB名（下記参照）
    - `code` (必須) 系列コード（カンマ区切りで複数可、同一期種のみ）
    - `startDate` (任意) 開始期（月次・日次ともYYYYMM形式）
    - `endDate` (任意) 終了期（同上）

    **主なDB名:**
    - `IR01` 基準割引率・基準貸付利率 / `IR04` 貸出約定平均金利
    - `FM01` 無担保コールO/N物レート / `FM08` 外国為替市況 / `FM09` 実効為替レート
    - `MD01` マネタリーベース / `MD02` マネーストック / `MD11` 預金・現金・貸出金
    - `LA01` 貸出先別貸出金
    - `PR01` 企業物価指数 / `PR02` 企業向けサービス価格指数
    - `CO` 短観 / `BP01` 国際収支統計 / `FF` 資金循環 / `PF02` 政府債務

     全DB名一覧: https://www.stat-search.boj.or.jp/info/api_manual.pdf （API機能利用マニュアル P7）

    **URL例:**
    - `/boj/pass?db=FM08&code=FXERD04&startDate=202401` （USD/JPY 日次）
    - `/boj/pass?db=FM08&code=FXERM07&startDate=202401` （USD/JPY 月次平均）

    ※ 系列コードは `/boj/meta?db={DB名}` で確認できます。
    """
    params = dict(request.query_params)

    # codeパラメータは必須のため、ない場合はわかりやすいエラーを返す
    if "code" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error": "codeパラメータは必須です",
                "hint":  "まず /boj/meta?db={DB名} で系列コードを確認してください",
                "example": "/boj/pass?db=FM08&code=FXERD04&startDate=202401",
            }
        )

    # dbパラメータも必須のため確認する
    if "db" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error": "dbパラメータは必須です",
                "hint":  "DB名を指定してください（例: FM08, CO, MD01）",
                "example": "/boj/pass?db=FM08&code=FXERD04&startDate=202401",
            }
        )

    params.setdefault("format", "json")
    params.setdefault("lang",   "jp")
    return await boj_fetch_all("getDataCode", params)


@router.get(
    "/layer",
    summary="階層API — 階層情報でデータを一括取得",
)
async def boj_layer(request: Request):
    """
    階層情報を指定してデータを一括取得します（日銀階層API）。
    上限を超える場合は自動でページネーション処理します。

    **クエリパラメータ:**
    - `db` (必須) DB名
    - `layer` (必須) 階層情報（カンマ区切り。`*` でワイルドカード）
    - `frequency` (必須) 期種: `CY`=暦年 / `FY`=年度 / `Q`=四半期 / `M`=月次 / `D`=日次
    - `startDate` (任意) 開始期（月次: YYYYMM形式）
    - `endDate` (任意) 終了期

    **URL例:**
    - `/boj/layer?db=FF&frequency=Q&layer=1,1,1`
    - `/boj/layer?db=BP01&frequency=M&startDate=202504&endDate=202509&layer=1,1,1`

    ※ 階層情報は `/boj/meta?db={DB名}` のLAYER1〜LAYER5列で確認できます。
    """
    params = dict(request.query_params)

    # db・frequency・layerパラメータは必須のため確認する
    missing = [p for p in ["db", "frequency", "layer"] if p not in params]
    if missing:
        return JSONResponse(
            status_code=400,
            content={
                "error":   f"必須パラメータが不足しています: {', '.join(missing)}",
                "hint":    "db・frequency・layerの3つが必須です",
                "example": "/boj/layer?db=FF&frequency=Q&layer=1,1,1",
            }
        )

    params.setdefault("format", "json")
    params.setdefault("lang",   "jp")
    return await boj_fetch_all("getDataLayer", params)


@router.get(
    "/meta",
    summary="メタデータAPI — 系列コード一覧・収録期間を取得",
)
async def boj_meta(request: Request):
    """
    指定したDBの系列コード一覧・収録期間などのメタ情報を取得します。
    `/boj/pass` や `/boj/layer` で使う系列コード・階層情報の確認に使います。

    **クエリパラメータ:**
    - `db` (必須) DB名

    **レスポンスの主なフィールド:**
    - `SERIES_CODE` 系列コード
    - `NAME_OF_TIME_SERIES_J` 系列名称（日本語）
    - `UNIT_J` 単位
    - `FREQUENCY` 期種
    - `LAYER1`〜`LAYER5` 階層情報
    - `START_OF_THE_TIME_SERIES` 収録開始期
    - `END_OF_THE_TIME_SERIES` 収録終了期

    **URL例:**
    - `/boj/meta?db=FM08` （外国為替市況の系列一覧）
    - `/boj/meta?db=CO` （短観の系列一覧）
    """
    params = dict(request.query_params)

    # dbパラメータは必須のため確認する
    if "db" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error":   "dbパラメータは必須です",
                "hint":    "取得したいDBを指定してください",
                "example": "/boj/meta?db=FM08",
            }
        )

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
