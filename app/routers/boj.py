# 日銀（BOJ）APIエンドポイント
# /boj/pass  : コードAPIパススルー（ページネーション自動処理）
# /boj/layer : 階層APIパススルー（ページネーション自動処理）
# /boj/meta  : メタデータAPIパススルー
#
# /boj/pass・/boj/layer は既定で縦持ち（1行1値）に変換して返す。
# format=json（既定）縦持ちJSON / format=csv 縦持ちCSV / format=raw 日銀APIの入れ子のまま

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse, Response
import httpx
import pandas as pd

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


# 出力形式（このAPI独自のformat。日銀APIへは常にformat=jsonで問い合わせる）
OUTPUT_FORMATS = ("json", "csv", "raw")

# 縦持ちの列順
LONG_COLUMNS = ["系列コード", "系列名", "分類", "単位", "期種", "時点", "値"]


def to_date(period, frequency: str) -> str:
    # 日銀の時期（SURVEY_DATES）を期首日の YYYY-MM-DD に変換する
    # 暦年 YYYY→1月1日 / 年度 YYYY→4月1日 / 暦年半期 YYYYHH→1月・7月 / 年度半期 YYYYHH→4月・10月
    # 四半期 YYYYQQ→1・4・7・10月 / 月次 YYYYMM→月初 / 週次・日次 YYYYMMDD→その日
    s    = str(period)
    freq = (frequency or "").upper()
    if len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    if len(s) == 4:
        return f"{s}-04-01" if "(MAR)" in freq else f"{s}-01-01"
    if len(s) != 6:
        return s  # 想定外の形式はそのまま返す
    n = int(s[4:])
    if freq.startswith("SEMIANNUAL"):
        month = (4 if "(SEP)" in freq else 1) + (n - 1) * 6
    elif freq.startswith("QUARTERLY"):
        month = 1 + (n - 1) * 3
    else:
        month = n
    return f"{s[:4]}-{month:02d}-01"


def to_long(data: dict) -> list[dict]:
    # 系列ごとの SURVEY_DATES・VALUES の配列を、1行1値のレコードに展開する
    rows = []
    for series in data.get("RESULTSET", []):
        values = series.get("VALUES") or {}
        dates  = values.get("SURVEY_DATES") or []
        nums   = values.get("VALUES") or []
        freq   = series.get("FREQUENCY")
        for period, value in zip(dates, nums):
            rows.append({
                "系列コード": series.get("SERIES_CODE"),
                "系列名":     series.get("NAME_OF_TIME_SERIES_J") or series.get("NAME_OF_TIME_SERIES"),
                "分類":       series.get("CATEGORY_J") or series.get("CATEGORY"),
                "単位":       series.get("UNIT_J") or series.get("UNIT"),
                "期種":       freq,
                "時点":       to_date(period, freq),
                "値":         value,  # 欠損値は null のまま
            })
    return rows


async def boj_fetch_and_format(endpoint: str, params: dict, label: str):
    # 日銀APIから全件取得し、指定の出力形式で返す
    fmt = (params.pop("format", None) or "json").lower()
    if fmt not in OUTPUT_FORMATS:
        return JSONResponse(status_code=400, content={"error": "format は json / csv / raw のいずれか"})

    params["format"] = "json"  # 日銀APIへは常にJSONで問い合わせる
    params.setdefault("lang", "jp")
    data = await boj_fetch_all(endpoint, params)

    if fmt == "raw":
        return data

    rows = to_long(data)
    if fmt == "csv":
        df = pd.DataFrame(rows, columns=LONG_COLUMNS)
        return Response(content=df.to_csv(index=False).encode("utf-8-sig"), media_type="text/csv; charset=utf-8")
    return {
        "source":  label,
        "message": data.get("MESSAGE"),
        "count":   len(rows),
        "data":    rows,
    }


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

    **出力形式（任意）:**
    - `format=json`（既定）縦持ち（1行1値）のJSON
    - `format=csv` 縦持ちのCSV（UTF-8 BOM付き）
    - `format=raw` 日銀APIのまま（系列ごとに SURVEY_DATES・VALUES が入れ子）

    **縦持ちの列:**
    系列コード / 系列名 / 分類 / 単位 / 期種 / 時点 / 値
    - `時点` は期首日の YYYY-MM-DD（月次→月初、四半期→1・4・7・10月、暦年→1月1日、年度→4月1日、
      暦年半期→1・7月、年度半期→4・10月、週次・日次→その日）
    - `値` の欠損は null（CSVは空欄）

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
    - `/boj/pass?db=PR01&code=PRCG20_2600550010&startDate=202001&format=csv` （輸入物価・原油、CSV）

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

    return await boj_fetch_and_format("getDataCode", params, "日銀 コードAPI")


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

    **出力形式（任意）:**
    - `format=json`（既定）縦持ち（1行1値）のJSON
    - `format=csv` 縦持ちのCSV（UTF-8 BOM付き）
    - `format=raw` 日銀APIのまま（系列ごとに SURVEY_DATES・VALUES が入れ子）

    **縦持ちの列:**
    系列コード / 系列名 / 分類 / 単位 / 期種 / 時点 / 値
    - `時点` は期首日の YYYY-MM-DD（月次→月初、四半期→1・4・7・10月、暦年→1月1日、年度→4月1日、
      暦年半期→1・7月、年度半期→4・10月、週次・日次→その日）
    - `値` の欠損は null（CSVは空欄）

    **URL例:**
    - `/boj/layer?db=FF&frequency=Q&layer=1,1,1`
    - `/boj/layer?db=BP01&frequency=M&startDate=202504&endDate=202509&layer=1,1,1`
    - `/boj/layer?db=PR01&frequency=M&layer=1,3,1,6&startDate=202001&format=csv` （輸入物価・石油石炭天然ガス）

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

    return await boj_fetch_and_format("getDataLayer", params, "日銀 階層API")


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
