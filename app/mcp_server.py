# MCPサーバー定義
# stats-api の各エンドポイントを Claude から呼べるツールとして公開する。
# 自分自身のFastAPIアプリを localhost 経由で呼ぶ構成（同一プロセス内）。

import json
import os
from typing import Annotated, Dict, Optional

import httpx
from pydantic import Field
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# --- 設定 ---
PORT         = os.getenv("PORT", "8080")
BASE_URL     = f"http://127.0.0.1:{PORT}"
HTTP_TIMEOUT = 120.0      # EDINETの範囲検索など長時間かかる処理に対応
MAX_CHARS    = 80000      # Claudeのコンテキストを守るための応答上限

# stateless_http: セッション管理不要（Cloud Run向き）
# json_response : SSEストリームを使わずJSONで返す（GZipMiddlewareと競合しない）
# transport_security: Cloud RunのHostヘッダーで弾かれないようDNSリバインディング保護を無効化
mcp = FastMCP(
    "stats_api",
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False
    ),
)


# --- 共通処理 ---
async def _get(path: str, params: dict | None = None) -> str:
    """内部エンドポイントをGETで呼び、JSON文字列を返す"""
    try:
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.get(f"{BASE_URL}{path}", params=clean)
            r.raise_for_status()
            text = json.dumps(r.json(), ensure_ascii=False)
    except httpx.HTTPStatusError as e:
        return f"Error {e.response.status_code}: {e.response.text[:500]}"
    except httpx.TimeoutException:
        return "Error: タイムアウト。取得期間や条件を絞って再実行してください。"
    except Exception as e:
        return f"Error: {type(e).__name__}: {e}"

    if len(text) > MAX_CHARS:
        return (
            text[:MAX_CHARS]
            + f"\n\n[切り詰め] 応答が大きすぎます（全{len(text):,}文字）。"
              "期間・地域・品目などで絞り込んで再取得してください。"
        )
    return text


def _extra(extra_params) -> dict:
    """追加パラメータをdictに変換する（dict / JSON文字列 / None のいずれも受け付ける）"""
    if not extra_params:
        return {}
    if isinstance(extra_params, dict):
        return extra_params
    return json.loads(extra_params)


# =============================================================================
# 仕様確認
# =============================================================================

@mcp.tool()
async def get_api_spec() -> str:
    """stats-api の全エンドポイント仕様（OpenAPI）を取得する。

    各ツールのパラメータ詳細や、ツール化されていないエンドポイントを
    調べたいときに使う。ルーター追加時もここに自動反映される。
    """
    return await _get("/openapi.json")


# =============================================================================
# 汎用（GCS保存データ / マスタ）
# =============================================================================

@mcp.tool()
async def stats_get_collection(
    collection_name: Annotated[str, Field(description="GCSに保存されたコレクション名")],
) -> str:
    """GCSに保存された統計データを汎用取得する。

    専用エンドポイント（d_kanko, jma, enecho など）がある場合はそちらを優先すること。
    """
    return await _get(f"/stats/{collection_name}")


@mcp.tool()
async def master_get(
    collection_name: Annotated[str, Field(
        description="マスタ名。_M_pref / _M_city / _M_calendar / _M_country / _M_zairyu_shikaku"
    )],
    year: Annotated[Optional[str], Field(description="_M_calendarのみ: 年で絞込。例 2026")] = None,
    month: Annotated[Optional[str], Field(description="_M_calendarのみ: 月で絞込 1-12")] = None,
    from_date: Annotated[Optional[str], Field(description="_M_calendarのみ: 開始日 YYYY-MM-DD")] = None,
    to_date: Annotated[Optional[str], Field(description="_M_calendarのみ: 終了日 YYYY-MM-DD")] = None,
    holiday_only: Annotated[Optional[str], Field(description="_M_calendarのみ: trueで祝日のみ")] = None,
    weekday: Annotated[Optional[str], Field(description="_M_calendarのみ: 曜日コード 0=月〜6=日")] = None,
) -> str:
    """マスタデータを取得する。

    利用可能なマスタ:
    - _M_pref            都道府県マスタ（47件）
    - _M_city            市区町村マスタ（全国）
    - _M_calendar        カレンダーマスタ（祝日・平日判定、1950年〜）
    - _M_country         国名マスタ（財務省貿易統計ベース）
    - _M_zairyu_shikaku  在留資格マスタ（e-Stat cat01ベース）

    絞り込みパラメータは _M_calendar のみ有効。全件取得は重いため
    _M_calendar は year か from_date/to_date での絞込を推奨。
    """
    return await _get(f"/master/{collection_name}", {
        "year": year, "month": month, "from": from_date, "to": to_date,
        "holiday_only": holiday_only, "weekday": weekday,
    })


# =============================================================================
# e-Stat
# =============================================================================

@mcp.tool()
async def estat_meta(
    stats_data_id: Annotated[str, Field(description="e-Stat統計表ID。例: 0003427113")],
    extra_params: Annotated[Optional[Dict[str, str]], Field(
        description='絞込条件をオブジェクトで指定。例: {"cdArea":"13A01","cdTimeFrom":"2020000000"}'
    )] = None,
) -> str:
    """e-Stat統計表のパラメータ一覧と、絞込条件を反映した件数を返す。

    estat_pass で実データを取る前に、必ずこれで件数とパラメータを確認すること。
    レスポンスの total_number（件数）と parameters（指定可能な選択肢）が重要。
    """
    return await _get(f"/estat/meta/{stats_data_id}", _extra(extra_params))


@mcp.tool()
async def estat_pass(
    stats_data_id: Annotated[str, Field(description="e-Stat統計表ID。例: 0003427113")],
    cd_area: Annotated[Optional[str], Field(description="地域コード。カンマ区切り可。例: 00000,13A01")] = None,
    cd_cat01: Annotated[Optional[str], Field(description="分類事項01のコード")] = None,
    cd_cat02: Annotated[Optional[str], Field(description="分類事項02のコード")] = None,
    cd_cat03: Annotated[Optional[str], Field(description="分類事項03のコード")] = None,
    cd_cat04: Annotated[Optional[str], Field(description="分類事項04のコード")] = None,
    cd_time: Annotated[Optional[str], Field(description="時間軸コード。カンマ区切り可。例: 2023000000")] = None,
    cd_time_from: Annotated[Optional[str], Field(description="時間軸の開始。例: 2024000000")] = None,
    cd_time_to: Annotated[Optional[str], Field(description="時間軸の終了")] = None,
    extra_params: Annotated[Optional[Dict[str, str]], Field(
        description='上記以外のe-Statパラメータをオブジェクトで指定。例: {"cdCat05":"001"}'
    )] = None,
) -> str:
    """e-Stat統計データを取得し、コードを日本語名称に変換して返す。

    大量データは応答が切り詰められる。必ず estat_meta で件数を確認し、
    地域・期間で絞ってから呼ぶこと。
    """
    # /estat/pass の既定出力はCSVのため、MCPでは明示的にJSONを要求する
    params = {
        "format": "json",
        "cdArea": cd_area,
        "cdCat01": cd_cat01, "cdCat02": cd_cat02,
        "cdCat03": cd_cat03, "cdCat04": cd_cat04,
        "cdTime": cd_time,
        "cdTimeFrom": cd_time_from, "cdTimeTo": cd_time_to,
    }
    params.update(_extra(extra_params))
    return await _get(f"/estat/pass/{stats_data_id}", params)


# =============================================================================
# 日銀 BOJ
# =============================================================================

@mcp.tool()
async def boj_meta(
    db: Annotated[str, Field(description="DB名。例: FM08（外国為替市況）、CO（短観）")],
) -> str:
    """日銀DBの系列コード一覧・系列名・収録期間・階層情報を取得する。

    boj_pass / boj_layer で使うコードを調べるために先に呼ぶ。

    主なDB名:
      IR01 基準割引率 / IR04 貸出約定平均金利
      FM01 無担保コールO/N / FM08 外国為替市況 / FM09 実効為替レート
      MD01 マネタリーベース / MD02 マネーストック / MD11 預金・現金・貸出金
      LA01 貸出先別貸出金 / PR01 企業物価指数 / PR02 企業向けサービス価格指数
      CO 短観 / BP01 国際収支 / FF 資金循環 / PF02 政府債務
    """
    return await _get("/boj/meta", {"db": db})


@mcp.tool()
async def boj_pass(
    db: Annotated[str, Field(description="DB名。例: FM08")],
    code: Annotated[str, Field(description="系列コード。カンマ区切りで複数可（同一期種のみ）")],
    start_date: Annotated[Optional[str], Field(description="開始期。YYYYMM形式。例: 202401")] = None,
    end_date: Annotated[Optional[str], Field(description="終了期。YYYYMM形式")] = None,
) -> str:
    """日銀の時系列統計データを系列コード指定で取得する（コードAPI）。

    例: db=FM08, code=FXERD04 → USD/JPY 日次
        db=FM08, code=FXERM07 → USD/JPY 月次平均
    系列コードが不明なら先に boj_meta を呼ぶこと。
    """
    return await _get("/boj/pass", {
        "db": db, "code": code, "startDate": start_date, "endDate": end_date,
    })


@mcp.tool()
async def boj_layer(
    db: Annotated[str, Field(description="DB名。例: FF（資金循環）、BP01（国際収支）")],
    layer: Annotated[str, Field(description="階層情報。カンマ区切り。* でワイルドカード。例: 1,1,1")],
    frequency: Annotated[str, Field(description="期種。CY=暦年 / FY=年度 / Q=四半期 / M=月次 / D=日次")],
    start_date: Annotated[Optional[str], Field(description="開始期。例: 202504")] = None,
    end_date: Annotated[Optional[str], Field(description="終了期")] = None,
) -> str:
    """日銀の時系列統計データを階層指定で一括取得する（階層API）。

    階層情報は boj_meta のレスポンス LAYER1〜LAYER5 列で確認できる。
    """
    return await _get("/boj/layer", {
        "db": db, "layer": layer, "frequency": frequency,
        "startDate": start_date, "endDate": end_date,
    })


# =============================================================================
# FRED（米国経済統計）
# =============================================================================

@mcp.tool()
async def fred_search(
    search_text: Annotated[str, Field(description="検索キーワード。スペース区切りでAND検索")],
    limit: Annotated[Optional[str], Field(description="取得件数。デフォルト1000、最大1000")] = "20",
) -> str:
    """FREDの系列をキーワード検索する。fred_pass で使う series_id を調べる用途。"""
    return await _get("/fred/search", {"search_text": search_text, "limit": limit})


@mcp.tool()
async def fred_meta(
    series_id: Annotated[str, Field(description="FRED系列ID。例: DEXJPUS")],
) -> str:
    """FRED系列のメタ情報（名称・単位・頻度・収録期間）を取得する。"""
    return await _get("/fred/meta", {"series_id": series_id})


@mcp.tool()
async def fred_pass(
    series_id: Annotated[str, Field(description="FRED系列ID")],
    observation_start: Annotated[Optional[str], Field(description="開始日 YYYY-MM-DD")] = None,
    observation_end: Annotated[Optional[str], Field(description="終了日 YYYY-MM-DD")] = None,
    frequency: Annotated[Optional[str], Field(
        description="集計頻度変換。d=日次 / w=週次 / m=月次 / q=四半期 / a=年次"
    )] = None,
) -> str:
    """FREDの時系列データを取得する。

    よく使う系列ID:
      DEXJPUS  USD/JPY為替（日次）    FEDFUNDS FF金利（月次）
      CPIAUCSL 米国CPI季調済（月次）  GDP      米国GDP（四半期）
      UNRATE   米国失業率（月次）      DGS10    10年債利回り（日次）
    """
    return await _get("/fred/pass", {
        "series_id": series_id,
        "observation_start": observation_start,
        "observation_end": observation_end,
        "frequency": frequency,
    })


# =============================================================================
# EIA（米国エネルギー）/ NDL（国立国会図書館）
# =============================================================================

@mcp.tool()
async def eia_pass(
    route: Annotated[str, Field(description="EIA APIのエンドポイントパス")],
    extra_params: Annotated[Optional[str], Field(
        description='その他パラメータをJSON文字列で。例: {"data[]":"value","facets[series][]":"RWTC","frequency":"monthly"}'
    )] = None,
) -> str:
    """米国エネルギー情報局（EIA）のデータを取得する。

    主なroute:
      petroleum/pri/spt/data/            WTI・Brent原油スポット価格
      petroleum/pri/gnd/dcus/nus/data/   米国ガソリン小売価格
      natural-gas/pri/sum/dcus/nus/data/ 米国天然ガス価格
    """
    params = {"route": route}
    params.update(_extra(extra_params))
    return await _get("/eia/pass", params)


@mcp.tool()
async def ndl_pass(
    extra_params: Annotated[str, Field(
        description='NDL OpenSearchのパラメータをJSON文字列で指定。例: {"any":"統計","cnt":"10"}'
    )],
) -> str:
    """国立国会図書館サーチ（NDL OpenSearch）で書誌情報を検索する。"""
    return await _get("/ndl/pass", _extra(extra_params))


# =============================================================================
# EDINET（有価証券報告書）
# =============================================================================

@mcp.tool()
async def edinet_codes(
    filer_name: Annotated[Optional[str], Field(description="提出者名で絞込（部分一致）。例: トヨタ")] = None,
    sec_code: Annotated[Optional[str], Field(description="証券コードで絞込（完全一致）。例: 7203")] = None,
) -> str:
    """EDINET登録企業の一覧を取得する。EDINETコードや決算日を調べる用途。

    絞込なしだと数千件返るため、必ず filer_name か sec_code を指定すること。
    """
    return await _get("/edinet/codes", {"filer_name": filer_name, "sec_code": sec_code})


@mcp.tool()
async def edinet_documents(
    date: Annotated[Optional[str], Field(description="単一日付 YYYY-MM-DD。省略時は当日")] = None,
    date_from: Annotated[Optional[str], Field(description="範囲検索の開始日。最大60日間")] = None,
    date_to: Annotated[Optional[str], Field(description="範囲検索の終了日")] = None,
    doc_type: Annotated[Optional[str], Field(
        description="書類種別コード。120=有価証券報告書 / 140=四半期 / 160=半期 / 180=臨時 / 350=大量保有"
    )] = None,
    period_end: Annotated[Optional[str], Field(
        description="決算日で絞込 YYYY-MM-DD。例: 2025-03-31。date省略時はピンポイント検索（15〜30秒）"
    )] = None,
    edinet_code: Annotated[Optional[str], Field(description="EDINETコード。例: E02144")] = None,
    filer_name: Annotated[Optional[str], Field(description="提出者名（部分一致）")] = None,
) -> str:
    """EDINETの提出書類一覧を取得する。docID を得るために使う。

    特定企業の有報を探すなら period_end + edinet_code の組み合わせが最速。
    date_from を使う範囲検索はレート制限で30日=約30秒かかるため多用しないこと。
    """
    return await _get("/edinet/documents", {
        "date": date, "date_from": date_from, "date_to": date_to,
        "doc_type": doc_type, "period_end": period_end,
        "edinet_code": edinet_code, "filer_name": filer_name,
    })


@mcp.tool()
async def edinet_get(
    doc_ids: Annotated[str, Field(description="書類管理番号。カンマ区切り、最大20件。例: S100XXXX")],
    file: Annotated[Optional[str], Field(
        description="CSVファイル名（部分一致）。省略時はZIP内のファイル一覧を返す。"
                    "例: jpcrp030000-asr（全財務データ）/ BalanceSheet / StatementOfIncome"
    )] = None,
) -> str:
    """EDINETの書類CSVをJSON形式で取得する。

    docID は edinet_documents から得る。csvFlag=1 の書類のみ対象。
    file を省略するとファイル一覧が返るので、まず一覧を見てから指定するとよい。
    """
    return await _get("/edinet/get", {"doc_ids": doc_ids, "file": file})


# =============================================================================
# 国内データ（観光・労働・エネルギー・気象・株価）
# =============================================================================

@mcp.tool()
async def kabuka_get(
    ticker: Annotated[Optional[str], Field(description="ティッカー（yfinance形式）。省略時 ^N225")] = None,
    from_date: Annotated[Optional[str], Field(description="開始日 YYYY-MM-DD")] = None,
    to_date: Annotated[Optional[str], Field(description="終了日 YYYY-MM-DD")] = None,
    interval: Annotated[Optional[str], Field(description="足種。1d=日次 / 1wk=週次 / 1mo=月次")] = None,
) -> str:
    """株価・指数データを取得する（Yahoo Finance）。

    期間を省略すると全期間となり応答が巨大になるため、from_date の指定を推奨。
    """
    return await _get("/kabuka", {
        "ticker": ticker, "from": from_date, "to": to_date, "interval": interval,
    })


@mcp.tool()
async def d_kanko_get(
    type: Annotated[Optional[str], Field(description="pref=都道府県のみ / city=市区町村のみ / 省略=両方")] = None,
    from_date: Annotated[Optional[str], Field(description="開始年月 YYYY-MM")] = None,
    to_date: Annotated[Optional[str], Field(description="終了年月 YYYY-MM")] = None,
    pref: Annotated[Optional[str], Field(description="都道府県名（部分一致）。例: 長野")] = None,
    city: Annotated[Optional[str], Field(description="市区町村名（部分一致）。type=cityのときのみ有効")] = None,
) -> str:
    """観光庁「デジタル観光統計オープンデータ」の来訪者数を取得する。

    単位は千人。パラメータ省略で全件返るため、期間か地域で絞ること。
    """
    return await _get("/d_kanko", {
        "type": type, "from": from_date, "to": to_date, "pref": pref, "city": city,
    })


@mcp.tool()
async def n_roudou_get() -> str:
    """長野労働局の受理地別・産業別 新規求人数（月次）を取得する。

    産業コード: all=合計 / D=建設 / E=製造 / G=情報通信 / H=運輸 / I=卸売小売
    J=金融保険 / K=不動産 / M=宿泊飲食 / N=生活関連 / O=教育 / P=医療福祉
    R=サービス / other=その他
    """
    return await _get("/n_roudou/juri_sangyo")


@mcp.tool()
async def enecho_gasoline(
    item: Annotated[Optional[str], Field(
        description="品目。ハイオク / レギュラー / 軽油 / 灯油店頭 / 灯油配達"
    )] = None,
    region: Annotated[Optional[str], Field(description="地域。全国 または都道府県名。例: 長野")] = None,
    from_date: Annotated[Optional[str], Field(description="開始日 YYYY-MM-DD")] = None,
    to_date: Annotated[Optional[str], Field(description="終了日 YYYY-MM-DD")] = None,
) -> str:
    """資源エネルギー庁の給油所小売価格調査（週次）を取得する。

    収録期間は1990年8月〜と長いため、item と region での絞込を推奨。
    価格の単位は円/L（灯油店頭・配達は円/18L）。
    """
    return await _get("/enecho/gasoline", {
        "item": item, "region": region, "from": from_date, "to": to_date,
    })


@mcp.tool()
async def jma_nagano(
    location: Annotated[Optional[str], Field(
        description="地点名。カンマ区切りで複数可。長野 / 松本 / 諏訪 / 飯田 / 軽井沢。省略時は全5地点"
    )] = None,
    from_date: Annotated[Optional[str], Field(description="開始日 YYYY-MM-DD")] = None,
    to_date: Annotated[Optional[str], Field(description="終了日 YYYY-MM-DD")] = None,
) -> str:
    """長野県5地点の週間天気予報を取得する（毎朝6:00 JST更新、過去分も蓄積）。

    注意: 明後日以降の気温は長野地点のみ値が入り、他4地点はnullになる。
    天気・降水確率も先の日付ほど県全体で1種類の値になる。
    """
    return await _get("/jma/nagano", {
        "location": location, "from": from_date, "to": to_date,
    })
