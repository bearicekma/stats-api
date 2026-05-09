# EDINET（金融庁 企業開示システム）APIエンドポイント
# /edinet/documents : 書類一覧API（単一日付・日付範囲・ピンポイント検索）
# /edinet/document  : 書類取得API（CSV展開・複数件対応）
# /edinet/codes     : EDINETコードリスト（全提出者一覧）

import asyncio
import calendar
import io
import os
import zipfile
from datetime import date, datetime, timedelta

import httpx
import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/edinet", tags=["EDINET 金融庁開示書類"])

EDINET_BASE         = "https://api.edinet-fsa.go.jp/api/v2"
RATE_LIMIT_SECONDS  = 1
MAX_DATE_RANGE_DAYS = 60


def _api_key() -> str:
    key = os.environ.get("EDINET", "")
    if not key:
        raise RuntimeError("環境変数 EDINET が設定されていません")
    return key


def _pinpoint_dates(period_end_str: str) -> list[date]:
    # period_endから提出日の候補を優先度順に返す
    # 提出は決算月+3ヶ月後半に集中するため、そこから検索を開始する
    pe = datetime.strptime(period_end_str, "%Y-%m-%d").date()
    m2 = (pe + relativedelta(months=2)).replace(day=1)
    m3 = (pe + relativedelta(months=3)).replace(day=1)
    m4 = (pe + relativedelta(months=4)).replace(day=1)

    def days_of(yr, mo, d_from, d_to):
        last = calendar.monthrange(yr, mo)[1]
        return [date(yr, mo, d) for d in range(d_from, min(d_to, last) + 1)]

    return (
        list(reversed(days_of(m3.year, m3.month, 16, 31))) +  # ① +3ヶ月後半（最多）
        list(reversed(days_of(m2.year, m2.month, 16, 31))) +  # ② +2ヶ月後半（早期）
        list(reversed(days_of(m3.year, m3.month,  1, 15))) +  # ③ +3ヶ月前半
        list(reversed(days_of(m4.year, m4.month,  1, 31))) +  # ④ +4ヶ月（遅延）
        list(reversed(days_of(m2.year, m2.month,  1, 15)))    # ⑤ +2ヶ月前半
    )


async def _fetch_one_day(date_str: str, api_key: str) -> list:
    # 1日分の書類一覧を取得する
    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            f"{EDINET_BASE}/documents.json",
            params={"date": date_str, "type": "2", "Subscription-Key": api_key},
        )
    if res.status_code != 200:
        return []
    return res.json().get("results") or []


async def _pinpoint_search(period_end_str: str, api_key: str, filter_fn) -> tuple[list, int]:
    # 優先度順に1日ずつ検索し、結果が見つかった時点で即座に返す
    candidates     = _pinpoint_dates(period_end_str)
    dates_searched = 0
    today          = date.today()

    for d in candidates:
        if d > today:
            continue
        results = await _fetch_one_day(d.isoformat(), api_key)
        dates_searched += 1
        filtered = filter_fn(results)
        if filtered:
            return filtered, dates_searched
        await asyncio.sleep(RATE_LIMIT_SECONDS)

    return [], dates_searched


@router.get(
    "/documents",
    summary="書類一覧API — 指定日または期間に提出された書類一覧を取得",
)
async def edinet_documents(request: Request):
    """
    指定した日付または期間に提出された書類の一覧をEDINET APIから取得します。
    APIキーは不要（サーバー側で付与）。

    **クエリパラメータ:**
    - `date` (任意) 単一日付（YYYY-MM-DD）。省略時は当日
    - `date_from` (任意) 範囲検索の開始日（YYYY-MM-DD）。指定時はdateより優先
    - `date_to` (任意) 範囲検索の終了日（YYYY-MM-DD）。省略時は当日。最大60日間
    - `doc_type` (任意) 書類種別コードで絞込（下記参照）
    - `period_end` (任意) 決算日で絞込（YYYY-MM-DD）。例: 3月決算なら `2025-03-31`
    - `edinet_code` (任意) EDINETコードで絞込（完全一致）。例: `E02144`
    - `filer_name` (任意) 提出者名で絞込（部分一致）。例: `トヨタ`

    **period_end 指定時の動作:**
    - `date` / `date_from` を省略した場合、ピンポイント検索を行います
    - 決算月+3ヶ月後半 → +2ヶ月後半 → +3ヶ月前半 → +4ヶ月 の優先順で検索します
    - 該当書類が見つかった時点で即座に返却します（通常15〜30秒）
    - `date` / `date_from` を同時指定した場合は通常の日付指定検索になります

    **⚠️ 日付範囲検索（date_from指定時）の注意:**
    1秒/リクエストのレート制限があります。30日間の場合、約30秒かかります。最大60日間。

    **レスポンスフィールド（results[]）:**
    - `docID` 書類管理番号（書類取得APIで使用）
    - `filerName` 提出者名
    - `edinetCode` EDINETコード
    - `secCode` 証券コード
    - `docTypeCode` 書類種別コード
    - `docDescription` 書類名称
    - `submitDateTime` 提出日時
    - `periodStart` / `periodEnd` 対象会計期間
    - `csvFlag` CSVファイル有無（`1`=あり）
    - `pdfFlag` PDFファイル有無（`1`=あり）

    **書類種別コード（doc_type）一覧:**
    - `010` 有価証券通知書 / `030` 有価証券届出書 / `040` 訂正有価証券届出書
    - `080` 発行登録書 / `090` 訂正発行登録書 / `100` 発行登録追補書類
    - `120` **有価証券報告書** / `130` 訂正有価証券報告書
    - `135` 確認書 / `136` 訂正確認書
    - `140` **四半期報告書** / `150` 訂正四半期報告書
    - `160` **半期報告書** / `170` 訂正半期報告書
    - `180` **臨時報告書** / `190` 訂正臨時報告書
    - `200` 親会社等状況報告書 / `220` 自己株券買付状況報告書
    - `235` 内部統制報告書 / `236` 訂正内部統制報告書
    - `240` 公開買付届出書 / `270` 公開買付報告書
    - `290` 意見表明報告書 / `310` 対質問回答報告書
    - `350` **大量保有報告書**

    **URL例:**
    - `/edinet/documents` 当日の書類一覧
    - `/edinet/documents?doc_type=120` 当日の有価証券報告書
    - `/edinet/documents?date=2026-05-07&doc_type=120` 指定日の有価証券報告書
    - `/edinet/documents?period_end=2025-03-31&doc_type=120` 3月決算をピンポイント検索
    - `/edinet/documents?period_end=2025-03-31&edinet_code=E02144` 特定企業をピンポイント検索
    - `/edinet/documents?date_from=2025-06-01&date_to=2025-07-31&filer_name=トヨタ` 範囲検索
    """
    params      = dict(request.query_params)
    date_single = params.get("date")
    date_from   = params.get("date_from")
    date_to     = params.get("date_to")
    doc_type    = params.get("doc_type")
    period_end  = params.get("period_end")
    edinet_code = params.get("edinet_code")
    filer_name  = params.get("filer_name")

    try:
        api_key = _api_key()
    except RuntimeError as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    def _filter(results: list) -> list:
        if doc_type:
            results = [r for r in results if r.get("docTypeCode") == doc_type]
        if period_end:
            results = [r for r in results if r.get("periodEnd") == period_end]
        if edinet_code:
            results = [r for r in results if r.get("edinetCode") == edinet_code]
        if filer_name:
            results = [r for r in results
                       if filer_name.lower() in (r.get("filerName") or "").lower()]
        return results

    # period_end指定かつdate/date_from未指定 → ピンポイント検索
    if period_end and not date_single and not date_from:
        results, searched = await _pinpoint_search(period_end, api_key, _filter)
        return {"count": len(results), "dates_searched": searched, "results": results}

    # date・date_from 省略時は当日をデフォルトにする
    if not date_single and not date_from:
        date_single = date.today().isoformat()

    # ── 単一日付 ─────────────────────────────────────────
    if date_single:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.get(
                f"{EDINET_BASE}/documents.json",
                params={"date": date_single, "type": "2", "Subscription-Key": api_key},
            )
        if res.status_code != 200:
            return JSONResponse(status_code=res.status_code, content={
                "error": f"EDINET APIエラー: {res.status_code}", "body": res.text[:200]
            })
        results = _filter(res.json().get("results") or [])
        return {"count": len(results), "dates_searched": 1, "results": results}

    # ── 日付範囲 ─────────────────────────────────────────
    from_date = datetime.strptime(date_from, "%Y-%m-%d").date()
    to_date   = datetime.strptime(date_to, "%Y-%m-%d").date() if date_to else date.today()

    if (to_date - from_date).days < 0:
        return JSONResponse(status_code=400, content={
            "error": "date_from は date_to より前の日付を指定してください"
        })
    if (to_date - from_date).days > MAX_DATE_RANGE_DAYS:
        return JSONResponse(status_code=400, content={
            "error": f"日付範囲は最大{MAX_DATE_RANGE_DAYS}日までです（約1秒/日）",
        })

    all_results    = []
    current        = from_date
    dates_searched = 0

    while current <= to_date:
        results = await _fetch_one_day(current.strftime("%Y-%m-%d"), api_key)
        all_results.extend(results)
        dates_searched += 1
        current += timedelta(days=1)
        if current <= to_date:
            await asyncio.sleep(RATE_LIMIT_SECONDS)

    filtered = _filter(all_results)
    return {"count": len(filtered), "dates_searched": dates_searched, "results": filtered}


@router.get(
    "/get",
    summary="書類取得API — CSVファイルをJSON形式で取得（複数件対応）",
)
async def edinet_document(request: Request):
    """
    書類管理番号（docID）を指定してCSVファイルをJSON形式で取得します。
    複数件同時取得に対応しています（最大20件）。
    書類管理番号は `/edinet/documents` の `results[].docID` から取得してください。
    APIキーは不要（サーバー側で付与）。

    **取得できる書類:** `csvFlag = "1"` の書類のみ対象です。

    **クエリパラメータ:**
    - `doc_ids` (必須) 書類管理番号（カンマ区切り、最大20件）。例: `S100XXXX,S100YYYY`
    - `file` (任意) 取得するCSVファイル名（部分一致）。省略時はZIP内のファイル一覧を返します

    **fileパラメータ省略時のレスポンス:**
    - `count` 取得件数
    - `data.{docID}.files` ZIP内のCSVファイル名一覧

    **file指定時のレスポンス:**
    - `count` 取得件数
    - `data.{docID}.file` 取得したCSVファイル名
    - `data.{docID}.count` レコード数
    - `data.{docID}.data` CSVの内容（JSON形式）

    **エラー時:** 該当doc_idのみエラー内容を格納し、他は正常返却します。

    **主なCSVファイルキーワード（fileパラメータ）:**
    - `jpcrp030000-asr` 有価証券報告書の全財務データ（推奨）
    - `BalanceSheet` 貸借対照表
    - `StatementOfIncome` 損益計算書
    - `StatementOfCashFlows` キャッシュフロー計算書
    - `StatementOfChanges` 株主資本等変動計算書

    **タクソノミ名の体系:**
    - `jpcrp` 有価証券報告書・四半期・半期の財務情報
    - `jpaud` 監査報告書・追加監査情報
    - 様式コード `030000`=有価証券報告書 / `040000`=四半期 / `060000`=半期

    **URL例:**
    - `/edinet/document?doc_ids=S100XXXX` 単一取得（ファイル一覧）
    - `/edinet/document?doc_ids=S100XXXX,S100YYYY&file=jpcrp030000-asr` 複数取得
    - `/edinet/document?doc_ids=S100XXXX&file=BalanceSheet` 貸借対照表を取得
    """
    params      = dict(request.query_params)
    doc_ids_raw = params.get("doc_ids", "")
    file_filter = params.get("file")

    if not doc_ids_raw:
        return JSONResponse(status_code=400, content={
            "error":   "doc_ids パラメータは必須です",
            "example": "/edinet/document?doc_ids=S100XXXX,S100YYYY&file=jpcrp030000-asr",
        })

    doc_ids = [d.strip() for d in doc_ids_raw.split(",") if d.strip()]
    if len(doc_ids) > 20:
        return JSONResponse(status_code=400, content={
            "error": f"doc_ids は最大20件までです（指定: {len(doc_ids)}件）"
        })

    try:
        api_key = _api_key()
    except RuntimeError as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    async def _fetch_one(doc_id: str) -> tuple[str, dict]:
        async with httpx.AsyncClient(timeout=60) as client:
            res = await client.get(
                f"{EDINET_BASE}/documents/{doc_id}",
                params={"type": "5", "Subscription-Key": api_key},
            )
        if res.status_code != 200:
            return doc_id, {"error": f"EDINET APIエラー: {res.status_code}"}

        try:
            zf = zipfile.ZipFile(io.BytesIO(res.content))
        except zipfile.BadZipFile:
            return doc_id, {"error": "ZIPファイルの展開に失敗しました"}

        csv_files = [
            name for name in zf.namelist()
            if name.startswith("XBRL_TO_CSV/") and name.endswith(".csv")
        ]

        if not file_filter:
            return doc_id, {"files": [f.replace("XBRL_TO_CSV/", "") for f in csv_files]}

        matched = [f for f in csv_files if file_filter.lower() in f.lower()]
        if not matched:
            return doc_id, {
                "error":           f"ファイルが見つかりません: {file_filter}",
                "available_files": [f.replace("XBRL_TO_CSV/", "") for f in csv_files],
            }

        target = matched[0]
        try:
            df = pd.read_csv(
                io.BytesIO(zf.read(target)),
                encoding="utf-16",
                sep="\t",
                dtype=str,
            )
        except Exception as e:
            return doc_id, {"error": f"CSV読み込みエラー: {str(e)}"}

        return doc_id, {
            "file":  target.replace("XBRL_TO_CSV/", ""),
            "count": len(df),
            "data":  df.to_dict(orient="records"),
        }

    results = await asyncio.gather(*[_fetch_one(doc_id) for doc_id in doc_ids])
    data    = {doc_id: result for doc_id, result in results}
    return {"count": len(data), "data": data}


@router.get(
    "/codes",
    summary="EDINETコードリスト — 全提出者一覧を取得",
)
async def edinet_codes(request: Request):
    """
    EDINETに登録された全提出者の一覧をEDINET公開ZIPから取得します。
    APIキーは不要。

    **クエリパラメータ:**
    - `filer_name` (任意) 提出者名で絞込（部分一致）。例: `トヨタ`
    - `sec_code` (任意) 証券コードで絞込（完全一致）。例: `7203`

    **レスポンスフィールド（data[]）:**
    - `EDINETコード` EDINETコード（/edinet/documentsのedinet_codeと対応）
    - `提出者名` 企業・ファンド名
    - `証券コード` 上場企業の証券コード（非上場はnull）
    - `提出者業種` 業種区分
    - `上場区分` 上場・非上場
    - `決算日` 決算月日（MM/DD形式）
    - `提出者法人番号` 法人番号

    **URL例:**
    - `/edinet/codes` 全件取得（数千件）
    - `/edinet/codes?filer_name=トヨタ` トヨタを含む企業を絞込
    - `/edinet/codes?sec_code=7203` 証券コード7203で絞込
    """
    params     = dict(request.query_params)
    filer_name = params.get("filer_name")
    sec_code   = params.get("sec_code")

    url = "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip"
    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.get(url)

    if res.status_code != 200:
        return JSONResponse(status_code=res.status_code, content={
            "error": f"EDINETコードリストのダウンロードに失敗しました: {res.status_code}"
        })

    try:
        zf       = zipfile.ZipFile(io.BytesIO(res.content))
        csv_name = [f for f in zf.namelist() if f.endswith(".csv")][0]
        df       = pd.read_csv(
            io.BytesIO(zf.read(csv_name)),
            encoding="cp932",
            skiprows=1,
            dtype=str,
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"CSV解析エラー: {str(e)}"})

    if filer_name:
        col = [c for c in df.columns if "提出者名" in c and "英字" not in c and "ヨミ" not in c]
        if col:
            df = df[df[col[0]].str.contains(filer_name, case=False, na=False)]
    if sec_code:
        col = [c for c in df.columns if "証券コード" in c]
        if col:
            df = df[df[col[0]] == sec_code]

    return {"count": len(df), "data": df.to_dict(orient="records")}
