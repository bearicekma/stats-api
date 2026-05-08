# EDINET（金融庁 企業開示システム）APIエンドポイント
# /edinet/documents        : 書類一覧API（単一日付・日付範囲検索）
# /edinet/document/{doc_id}: 書類取得API（CSV展開してJSONで返す）

import asyncio
import io
import os
import zipfile
from datetime import date, datetime, timedelta

import httpx
import pandas as pd
from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/edinet", tags=["EDINET 金融庁開示書類"])

EDINET_BASE         = "https://api.edinet-fsa.go.jp/api/v2"
RATE_LIMIT_SECONDS  = 3   # EDINET API推奨のリクエスト間隔（秒）
MAX_DATE_RANGE_DAYS = 60  # 日付範囲検索の上限


def _api_key() -> str:
    # 環境変数からAPIキーを取得する
    key = os.environ.get("EDINET", "")
    if not key:
        raise RuntimeError("環境変数 EDINET が設定されていません")
    return key


async def _fetch_one_day(date_str: str, api_key: str) -> list:
    # 1日分の書類一覧を取得する（日付範囲ループ用）
    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            f"{EDINET_BASE}/documents.json",
            params={"date": date_str, "type": "2", "Subscription-Key": api_key},
        )
    if res.status_code != 200:
        return []
    return res.json().get("results") or []


@router.get(
    "/documents",
    summary="書類一覧API — 指定日または期間に提出された書類一覧を取得",
)
async def edinet_documents(request: Request):
    """
    指定した日付または期間に提出された書類の一覧をEDINET APIから取得します。
    APIキーは不要（サーバー側で付与）。

    **クエリパラメータ:**
    - `date` (必須 / date_fromと二択) 単一日付（YYYY-MM-DD）
    - `date_from` (必須 / dateと二択) 範囲検索の開始日（YYYY-MM-DD）
    - `date_to` (任意) 範囲検索の終了日（YYYY-MM-DD）。省略時は当日。最大60日間
    - `doc_type` (任意) 書類種別コードで絞込（下記参照）
    - `period_end` (任意) 決算日で絞込（YYYY-MM-DD）。例: 3月決算なら `2026-03-31`

    **⚠️ 日付範囲検索の注意:**
    EDINET APIのレート制限（3秒/リクエスト）により、範囲が広いほど時間がかかります。
    30日間の場合、約90秒かかります。

    **レスポンスフィールド（results[]）:**
    - `docID` 書類管理番号（書類取得APIで使用）
    - `filerName` 提出者名
    - `secCode` 証券コード
    - `edinetCode` EDINETコード
    - `docTypeCode` 書類種別コード
    - `docDescription` 書類名称
    - `submitDateTime` 提出日時
    - `periodStart` / `periodEnd` 対象会計期間
    - `csvFlag` CSVファイル有無（`1`=あり）
    - `xbrlFlag` XBRLファイル有無（`1`=あり）
    - `pdfFlag` PDFファイル有無（`1`=あり）

    **書類種別コード（doc_type）一覧:**
    - `010` 有価証券通知書 / `020` 変更通知書（有価証券通知書）
    - `030` 有価証券届出書 / `040` 訂正有価証券届出書
    - `080` 発行登録書 / `090` 訂正発行登録書 / `100` 発行登録追補書類
    - `120` **有価証券報告書** / `130` 訂正有価証券報告書
    - `135` 確認書 / `136` 訂正確認書
    - `140` **四半期報告書** / `150` 訂正四半期報告書
    - `160` **半期報告書** / `170` 訂正半期報告書
    - `180` **臨時報告書** / `190` 訂正臨時報告書
    - `200` 親会社等状況報告書 / `210` 訂正親会社等状況報告書
    - `220` 自己株券買付状況報告書 / `230` 訂正自己株券買付状況報告書
    - `235` 内部統制報告書 / `236` 訂正内部統制報告書
    - `240` 公開買付届出書 / `250` 訂正公開買付届出書
    - `270` 公開買付報告書 / `280` 訂正公開買付報告書
    - `290` 意見表明報告書 / `300` 訂正意見表明報告書
    - `310` 対質問回答報告書 / `320` 訂正対質問回答報告書
    - `350` **大量保有報告書**

    **URL例:**
    - `/edinet/documents?date=2026-05-07` 単一日付
    - `/edinet/documents?date=2026-05-07&doc_type=120` 有価証券報告書のみ
    - `/edinet/documents?date_from=2025-06-01&date_to=2025-07-31&doc_type=120&period_end=2025-03-31`
    """
    params     = dict(request.query_params)
    date_single = params.get("date")
    date_from   = params.get("date_from")
    date_to     = params.get("date_to")
    doc_type    = params.get("doc_type")
    period_end  = params.get("period_end")

    api_key = _api_key()

    # date または date_from が必須
    if not date_single and not date_from:
        return JSONResponse(status_code=400, content={
            "error":          "date または date_from パラメータは必須です",
            "example_single": "/edinet/documents?date=2026-05-07",
            "example_range":  "/edinet/documents?date_from=2025-06-01&date_to=2025-07-31&doc_type=120",
        })

    def _filter(results: list) -> list:
        # doc_type・period_end でフィルタリングする
        if doc_type:
            results = [r for r in results if r.get("docTypeCode") == doc_type]
        if period_end:
            results = [r for r in results if r.get("periodEnd") == period_end]
        return results

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
            "error": f"日付範囲は最大{MAX_DATE_RANGE_DAYS}日までです",
            "hint":  "レート制限のため範囲が広いほど時間がかかります（約3秒/日）",
        })

    all_results  = []
    current      = from_date
    dates_searched = 0

    while current <= to_date:
        results = await _fetch_one_day(current.strftime("%Y-%m-%d"), api_key)
        all_results.extend(results)
        dates_searched += 1
        current += timedelta(days=1)
        if current <= to_date:
            await asyncio.sleep(RATE_LIMIT_SECONDS)

    return {
        "count":          len(_filter(all_results)),
        "dates_searched": dates_searched,
        "results":        _filter(all_results),
    }


@router.get(
    "/document/{doc_id}",
    summary="書類取得API — CSVファイルをJSON形式で取得",
)
async def edinet_document(doc_id: str, request: Request):
    """
    書類管理番号（docID）を指定してCSVファイルをJSON形式で取得します。
    書類管理番号は `/edinet/documents` の `results[].docID` から取得してください。
    APIキーは不要（サーバー側で付与）。

    **取得できる書類:** `csvFlag = "1"` の書類のみ対象です。

    **クエリパラメータ:**
    - `file` (任意) 取得するCSVファイル名（部分一致）。省略時はZIP内のファイル一覧を返します

    **fileパラメータ省略時のレスポンス:**
```json
    {
      "doc_id": "S100XXXX",
      "files": ["jpcrp_cor-BalanceSheet.csv", "jpcrp_cor-StatementOfIncome.csv", ...]
    }
```

    **file指定時のレスポンス:**
```json
    {
      "doc_id": "S100XXXX",
      "file": "jpcrp_cor-BalanceSheet.csv",
      "count": 120,
      "data": [{"要素ID": "...", "コンテキストID": "...", "値": "...", "単位ID": "..."}, ...]
    }
```

    **主なfileキーワード:**
    - `BalanceSheet` 貸借対照表
    - `StatementOfIncome` 損益計算書
    - `StatementOfCashFlows` キャッシュフロー計算書
    - `StatementOfChanges` 株主資本等変動計算書

    **URL例:**
    - `/edinet/document/S100XXXX` ファイル一覧を取得
    - `/edinet/document/S100XXXX?file=BalanceSheet` 貸借対照表を取得
    - `/edinet/document/S100XXXX?file=StatementOfIncome` 損益計算書を取得
    """
    file_filter = dict(request.query_params).get("file")
    api_key     = _api_key()

    # type=5（CSV）でZIPをダウンロードする
    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.get(
            f"{EDINET_BASE}/documents/{doc_id}",
            params={"type": "5", "Subscription-Key": api_key},
        )

    if res.status_code != 200:
        return JSONResponse(status_code=res.status_code, content={
            "error": f"EDINET APIエラー: {res.status_code}", "doc_id": doc_id
        })

    # ZIPをメモリ上で展開する
    try:
        zf = zipfile.ZipFile(io.BytesIO(res.content))
    except zipfile.BadZipFile:
        return JSONResponse(status_code=500, content={
            "error": "ZIPファイルの展開に失敗しました", "doc_id": doc_id
        })

    # XBRL_TO_CSV ディレクトリ内のCSVファイル一覧を取得する
    csv_files = [
        name for name in zf.namelist()
        if name.startswith("XBRL_TO_CSV/") and name.endswith(".csv")
    ]

    # fileパラメータ未指定の場合はファイル一覧を返す
    if not file_filter:
        return {
            "doc_id": doc_id,
            "files":  [f.replace("XBRL_TO_CSV/", "") for f in csv_files],
        }

    # 部分一致でファイルを検索する
    matched = [f for f in csv_files if file_filter.lower() in f.lower()]
    if not matched:
        return JSONResponse(status_code=404, content={
            "error":           f"ファイルが見つかりません: {file_filter}",
            "available_files": [f.replace("XBRL_TO_CSV/", "") for f in csv_files],
        })

    # 最初にマッチしたファイルをUTF-16・タブ区切りで読み込む
    target = matched[0]
    try:
        df = pd.read_csv(
            io.BytesIO(zf.read(target)),
            encoding="utf-16",
            sep="\t",
            dtype=str,
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"CSV読み込みエラー: {str(e)}"})

    return {
        "doc_id": doc_id,
        "file":   target.replace("XBRL_TO_CSV/", ""),
        "count":  len(df),
        "data":   df.to_dict(orient="records"),
    }
