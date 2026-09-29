# ハローワーク 長野県新着求人エンドポイント
# /hellowork/kyujin : 新着求人の詳細データ（取得月ごとの Parquet を束ねて返す）
# /hellowork/months : 保存済みの取得月の一覧

import os
import re
import tempfile
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd
from fastapi           import APIRouter, Query
from fastapi.responses import JSONResponse, Response
from google.cloud      import storage

router = APIRouter(prefix="/hellowork", tags=["ハローワーク 新着求人"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
DATA_PREFIX = "hellowork/kyujin/"
JST         = timezone(timedelta(hours=9))
MONTH_RE    = re.compile(r"^(\d{4})-?(\d{2})$")


def _list_months(bucket) -> list[str]:
    # 保存済みの取得月（YYYYMM）を昇順で返す
    names = [b.name for b in bucket.list_blobs(prefix=DATA_PREFIX)]
    return sorted(m.group(1) for n in names for m in [re.search(r"/(\d{6})\.parquet$", n)] if m)


def _to_yyyymm(value: str | None) -> str | None:
    # 「2026-09」「202609」のどちらでも受け付ける
    if not value:
        return None
    m = MONTH_RE.match(value.strip())
    if not m:
        raise ValueError(f"月の形式が不正です: {value}（例: 2026-09）")
    return m.group(1) + m.group(2)


@router.get("/months", summary="保存済みの取得月一覧")
async def get_months():
    """保存済みの取得月（`from` / `to` に指定できる値）を返します。"""
    try:
        bucket = storage.Client().bucket(BUCKET_NAME)
        months = _list_months(bucket)
        return {"count": len(months), "months": [f"{m[:4]}-{m[4:]}" for m in months]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/kyujin", summary="長野県 新着求人（詳細）")
async def get_kyujin(
    from_:  str | None = Query(None, alias="from", description="開始の取得月（YYYY-MM）。省略時は当月"),
    to:     str | None = Query(None, description="終了の取得月（YYYY-MM）。省略時は from と同じ月"),
    kind:   str | None = Query(None, description="求人種別で絞込（一般 / 障害者）"),
    format: str        = Query("json", description="json（既定）または csv"),
):
    """
    ハローワークインターネットサービスに掲載された、長野県内が就業場所の新着求人（詳細ページの内容）を返します。
    毎晩 20:00〜20:45 に「新着（直近3日）」の一般求人・障害者求人を検索し、未取得の求人だけを保存しています。

    ---

    **クエリパラメータ:**
    - `from` (任意) 開始の取得月（`2026-09` または `202609`）。省略時は当月
    - `to` (任意) 終了の取得月。省略時は `from` と同じ月
    - `kind` (任意) `一般` または `障害者`
    - `format` (任意) `json`（既定）または `csv`

    **主な列（全187列）:**
    - `求人番号`（主キー）/ `求人種別`（一般・障害者）/ `就業形態`（フルタイム・パート）
    - `取得日` / `取得月` / `受付年月日` / `紹介期限日`
    - `事業所名` / `法人番号` / `産業分類` / `職種` / `仕事内容` / `雇用形態`
    - `産業分類_大分類コード` / `産業分類_中分類コード` / `産業分類_小分類コード`（`_M_sangyo` のコード）
    - `就業場所_住所` / `就業場所_市区町村` / `就業場所_市区町村コード`（`_M_city` の5桁コード）
    - `賃金` / `賃金形態` / `賃金下限` / `賃金上限`（整数）/ `年間休日数`（整数）
    - `従業員数_企業全体` など従業員数4列（整数）
    - `資本金`（整数・円単位。例: 8,550万円 → 85500000）
    - `その他項目`（対応表にない項目をJSON文字列で保持）

    担当者の氏名・カナ・メールアドレスは保存していません。

    **URL例:**
    - `/hellowork/kyujin` 当月分
    - `/hellowork/kyujin?from=2026-09&to=2026-12&format=csv`
    - `/hellowork/kyujin?from=2026-10&kind=障害者`

    ※出典: ハローワークインターネットサービス（厚生労働省）。求人内容は取得時点のものです。
    """
    tmp_paths = []
    try:
        start = _to_yyyymm(from_) or datetime.now(JST).strftime("%Y%m")
        end   = _to_yyyymm(to) or start
        if kind is not None and kind not in ("一般", "障害者"):
            return JSONResponse(status_code=400, content={"error": "kind は 一般 または 障害者 を指定してください"})

        bucket  = storage.Client().bucket(BUCKET_NAME)
        targets = [m for m in _list_months(bucket) if start <= m <= end]
        if not targets:
            return JSONResponse(status_code=404, content={"error": f"{start}〜{end} のデータはありません", "hint": "/hellowork/months で保存済みの月を確認できます"})

        # 該当月のファイルだけを一時フォルダにダウンロードし、DuckDBでまとめて読む
        for m in targets:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_paths.append(tmp.name)
            bucket.blob(f"{DATA_PREFIX}{m}.parquet").download_to_filename(tmp_paths[-1])

        files  = ", ".join(f"'{p}'" for p in tmp_paths)
        where  = "WHERE 求人種別 = ?" if kind else ""
        sql    = f"SELECT * FROM read_parquet([{files}], union_by_name=true) {where} ORDER BY 取得日, 求人番号"
        cur    = duckdb.connect().execute(sql, [kind] if kind else [])
        cols   = [d[0] for d in cur.description]
        rows   = cur.fetchall()

        if format == "csv":
            df = pd.DataFrame(rows, columns=cols)
            return Response(content=df.to_csv(index=False).encode("utf-8-sig"), media_type="text/csv; charset=utf-8")
        data = [dict(zip(cols, r)) for r in rows]
        return {"count": len(data), "months": targets, "updated_at": str(datetime.now()), "data": data}
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        for p in tmp_paths:
            if os.path.exists(p):
                os.remove(p)
