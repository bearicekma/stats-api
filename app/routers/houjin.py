# 国税庁 法人番号公表サイトの全件データ（全国・閉鎖法人を含む）から作った法人マスタを返すルータ
# データは scripts/collect_houjin.py（GitHub Actions、毎月3日）で都道府県ごとのParquetにしてGCSに置いている
# 全国では数百万行になるため、都道府県（pref）の指定を必須にし、その都道府県のファイルだけを読む
import json
import os
import re
import tempfile
import threading
import unicodedata
from collections import OrderedDict

import duckdb
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse
from google.cloud import storage

router = APIRouter(prefix="/houjin", tags=["法人番号"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
PREFIX      = "houjin/zenken"
META_PATH   = f"{PREFIX}/_meta.json"
PREF_CODES  = [f"{i:02d}" for i in range(1, 48)] + ["99"]   # 99 = 国外
MAX_PREFS   = 5          # 1回に指定できる都道府県の数（Cloud Run のメモリ対策）
JSON_MAX    = 100_000    # JSONで返す行数の上限。これを超える場合は format=csv（ストリーミング）を使う
CACHE_FILES = 4          # 一時ファイルとして保持する都道府県ファイルの数（Cloud Run の /tmp はメモリ上にあるため絞る）
CHUNK_ROWS  = 50_000     # CSVストリーミングで1回に書き出す行数

_cache: "OrderedDict[str, tuple]" = OrderedDict()   # GCSパス → (世代番号, 一時ファイルのパス)
_lock = threading.Lock()


def _local(path: str):
    # GCSのファイルを一時ファイルにして、そのパスを返す。無ければ None
    # 世代番号が同じなら前回の一時ファイルを使い、古いものから順に CACHE_FILES 個までに抑える
    blob = storage.Client().bucket(BUCKET_NAME).get_blob(path)
    if blob is None:
        return None
    with _lock:
        hit = _cache.get(path)
        if hit and hit[0] == blob.generation and os.path.exists(hit[1]):
            _cache.move_to_end(path)
            return hit[1]
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        blob.download_to_filename(tmp_path)
        if hit and os.path.exists(hit[1]):
            os.remove(hit[1])
        _cache[path] = (blob.generation, tmp_path)
        _cache.move_to_end(path)
        while len(_cache) > CACHE_FILES:
            _, (_, old) = _cache.popitem(last=False)
            if os.path.exists(old):
                os.remove(old)
        return tmp_path


def _meta():
    blob = storage.Client().bucket(BUCKET_NAME).get_blob(META_PATH)
    return json.loads(blob.download_as_text()) if blob else None


def _tokens(text, sep: str = ",") -> list[str]:
    return [t.strip() for t in str(text or "").split(sep) if t.strip()]


def _zenkaku(s: str) -> str:
    # 半角英数記号を全角にする（法人番号データの商号は英数字が全角のことが多いため）
    return "".join(chr(ord(c) + 0xFEE0) if "!" <= c <= "~" else ("　" if c == " " else c) for c in s)


def _no_data():
    return JSONResponse(status_code=503, content={"error": "データがまだありません（収集ワークフロー collect-houjin を mode=full で実行してください）"})


def _bad(msg: str, hint: str = None):
    body = {"error": msg}
    if hint:
        body["hint"] = hint
    return JSONResponse(status_code=400, content=body)


@router.get("/master", summary="法人マスタ（国税庁 法人番号 全件データ、閉鎖法人を含む）")
async def get_master(
    pref: str = Query(..., description="都道府県コード2桁（必須）。カンマ区切りで5つまで。99=国外。例: 20（長野県）"),
    city: str = Query(None, description="団体コード5桁（_M_city の code_5_digit）。カンマ区切りで複数可。政令市は区のコード。例: 20202（松本市）"),
    status: str = Query("all", description="all（既定）/ active=存続のみ / closed=閉鎖のみ"),
    kind: str = Query(None, description="法人種別コード3桁。カンマ区切り。例: 301（株式会社）,305（合同会社）"),
    name: str = Query(None, description="商号・フリガナの部分一致。全角・半角の違いを吸収し、ひらがなはカタカナでも照合する。例: 信州"),
    number: str = Query(None, description="法人番号13桁。カンマ区切りで複数可"),
    limit: int = Query(None, ge=1, description="取得件数の上限。省略時は全件"),
    format: str = Query("json", description="json（既定、10万行まで）または csv（件数無制限、ストリーミング）"),
):
    """
    国税庁 法人番号公表サイト「基本3情報ダウンロード」の全件データ（毎月末時点）から作った法人マスタ。
    閉鎖した法人も含む（状態=閉鎖）。法人番号は本店（主たる事務所）の所在地で登録されている。

    ### 使用例
    - 長野県の全法人（Power Query 向け）: `?pref=20&format=csv`
    - 松本市の存続している株式会社: `?pref=20&city=20202&status=active&kind=301`
    - 長野県で名称に「信州」を含む法人: `?pref=20&name=信州`

    ### 列
    法人番号 / 商号 / フリガナ / 英語商号 / 法人種別コード / 法人種別 /
    都道府県コード / 団体コード（5桁、_M_city と結合可）/ 都道府県 / 市区町村 / 丁目番地等 / 郵便番号 / 国外所在地 /
    状態（存続・閉鎖）/ 閉鎖年月日 / 閉鎖事由コード / 閉鎖事由 / 承継先法人番号 /
    法人番号指定年月日 / 変更年月日 / 更新年月日 / 最終処理区分コード / 最終処理区分 / 変更事由の詳細 / 検索対象除外

    ### 注意
    - 東京都などは100万行を超える。Excelのシートには入らないので、Power Query でデータモデルに読み込むか、city・status・kind で絞る
    - JSONは10万行まで。それを超える場合は format=csv を使う
    - 法人種別: 101 国の機関 / 201 地方公共団体 / 301 株式会社 / 302 有限会社 / 303 合名会社 / 304 合資会社 /
      305 合同会社 / 399 その他の設立登記法人 / 401 外国会社等 / 499 その他
    - 閉鎖事由: 01 清算の結了等 / 11 合併による解散等 / 21 登記官による閉鎖 / 31 その他の清算の結了等
    - 出典: 国税庁法人番号公表サイト「基本3情報ダウンロード」（全件データ）を加工して作成
    """
    if format not in ("json", "csv"):
        return _bad("format は json または csv")
    if status not in ("all", "active", "closed"):
        return _bad("status は all / active / closed")
    prefs = list(dict.fromkeys(p.zfill(2) for p in _tokens(pref)))
    bad = [p for p in prefs if p not in PREF_CODES]
    if not prefs or bad:
        return _bad(f"pref が正しくありません: {bad or pref}", "都道府県コード2桁（01〜47）または 99（国外）")
    if len(prefs) > MAX_PREFS:
        return _bad(f"pref は {MAX_PREFS} つまでです")

    where, params = [], []
    cities = [c.zfill(5) for c in _tokens(city)]
    if cities:
        if any(not re.fullmatch(r"\d{5}", c) for c in cities):
            return _bad("city は団体コード5桁（例: 20202）")
        where.append(f"団体コード IN ({','.join('?' * len(cities))})")
        params.extend(cities)
    if status != "all":
        where.append("状態 = ?")
        params.append("存続" if status == "active" else "閉鎖")
    kinds = _tokens(kind)
    if kinds:
        if any(not re.fullmatch(r"\d{3}", k) for k in kinds):
            return _bad("kind は法人種別コード3桁（例: 301）")
        where.append(f"法人種別コード IN ({','.join('?' * len(kinds))})")
        params.extend(kinds)
    nums = _tokens(number)
    if nums:
        if any(not re.fullmatch(r"\d{13}", n) for n in nums):
            return _bad("number は法人番号13桁")
        where.append(f"法人番号 IN ({','.join('?' * len(nums))})")
        params.extend(nums)
    if name and name.strip():
        q = name.strip()
        nf = unicodedata.normalize("NFKC", q)
        kata = "".join(chr(ord(c) + 0x60) if "ぁ" <= c <= "ゖ" else c for c in nf)   # ひらがな→カタカナ（フリガナ照合用）
        pats = list(dict.fromkeys([q, nf, _zenkaku(nf), kata]))
        cond = []
        for p in pats:
            cond.append("(商号 LIKE ? OR フリガナ LIKE ?)")
            params.extend([f"%{p}%", f"%{p}%"])
        where.append("(" + " OR ".join(cond) + ")")

    try:
        paths = []
        for p in prefs:
            lp = _local(f"{PREFIX}/pref={p}.parquet")
            if lp is None:
                return _no_data()
            paths.append(lp)
        src = "read_parquet([" + ", ".join(f"'{x}'" for x in paths) + "])"
        sql = f"SELECT * FROM {src}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY 都道府県コード NULLS LAST, 団体コード NULLS LAST, 法人番号"
        if limit:
            sql += f" LIMIT {int(limit)}"
        meta = _meta() or {}

        if format == "csv":
            con = duckdb.connect()
            reader = con.execute(sql, params).fetch_record_batch(CHUNK_ROWS)

            def _stream():
                # 先頭だけBOM付きヘッダー。以降は CHUNK_ROWS 行ずつCSVにして送る（全件をメモリに載せない）
                try:
                    yield ("﻿" + ",".join(reader.schema.names) + "\n").encode("utf-8")
                    for batch in reader:
                        yield batch.to_pandas().to_csv(index=False, header=False).encode("utf-8")
                finally:
                    con.close()

            return StreamingResponse(_stream(), media_type="text/csv; charset=utf-8",
                                     headers={"X-Houjin-Base-Date": str(meta.get("基準日") or "")})

        con = duckdb.connect()
        n = con.execute(f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
        if n > JSON_MAX:
            con.close()
            return _bad(f"結果が {n:,} 行あり、JSONの上限 {JSON_MAX:,} 行を超えています",
                        "format=csv を使うか、city・status・kind・name で絞り込んでください。先頭だけ見るなら limit")
        df = con.execute(sql, params).df()
        con.close()
        return {"collection": "houjin/master", "基準日": meta.get("基準日"), "count": len(df),
                "data": df.to_dict(orient="records")}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"{type(e).__name__}: {e}"})


@router.get("/meta", summary="法人マスタの基準日と都道府県別の件数")
async def get_meta(format: str = Query("json", description="json（既定）または csv")):
    """
    法人マスタの基準日（全件データの作成時点＝前月末）、取得日時、都道府県別の件数（存続・閉鎖）。
    """
    try:
        meta = _meta()
        if meta is None:
            return _no_data()
        if format == "csv":
            import pandas as pd
            df = pd.DataFrame(meta.get("都道府県別", []))
            df.insert(0, "基準日", meta.get("基準日"))
            return StreamingResponse(iter([df.to_csv(index=False).encode("utf-8-sig")]), media_type="text/csv; charset=utf-8")
        return meta
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"{type(e).__name__}: {e}"})
