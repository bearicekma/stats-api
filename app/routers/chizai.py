# 地方財政状況調査（総務省）調査表データを返すルータ
# e-StatのDBは2017年度で更新停止のため、ファイル提供CSVを縦持ちにしたParquet（GCS）から返す
# データは scripts/collect_chizai.py（GitHub Actions）で作成・更新する
import os
import re
import tempfile
from datetime import datetime

import duckdb
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response
from google.cloud import storage

from app.collectors.chizai import _norm as norm_name   # 名称の正規化（索引作成と同じ規則）

router = APIRouter(prefix="/chizai", tags=["地方財政状況調査"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
PREFIX = "chizai"
KUBUN_LIST = ["pref"]   # 市町村分を取り込んだら "city" を追加


def _download(path: str):
    # GCSのParquetを一時ファイルに落としてパスを返す。無ければNone
    blob = storage.Client().bucket(BUCKET_NAME).blob(path)
    if not blob.exists():
        return None
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    blob.download_to_filename(tmp_path)
    return tmp_path


def _codes(text, width: int) -> list[str]:
    # "1,2,03" → ["01","02","03"]（カンマ区切り・0埋め）
    if not text:
        return []
    return [c.strip().zfill(width) for c in str(text).split(",") if c.strip()]


def _name_list(text) -> list[str]:
    # "市中銀行|ゆうちょ銀行" → 正規化した名称のリスト（区切りは |）
    if not text:
        return []
    return [norm_name(t) for t in str(text).split("|") if t.strip()]


def _query(path: str, where: list[str], params: list, order: str, limit, fmt: str, label: str, names: list = None, partial: bool = False):
    # Parquetを DuckDB で絞り込み、JSON または CSV で返す共通処理
    # names: [(列名, 正規化名称リスト)]。表記ゆれ（空白・中黒・名称中の年度）を無視して名称で絞り込む
    tmp_path = None
    try:
        tmp_path = _download(path)
        if tmp_path is None:
            return JSONResponse(status_code=404, content={"error": f"データがありません: {label}", "hint": "表番号は /chizai/tables で確認できます"})
        src = f"read_parquet('{tmp_path}')"
        con = duckdb.connect()
        where, params = list(where), list(params)
        for col, targets in names or []:
            if not targets:
                continue
            # 名称の種類は少ないので、重複を除いた名称だけPythonで正規化して照合する
            raw = [r[0] for r in con.execute(f"SELECT DISTINCT {col} FROM {src}").fetchall() if r[0] is not None]
            if partial:
                hit = [r for r in raw if any(t in norm_name(r) for t in targets)]
            else:
                hit = [r for r in raw if norm_name(r) in targets]
            if not hit:
                con.close()
                return {"collection": label, "updated_at": str(datetime.now()), "count": 0, "data": [],
                        "hint": f"{col}に一致するものがありません。名称は /chizai/meta で確認してください"}
            where.append(f"{col} IN ({','.join('?' * len(hit))})")
            params.extend(hit)
        sql = f"SELECT * FROM {src}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {order}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        df = con.execute(sql, params).df()
        con.close()

        if fmt == "csv":
            return Response(content=df.to_csv(index=False).encode("utf-8-sig"), media_type="text/csv; charset=utf-8")
        data = df.to_dict(orient="records")
        return {"collection": label, "updated_at": str(datetime.now()), "count": len(data), "data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


def _check(kubun: str, hyo: str = None):
    # 入力チェック。問題があればエラーレスポンスを返す
    if kubun not in KUBUN_LIST:
        return JSONResponse(status_code=400, content={"error": f"kubun は {KUBUN_LIST} のいずれか"})
    if hyo is not None and not re.fullmatch(r"\d{1,2}", hyo):
        return JSONResponse(status_code=400, content={"error": "hyo は表番号（数字2桁）。例: 02"})
    return None


@router.get("/tables", summary="表の一覧")
async def get_tables(
    kubun: str = Query("pref", description="pref=都道府県分"),
    hyo: str = Query(None, description="表番号で絞込。例: 46"),
    nendo: int = Query(None, description="この決算年度（西暦）に収録がある表だけ返す。例: 2024"),
    format: str = Query("json", description="json（既定）または csv"),
):
    """
    地方財政状況調査（調査表）の表番号・表名称・収録年度の一覧。

    **同じ表番号が、年度によって別の表に使われていることがあります**（例: 表46は
    1989〜2004年度が「公共用地先行取得の状況」、2011年度以降が「歳入内訳（復旧・復興事業分）」）。
    このため、1行は「表番号 × 表（内容）」の単位で、その表としての収録年度を持ちます。

    ### 列
    - `表名称` その表の最新年度の名称
    - `決算年度_最初` / `決算年度_最新` / `年度数` その名称で収録されている年度。`年度数` が最初〜最新の年数より少なければ途中に欠けがあります
    - `件数` データ件数
    - `表記ゆれ` 同じ表とみなした別の名称（省略形や、様式改正による「その3」→「その2」の繰り上げなど）。行・列の項目名が半分以上重なれば同じ表とみなします。空白・中黒・名称中の年度だけの違いはここには出しません
    - `同番号の別表` true なら、同じ表番号に別の表がある。`/chizai/data` で年度を区切って使ってください
    """
    err = _check(kubun, hyo)
    if err:
        return err
    where, params = [], []
    if hyo is not None:
        where.append("表番号 = ?")
        params.append(hyo.zfill(2))
    if nendo is not None:
        where.append("? BETWEEN 決算年度_最初 AND 決算年度_最新")
        params.append(nendo)
    return _query(f"{PREFIX}/{kubun}/_tables.parquet", where, params, "表番号, 決算年度_最初", None, format, f"chizai/{kubun}/tables")


@router.get("/meta", summary="表の行・列の一覧")
async def get_meta(
    kubun: str = Query("pref", description="pref=都道府県分"),
    hyo: str = Query(..., description="表番号。例: 02（決算収支の状況）"),
    nendo: int = Query(None, description="この決算年度（西暦）に収録がある行・列だけ返す。例: 2024"),
    format: str = Query("json", description="json（既定）または csv"),
):
    """
    指定した表の行番号・列番号とその名称、収録年度の一覧。

    `/chizai/data` の `gyo`・`retsu` に指定する番号はここで確認します。
    表の様式改正で、同じ番号が年度によって別の項目を指していることがあるため、
    1行は「番号 × 項目（名称）」の単位で、その名称としての収録年度を持ちます。
    名称中の年度（「令和6年度」「元年度契約額」など）の違いは同じ項目として扱います。
    特定の年度の様式だけ見たいときは `nendo` を指定してください。
    """
    err = _check(kubun, hyo)
    if err:
        return err
    where, params = ["表番号 = ?"], [hyo.zfill(2)]
    if nendo is not None:
        where.append("? BETWEEN 決算年度_最初 AND 決算年度_最新")
        params.append(nendo)
    return _query(f"{PREFIX}/{kubun}/_meta.parquet", where, params, "区分 DESC, 番号, 決算年度_最初", None, format, f"chizai/{kubun}/{hyo.zfill(2)}/meta")


@router.get("/data", summary="調査表データ（縦持ち）")
async def get_data(
    kubun: str = Query("pref", description="pref=都道府県分"),
    hyo: str = Query(..., description="表番号。例: 02（決算収支の状況）。一覧は /chizai/tables"),
    nendo_from: int = Query(None, description="決算年度（西暦）の開始。例: 2018"),
    nendo_to: int = Query(None, description="決算年度（西暦）の終了。例: 2024"),
    dantai: str = Query(None, description="団体コード6桁 または 市区町村コード5桁。カンマ区切りで複数可。例: 20000（長野県）、00000（全国）"),
    gyo: str = Query(None, description="行番号。カンマ区切りで複数可。例: 01"),
    retsu: str = Query(None, description="列番号。カンマ区切りで複数可。例: 001,005"),
    gyo_name: str = Query(None, description="行名称で絞込（完全一致。空白・中黒・名称中の年度の違いは無視）。| 区切りで複数可。例: 市中銀行"),
    retsu_name: str = Query(None, description="列名称で絞込（gyo_name と同じ規則）。例: 実質収支、令和6年度末現在高"),
    name_match: str = Query("exact", description="gyo_name・retsu_name の照合方法。exact=完全一致（既定）/ partial=部分一致（例: 財政融資資金 で「内訳・財政融資資金」も拾う）"),
    limit: int = Query(None, ge=1, description="取得件数の上限。省略時は全件"),
    format: str = Query("json", description="json（既定）または csv"),
):
    """
    総務省「地方財政状況調査」調査表の数値を縦持ち（1行1数値）で返す。

    ### 使用例
    - 長野県の決算収支（当年度分）の推移
      `?hyo=02&dantai=20000&gyo=01`
    - 全都道府県の実質収支（2018年度以降）をCSVで
      `?hyo=02&retsu=005&gyo=01&nendo_from=2018&format=csv`
    - 長野県の地方債現在高のうち市中銀行分の推移（行番号が年度で変わっても名称で追える）
      `?hyo=39&dantai=20000&gyo_name=市中銀行&retsu_name=差引現在高`
    - 「内訳・財政融資資金」と「財政融資資金」のように前後の付け方が違う名称もまとめて取る（部分一致）
      `?hyo=39&dantai=20000&gyo_name=財政融資資金&name_match=partial`

    ### 列
    決算年度 / 団体コード（6桁）/ 市区町村コード（5桁、マスタ `_M_city`・`_M_pref` と結合用）/
    都道府県名 / 団体名 / 団体区分 / 表番号 / 表名称 / 行番号 / 行名称 / 列番号 / 列名称 / 値

    ### 注意
    - 出典はe-Statのファイル提供CSV（1989年度〜）。e-StatのDB（〜2017年度）とはコード体系が異なります
    - 同じ表番号・行番号・列番号が、年度によって別の表・項目に使われていることがあります。
      長期間をつなぐときは番号ではなく `gyo_name`・`retsu_name`（名称）で絞り込み、
      `/chizai/tables?hyo=..` と `/chizai/meta?hyo=..` で収録年度を確認してください
    - 名称は「令和6年度末現在高」と「平成9年度末現在高」のように年度だけ違うものを同じとみなします。
      「内訳・財政融資資金」のような前後の付け方の違いは `name_match=partial` で拾えます。
      部分一致は「財政融資資金・うち旧資金運用部資金」のような下位項目も拾うので、行名称を確認してください。
      「簡保資金」→「郵政公社資金」のような改名は別名称です（/chizai/meta の名称一覧で確認）
    - 全国計は 団体コード `000000`（市区町村コード `00000`）
    - 金額の単位は原則千円（表により比率等を含む）
    - 表02（決算収支の状況）は様式上、行01=当年度・行02=前年度です。年度をつなぐときは `gyo=01` で絞ってください
    - `-` や空欄は null
    """
    err = _check(kubun, hyo)
    if err:
        return err
    if name_match not in ("exact", "partial"):
        return JSONResponse(status_code=400, content={"error": "name_match は exact または partial"})

    where, params = [], []
    if nendo_from is not None:
        where.append("決算年度 >= ?")
        params.append(nendo_from)
    if nendo_to is not None:
        where.append("決算年度 <= ?")
        params.append(nendo_to)

    codes = [c.strip() for c in (dantai or "").split(",") if c.strip()]
    if codes:
        c6 = [c for c in codes if len(c) == 6]
        c5 = [c for c in codes if len(c) != 6]
        cond = []
        if c6:
            cond.append(f"団体コード IN ({','.join('?' * len(c6))})")
            params.extend(c6)
        if c5:
            cond.append(f"市区町村コード IN ({','.join('?' * len(c5))})")
            params.extend([c.zfill(5) for c in c5])
        where.append("(" + " OR ".join(cond) + ")")

    for col, vals in [("行番号", _codes(gyo, 2)), ("列番号", _codes(retsu, 3))]:
        if vals:
            where.append(f"{col} IN ({','.join('?' * len(vals))})")
            params.extend(vals)

    h = hyo.zfill(2)
    names = [("行名称", _name_list(gyo_name)), ("列名称", _name_list(retsu_name))]
    return _query(f"{PREFIX}/{kubun}/{h}/data.parquet", where, params, "決算年度, 団体コード, 行番号, 列番号", limit, format, f"chizai/{kubun}/{h}", names, partial=(name_match == "partial"))
