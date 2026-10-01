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
ITEMS_CSV = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "chizai_items.csv")
_items_cache = None


def _items(kubun: str, hyo: str):
    # 項目対応表（app/data/chizai_items.csv）のうち、指定の表の分を返す。対応表がなければ None
    # 対応表は「表番号×行番号×列番号×年度範囲 → 項目コード・項目名」。
    # 年度で番号の意味が変わったり、名前が重複したりしても、項目名で同じ系列を取れるようにする
    global _items_cache
    if _items_cache is None:
        import pandas as pd
        d = pd.read_csv(ITEMS_CSV, dtype=str, encoding="utf-8").fillna("")
        d["開始"] = d["決算年度_開始"].astype(int)
        d["終了"] = d["決算年度_終了"].astype(int)
        _items_cache = d
    d = _items_cache[(_items_cache["区分"] == kubun) & (_items_cache["表番号"] == hyo)]
    return d if len(d) else None


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


def _query(path: str, where: list[str], params: list, order: str, limit, fmt: str, label: str, names: list = None, partial: bool = False, items=None):
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
        if items is not None:
            # 項目対応表を（行番号・列番号・年度範囲）で結合し、項目コード・項目名の列を足す
            # 軸=セル/行 → 項目（項目コード・項目名）、軸=列 → 指標（指標名）。番号 '*' はすべての行・列に当てはまる
            cols = ["行番号", "列番号", "開始", "終了", "項目コード", "項目名"]
            ren = {"行番号": "x_行", "列番号": "x_列"}
            con.register("xi", items[items["軸"].isin(["セル", "行"])][cols].rename(columns=ren))
            con.register("xc", items[items["軸"] == "列"][cols].rename(columns=ren))
            sql = (f"SELECT * FROM (SELECT d.*, x.項目コード, x.項目名, c.項目名 AS 指標名 FROM {src} d "
                   "LEFT JOIN xi x ON (x.x_行 = '*' OR d.行番号 = x.x_行) AND (x.x_列 = '*' OR d.列番号 = x.x_列) "
                   "AND d.決算年度 BETWEEN x.開始 AND x.終了 "
                   "LEFT JOIN xc c ON (c.x_行 = '*' OR d.行番号 = c.x_行) AND d.列番号 = c.x_列 "
                   "AND d.決算年度 BETWEEN c.開始 AND c.終了)")
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
    hyo: str = Query(None, description="表番号。例: 02（決算収支の状況）。省略すると表の一覧（/chizai/tables と同じ）を返す"),
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
    `hyo` を省略すると表の一覧を返します（`/chizai/tables` と同じ）。
    """
    if hyo is None:
        return await get_tables(kubun=kubun, hyo=None, nendo=nendo, format=format)
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
    item: str = Query(None, description="統一項目名または項目コードで絞込（項目対応表のある表のみ）。| 区切りで複数可。年度で番号や名称が変わっても同じ系列を返す。一覧は /chizai/items。例: 地方税、国庫支出金/普通建設事業費支出金"),
    shihyo: str = Query(None, description="指標名で絞込（行と列の2次元の表で、列の指標を統一したもの。例: 表15の 決算額）。| 区切りで複数可"),
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

    - 歳入内訳の地方税の推移（年度で列番号が変わっても1本の系列で取れる。項目対応表のある表のみ）
      `?hyo=04&dantai=20000&item=地方税`

    ### 列
    決算年度 / 団体コード（6桁）/ 市区町村コード（5桁、マスタ `_M_city`・`_M_pref` と結合用）/
    都道府県名 / 団体名 / 団体区分 / 表番号 / 表名称 / 行番号 / 行名称 / 列番号 / 列名称 / 値
    （項目対応表のある表は、末尾に 項目コード / 項目名 / 指標名 が付く）

    ### 項目対応表
    年度による番号の入れ替わり・改名・同名項目（「その他」など）を整理した対応表です。現在の対象: 表04・07〜13・15・37・39。
    `item` で指定すると、年度ごとに該当する行・列を自動で選びます。一覧と備考は `/chizai/items`

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
    - 表04・46・47・16（2020年度〜）は様式を複数の行に折り返しており、行02以降の列は行01と別の項目です。
      CSVに見出しがないため列名称は null で、表04は 項目名 で内容が分かります
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
    items = _items(kubun, h)
    if item:
        if items is None:
            return JSONResponse(status_code=400, content={"error": f"表{h}には項目対応表がまだありません。gyo_name・retsu_name を使ってください"})
        targets = [t.strip() for t in item.split("|") if t.strip()]
        it = items[items["軸"].isin(["セル", "行"])]
        known = set(it["項目名"]) | set(it["項目コード"])
        unknown = [t for t in targets if t not in known]
        if unknown:
            return JSONResponse(status_code=400, content={"error": f"項目がありません: {unknown}", "hint": f"/chizai/items?hyo={h} で確認してください"})
        ph = ",".join("?" * len(targets))
        where.append(f"(項目名 IN ({ph}) OR 項目コード IN ({ph}))")
        params.extend(targets + targets)
    if shihyo:
        ms = [t.strip() for t in shihyo.split("|") if t.strip()]
        known = set(items[items["軸"] == "列"]["項目名"]) if items is not None else set()
        if not known:
            return JSONResponse(status_code=400, content={"error": f"表{h}には指標の対応表がありません"})
        unknown = [t for t in ms if t not in known]
        if unknown:
            return JSONResponse(status_code=400, content={"error": f"指標がありません: {unknown}", "hint": f"/chizai/items?hyo={h} の 軸=列 を確認してください"})
        where.append(f"指標名 IN ({','.join('?' * len(ms))})")
        params.extend(ms)
    return _query(f"{PREFIX}/{kubun}/{h}/data.parquet", where, params, "決算年度, 団体コード, 行番号, 列番号", limit, format, f"chizai/{kubun}/{h}", names, partial=(name_match == "partial"), items=items)


@router.get("/items", summary="項目対応表")
async def get_items(
    kubun: str = Query("pref", description="pref=都道府県分"),
    hyo: str = Query(..., description="表番号。現在の対象: 04・07〜13・15・37・39"),
    detail: bool = Query(False, description="true で年度範囲ごとの行番号・列番号・元の名称まで返す"),
    format: str = Query("json", description="json（既定）または csv"),
):
    """
    `/chizai/data` の `item` に指定できる統一項目の一覧。

    - `軸` は セル（行×列で1項目）／行（行が項目）／列（列が指標）。2次元の表では 項目×指標 で値が決まる
    - `項目名` は「親/子」の形（例: 国庫支出金/普通建設事業費支出金）。軸=列 の名前は `/chizai/data` の `shihyo` に指定する
    - `備考` に改名・統合・区分変更などの注意を記載
    - `detail=true` で、各項目が年度ごとにどの行番号・列番号・元の名称だったかを返す
    """
    err = _check(kubun, hyo)
    if err:
        return err
    h = hyo.zfill(2)
    d = _items(kubun, h)
    if d is None:
        return JSONResponse(status_code=404, content={"error": f"表{h}の項目対応表はまだありません"})
    if detail:
        out = d.drop(columns=["開始", "終了"]).sort_values(["項目名", "決算年度_開始", "行番号"])
    else:
        latest = d.sort_values("終了").groupby(["軸", "項目コード"]).tail(1).set_index(["軸", "項目コード"])
        g = d.groupby(["軸", "項目コード", "項目名"]).agg(決算年度_最初=("開始", "min"), 決算年度_最新=("終了", "max"),
                                                  備考=("備考", lambda x: next((v for v in x if v), ""))).reset_index()
        # 並びは様式の順（各項目が最後に載った年度の行番号・列番号）。廃止された項目もその位置に並ぶ
        keys = list(zip(g["軸"], g["項目コード"]))
        g["行"] = [latest.loc[k, "行番号"] for k in keys]
        g["列"] = [latest.loc[k, "列番号"] for k in keys]
        g["軸順"] = g["軸"].map({"セル": 0, "行": 0, "列": 1})
        out = g.sort_values(["軸順", "行", "列", "決算年度_最新"], ascending=[True, True, True, False]).drop(columns=["行", "列", "軸順"])
    if format == "csv":
        return Response(content=out.to_csv(index=False).encode("utf-8-sig"), media_type="text/csv; charset=utf-8")
    data = out.to_dict(orient="records")
    return {"collection": f"chizai/{kubun}/{h}/items", "updated_at": str(datetime.now()), "count": len(data), "data": data}
