# 在留外国人統計（出入国在留管理庁）テーブルデータ「国籍・地域別 在留資格別 市区町村別」を返すルータ
# e-StatのファイルをまとめたParquet（GCS）から、指定した軸（by）で集計して返す
# データは scripts/collect_zairyu.py（GitHub Actions）で作成・更新する
import os
import re
import tempfile
import threading
import unicodedata
from datetime import datetime

import duckdb
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response
from google.cloud import storage

router = APIRouter(prefix="/zairyu", tags=["在留外国人統計"])

BUCKET_NAME  = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
DATA_PATH    = "zairyu/t2/data.parquet"
PERIODS_PATH = "zairyu/t2/_periods.parquet"
SHIKAKU_PATH = "master/_M_zairyu_shikaku/data.parquet"

# by に指定できる軸 → 出力する列（コード列, 名称列）と並べ順
DIMS = {
    "kokuseki": (["州コード", "州", "国籍・地域コード", "国籍・地域"], ["州コード", "国籍・地域コード"]),
    "shikaku":  (["在留資格コード", "在留資格"], ["在留資格コード"]),
    "nenrei5":  (["年齢5歳階級コード", "年齢5歳階級"], ["年齢5歳階級コード"]),
    "nenrei":   (["年齢"], ["TRY_CAST(regexp_extract(年齢, '\\d+') AS INTEGER)"]),
    "seibetsu": (["性別コード", "性別"], ["性別コード"]),
}
AGE_SEX = {"nenrei5", "nenrei", "seibetsu"}
MAX_ROWS = 1_000_000   # Excelの行数上限（1,048,576）に合わせた返却行数の上限

# GCSのParquetは更新時だけ取り直す（世代番号が同じなら前回の一時ファイルを使う）
_cache: dict = {}
_lock = threading.Lock()


def _local(path: str):
    # GCSのファイルを一時ファイルにして、そのパスを返す。無ければ None
    blob = storage.Client().bucket(BUCKET_NAME).get_blob(path)
    if blob is None:
        return None
    with _lock:
        hit = _cache.get(path)
        if hit and hit[0] == blob.generation and os.path.exists(hit[1]):
            return hit[1]
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        blob.download_to_filename(tmp_path)
        if hit and os.path.exists(hit[1]):
            os.remove(hit[1])
        _cache[path] = (blob.generation, tmp_path)
        return tmp_path


def _norm(s) -> str:
    # 名称の照合用: 全角半角・空白・括弧の違いを無視する（技能実習1号イ＝技能実習１号イ、朝鮮＝（朝鮮））
    s = unicodedata.normalize("NFKC", "" if s is None else str(s))
    return re.sub(r"[\s()（）]", "", s)


def _tokens(text, sep: str) -> list[str]:
    return [t.strip() for t in str(text or "").split(sep) if t.strip()]


def _respond(df: pd.DataFrame, fmt: str, label: str, hint: str = None):
    if fmt == "csv":
        return Response(content=df.to_csv(index=False).encode("utf-8-sig"), media_type="text/csv; charset=utf-8")
    body = {"collection": label, "updated_at": str(datetime.now()), "count": len(df), "data": df.to_dict(orient="records")}
    if hint and not len(df):
        body["hint"] = hint
    return body


def _no_data():
    return JSONResponse(status_code=503, content={"error": "データがまだありません（収集ワークフロー collect-zairyu を実行してください）"})


@router.get("/data", summary="在留外国人数（市区町村別 × 国籍・地域 × 在留資格 × 年齢・性別）")
async def get_data(
    from_: str = Query(None, alias="from", description="調査年月の開始 YYYYMM。例: 202412"),
    to: str = Query(None, description="調査年月の終了 YYYYMM。例: 202512"),
    pref: str = Query(None, description="都道府県コード2桁。カンマ区切りで複数可。例: 20（長野県）"),
    city: str = Query(None, description="市区町村コード5桁。カンマ区切りで複数可。seirei=city のときは政令市のコード（例: 14100 横浜市）。例: 20202（松本市）"),
    kokuseki: str = Query(None, description="国籍・地域の名称 または 3桁コード。州名（アジア等）も可。| 区切りで複数可。例: ベトナム|フィリピン、037"),
    shikaku: str = Query(None, description="在留資格の名称 または 4桁コード（_M_zairyu_shikaku と同じ）。| 区切りで複数可。例: 永住者、1430、特定技能1号"),
    by: str = Query("kokuseki,shikaku", description="結果に残す軸。kokuseki / shikaku / nenrei5 / nenrei / seibetsu をカンマ区切り。外した軸は合計する。空 または total で市区町村の合計だけ"),
    seirei: str = Query("ward", description="ward=政令市は区ごと（既定）/ city=政令市の区を市にまとめる（東京23区はそのまま）"),
    limit: int = Query(None, ge=1, description="取得件数の上限。省略時は全件"),
    format: str = Query("json", description="json（既定）または csv"),
):
    """
    出入国在留管理庁「在留外国人統計」テーブルデータ（国籍・地域別 在留資格別 市区町村別）の在留外国人数。
    6月末・12月末の年2回（2023年12月末〜）。指定した軸（`by`）で集計して返す。

    ### 使用例
    - 長野県の市町村別 総数の推移: `?pref=20&by=&format=csv`
    - 長野県の市町村別・国籍別: `?pref=20&by=kokuseki`
    - 松本市のベトナム人を在留資格別に: `?city=20202&kokuseki=ベトナム&by=shikaku`
    - 長野市の年齢5歳階級×性別（2024年12月末〜）: `?city=20201&by=nenrei5,seibetsu`
    - 政令市単位の特定技能1号: `?shikaku=特定技能1号&by=&seirei=city`

    ### 列
    調査年月 / 基準日 / 都道府県コード / 都道府県 / 市区町村コード / 市区町村 / 地域区分
    ＋ by=kokuseki → 州コード / 州 / 国籍・地域コード / 国籍・地域
    ＋ by=shikaku → 在留資格コード（4桁）/ 在留資格
    ＋ by=nenrei5 → 年齢5歳階級コード / 年齢5歳階級　＋ by=nenrei → 年齢　＋ by=seibetsu → 性別コード / 性別
    ＋ 人数

    ### 注意
    - 年齢・性別は2024年12月末から。by に nenrei5・nenrei・seibetsu を含めると、それより前の時点は返さない
    - 外国人5,000人未満の市町村は年齢・性別が「秘匿」。総数10人以下の市町村は国籍・地域と在留資格も「秘匿」（コードは null）
    - 地域区分: 市町村 / 政令市の区 / 特別区 / 政令市（seirei=city のとき）/ 未定・不詳（コード 99999）/
      その他（総数10人以下の市区町村の合計。2023年12月末・2024年6月末のみ、コード 99998）
    - 2024年6月末までは総数10人以下の市区町村が「その他」にまとめられているため、市区町村の数・都道府県の合計が後の時点と少し違う
    - 国籍・地域コードは出入国在留管理庁の分類（州コード2桁＋3桁）。在留資格コードは _M_zairyu_shikaku と同じ4桁
    - 名称は全角半角・括弧の違いを無視して照合する（特定技能1号＝特定技能１号、朝鮮＝（朝鮮））
    - 結果が100万行を超えるとエラー（Excelの行数上限）。期間・地域・軸で絞り込む
    """
    if format not in ("json", "csv"):
        return JSONResponse(status_code=400, content={"error": "format は json または csv"})
    if seirei not in ("ward", "city"):
        return JSONResponse(status_code=400, content={"error": "seirei は ward または city"})
    for name, v in [("from", from_), ("to", to)]:
        if v is not None and not re.fullmatch(r"\d{6}", v):
            return JSONResponse(status_code=400, content={"error": f"{name} は YYYYMM（例: 202512）"})
    dims = [] if (by or "").strip().lower() in ("", "total") else _tokens(by, ",")
    bad = [d for d in dims if d not in DIMS]
    if bad:
        return JSONResponse(status_code=400, content={"error": f"by に指定できない軸: {bad}", "hint": f"使える軸: {list(DIMS)}"})
    dims = list(dict.fromkeys(dims))

    tmp_path = None
    try:
        tmp_path = _local(DATA_PATH)
        if tmp_path is None:
            return _no_data()
        src = f"read_parquet('{tmp_path}')"
        con = duckdb.connect()
        where, params = [], []
        if from_:
            where.append("調査年月 >= ?")
            params.append(from_)
        if to:
            where.append("調査年月 <= ?")
            params.append(to)
        prefs = [p.zfill(2) for p in _tokens(pref, ",")]
        if prefs:
            where.append(f"都道府県コード IN ({','.join('?' * len(prefs))})")
            params.extend(prefs)
        if AGE_SEX & set(dims):
            where.append("性別 IS NOT NULL")   # 年齢・性別のない時点（2024年6月末まで）を除く

        # 国籍・地域: 3桁コード / 名称 / 州名。名称は種類が少ないので、重複を除いた値だけPythonで照合する
        ks = _tokens(kokuseki, "|")
        if ks:
            codes = [t for t in ks if re.fullmatch(r"\d{3}", t)]
            names = {_norm(t) for t in ks if t not in codes}
            raw = con.execute(f"SELECT DISTINCT 国籍・地域, 州, 国籍・地域コード FROM {src}").fetchall()
            hit_k = sorted({k for k, s, c in raw if _norm(k) in names})
            hit_s = sorted({s for k, s, c in raw if s and _norm(s) in names})
            matched = {_norm(x) for x in hit_k + hit_s}
            known = {c for k, s, c in raw}
            miss = [t for t in ks if (t in codes and t not in known) or (t not in codes and _norm(t) not in matched)]
            if miss:
                return JSONResponse(status_code=400, content={"error": f"国籍・地域が見つかりません: {miss}", "hint": "一覧は /zairyu/meta?type=kokuseki"})
            cond = []
            if codes:
                cond.append(f"国籍・地域コード IN ({','.join('?' * len(codes))})")
                params.extend(codes)
            if hit_k:
                cond.append(f"国籍・地域 IN ({','.join('?' * len(hit_k))})")
                params.extend(hit_k)
            if hit_s:
                cond.append(f"州 IN ({','.join('?' * len(hit_s))})")
                params.extend(hit_s)
            where.append("(" + " OR ".join(cond) + ")")

        # 在留資格: 4桁コード / 名称
        ss = _tokens(shikaku, "|")
        if ss:
            codes = [t for t in ss if re.fullmatch(r"\d{4}", t)]
            names = {_norm(t) for t in ss if t not in codes}
            raw = con.execute(f"SELECT DISTINCT 在留資格, 在留資格コード FROM {src}").fetchall()
            hit = sorted({s for s, c in raw if _norm(s) in names})
            known = {c for s, c in raw}
            miss = [t for t in ss if (t in codes and t not in known) or (t not in codes and _norm(t) not in {_norm(h) for h in hit})]
            if miss:
                return JSONResponse(status_code=400, content={"error": f"在留資格が見つかりません: {miss}", "hint": "一覧は /zairyu/meta?type=shikaku"})
            cond = []
            if codes:
                cond.append(f"在留資格コード IN ({','.join('?' * len(codes))})")
                params.extend(codes)
            if hit:
                cond.append(f"在留資格 IN ({','.join('?' * len(hit))})")
                params.extend(hit)
            where.append("(" + " OR ".join(cond) + ")")

        # 地域の列。seirei=city なら政令市の区を市に置き換える
        if seirei == "city":
            area = ["COALESCE(政令市コード, 市区町村コード) AS 市区町村コード", "COALESCE(政令市, 市区町村) AS 市区町村",
                    "CASE WHEN 政令市コード IS NOT NULL THEN '政令市' ELSE 地域区分 END AS 地域区分"]
        else:
            area = ["市区町村コード", "市区町村", "地域区分"]
        dim_cols = [c for d in dims for c in DIMS[d][0]]
        keys = ["調査年月", "基準日", "都道府県コード", "都道府県", "市区町村コード", "市区町村", "地域区分"] + dim_cols
        inner = f"SELECT 調査年月, 基準日, 都道府県コード, 都道府県, {', '.join(area)}, {', '.join(dim_cols + ['人数'])} FROM {src}"
        if where:
            inner += " WHERE " + " AND ".join(where)

        sql = f"SELECT {', '.join(keys)}, CAST(SUM(人数) AS BIGINT) AS 人数 FROM ({inner})"
        cities = [c.zfill(5) for c in _tokens(city, ",")]
        if cities:
            sql += f" WHERE 市区町村コード IN ({','.join('?' * len(cities))})"
            params.extend(cities)
        order = ["調査年月", "市区町村コード"] + [o for d in dims for o in DIMS[d][1]]
        sql += f" GROUP BY {', '.join(keys)} ORDER BY {', '.join(o + ' NULLS LAST' for o in order)}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        df = con.execute(sql, params).df()
        con.close()
        if len(df) > MAX_ROWS:
            return JSONResponse(status_code=400, content={
                "error": f"結果が {len(df):,} 行あり、上限 {MAX_ROWS:,} 行を超えています",
                "hint": "from・to・pref・city で絞り込むか、by の軸を減らしてください（例: by=kokuseki）。先頭だけ見るなら limit"})
        hint = "該当なし。年齢・性別（nenrei5・nenrei・seibetsu）は2024年12月末からです" if AGE_SEX & set(dims) else "該当なし。条件を確認してください"
        return _respond(df, format, "zairyu/data", hint)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"{type(e).__name__}: {e}"})


@router.get("/periods", summary="収録している時点の一覧")
async def get_periods(format: str = Query("json", description="json（既定）または csv")):
    """
    収録している調査時点（6月末・12月末）の一覧。
    列: 調査年月 / 基準日 / 年齢性別（年齢・性別の有無）/ 表番号 / statInfId（e-Statのファイル番号）/ 更新日 / 行数 / 総数 / 市区町村数
    """
    try:
        tmp_path = _local(PERIODS_PATH)
        if tmp_path is None:
            return _no_data()
        df = duckdb.query(f"SELECT * FROM read_parquet('{tmp_path}') ORDER BY 調査年月").df()
        return _respond(df, format, "zairyu/periods")
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"{type(e).__name__}: {e}"})


@router.get("/meta", summary="国籍・地域、在留資格、年齢区分、性別の一覧")
async def get_meta(
    type: str = Query("kokuseki", description="kokuseki（既定）/ shikaku / nenrei5 / seibetsu"),
    format: str = Query("json", description="json（既定）または csv"),
):
    """
    `/zairyu/data` の `kokuseki`・`shikaku` に指定できる名称・コードと、収録時点（最初・最新）。
    `type=shikaku` は _M_zairyu_shikaku の 集計用グループ（name_base）と大分類も付く。秘匿は含めない。
    """
    cols = {
        "kokuseki": ["州コード", "州", "国籍・地域コード", "国籍・地域"],
        "shikaku":  ["在留資格コード", "在留資格"],
        "nenrei5":  ["年齢5歳階級コード", "年齢5歳階級"],
        "seibetsu": ["性別コード", "性別"],
    }
    if type not in cols:
        return JSONResponse(status_code=400, content={"error": f"type は {list(cols)} のいずれか"})
    try:
        tmp_path = _local(DATA_PATH)
        if tmp_path is None:
            return _no_data()
        c = cols[type]
        sql = (f"SELECT {', '.join(c)}, MIN(調査年月) AS 調査年月_最初, MAX(調査年月) AS 調査年月_最新 "
               f"FROM read_parquet('{tmp_path}') WHERE {c[0]} IS NOT NULL GROUP BY {', '.join(c)} "
               f"ORDER BY {', '.join(x for x in c if x.endswith('コード'))}")
        con = duckdb.connect()
        df = con.execute(sql).df()
        if type == "shikaku":
            m_path = _local(SHIKAKU_PATH)
            if m_path is not None:
                m = con.execute(f"SELECT code AS 在留資格コード, name_base AS 集計用グループ, category_major AS 大分類 FROM read_parquet('{m_path}')").df()
                df = df.merge(m, on="在留資格コード", how="left")
        con.close()
        return _respond(df, format, f"zairyu/meta/{type}")
    except Exception as e:   # 引数 type が組込みの type を隠すため __class__ を使う
        return JSONResponse(status_code=500, content={"error": f"{e.__class__.__name__}: {e}"})
