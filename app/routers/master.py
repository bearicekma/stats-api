# マスタデータエンドポイント
# /master/_M_pref     : 都道府県マスタ
# /master/_M_city     : 市区町村マスタ
# /master/_M_calendar : カレンダーマスタ（祝日・平日判定）
# /master/_M_country  : 国名マスタ（財務省貿易統計 統計国名符号表ベース）
# /master/_M_zairyu_shikaku : 在留資格マスタ（e-Stat 在留外国人統計 cat01ベース）
# /master/_M_sangyo   : 産業分類マスタ（日本標準産業分類 令和5年改定、大・中・小分類）
# /master/_M_shokugyo    : 職業分類マスタ（日本標準職業分類 平成21年告示、大・中・小分類）
# /master/_M_shokugyo_hw : 職業分類マスタ（厚生労働省編職業分類 令和4年改定、大・中・小分類）
# /master/_M_houjin   : 法人マスタ（国税庁 法人番号 全件データ、全国・閉鎖法人を含む、約580万件）
#                       大きいため、絞り込み必須・GCSの世代が変わったときだけ読み直す・CSVはストリーミング

from datetime import datetime

import duckdb
import tempfile
import os
import re
import threading
import unicodedata

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from google.cloud      import storage

router = APIRouter(prefix="/master", tags=["マスタデータ"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")


def _download_parquet(collection_name: str) -> str:
    # GCSからParquetを一時ファイルにダウンロードしてパスを返す
    gcs      = storage.Client()
    bucket   = gcs.bucket(BUCKET_NAME)
    gcs_path = f"master/{collection_name}/data.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    bucket.blob(gcs_path).download_to_filename(tmp_path)
    return tmp_path


# ── _M_houjin（法人マスタ）──────────────────────────────────
# 全国で約580万行・約220MBあるため、他のマスタと違って
#   ・一時ファイルを残し、GCSの世代番号が変わったときだけ取り直す
#   ・pref / city / number のどれかでの絞り込みを必須にする
#   ・CSVは少しずつ書き出す（ストリーミング）。JSONは10万行まで
# ファイルは scripts/collect_houjin.py（GitHub Actions、毎月3日）が作成する
HOUJIN_PATH     = "master/_M_houjin/data.parquet"
HOUJIN_JSON_MAX = 100_000
HOUJIN_CHUNK    = 50_000
_houjin_cache: dict = {}
_houjin_lock = threading.Lock()


def _houjin_local() -> str | None:
    blob = storage.Client().bucket(BUCKET_NAME).get_blob(HOUJIN_PATH)
    if blob is None:
        return None
    with _houjin_lock:
        hit = _houjin_cache.get("file")
        if hit and hit[0] == blob.generation and os.path.exists(hit[1]):
            return hit[1]
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            path = tmp.name
        blob.download_to_filename(path)
        _houjin_cache["file"] = (blob.generation, path)
        # 古い世代のファイルは、読み込み中のリクエストがあり得るので少し後で消す
        if hit and os.path.exists(hit[1]):
            threading.Timer(600, lambda p=hit[1]: os.path.exists(p) and os.remove(p)).start()
        return path


def _tokens(text, sep: str = ",") -> list:
    return [t.strip() for t in str(text or "").split(sep) if t.strip()]


def _zenkaku(s: str) -> str:
    # 半角英数記号を全角にする（法人番号データの商号は英数字が全角のことが多い）
    return "".join(chr(ord(c) + 0xFEE0) if "!" <= c <= "~" else ("　" if c == " " else c) for c in s)


def _bad(msg: str, hint: str = None):
    body = {"error": msg}
    if hint:
        body["hint"] = hint
    return JSONResponse(status_code=400, content=body)


def _get_houjin(params: dict):
    fmt    = params.get("format", "json")
    status = params.get("status", "all")
    if fmt not in ("json", "csv"):
        return _bad("format は json または csv")
    if status not in ("all", "active", "closed"):
        return _bad("status は all / active / closed")
    prefs  = list(dict.fromkeys(p.zfill(2) for p in _tokens(params.get("pref"))))
    cities = [c.zfill(5) for c in _tokens(params.get("city"))]
    nums   = _tokens(params.get("number"))
    if not (prefs or cities or nums):
        return _bad("_M_houjin は全国で約580万件あるため、pref・city・number のどれかで絞り込んでください",
                    "例: /master/_M_houjin?pref=20&format=csv（長野県）")
    if any(not re.fullmatch(r"\d{2}", p) or not ("01" <= p <= "47" or p == "99") for p in prefs):
        return _bad("pref は都道府県コード2桁（01〜47）または 99（国外）")
    if any(not re.fullmatch(r"\d{5}", c) for c in cities):
        return _bad("city は団体コード5桁（例: 20202）")
    if any(not re.fullmatch(r"\d{13}", n) for n in nums):
        return _bad("number は法人番号13桁")
    kinds = _tokens(params.get("kind"))
    if any(not re.fullmatch(r"\d{3}", k) for k in kinds):
        return _bad("kind は法人種別コード3桁（例: 301）")
    limit = params.get("limit")
    if limit is not None and not str(limit).isdigit():
        return _bad("limit は正の整数")

    where, args = [], []
    for col, vals in (("都道府県コード", prefs), ("団体コード", cities), ("法人番号", nums), ("法人種別コード", kinds)):
        if vals:
            where.append(f"{col} IN ({','.join('?' * len(vals))})")
            args.extend(vals)
    if status != "all":
        where.append("状態 = ?")
        args.append("存続" if status == "active" else "閉鎖")
    name = (params.get("name") or "").strip()
    if name:
        nf   = unicodedata.normalize("NFKC", name)
        kata = "".join(chr(ord(c) + 0x60) if "ぁ" <= c <= "ゖ" else c for c in nf)   # ひらがな→カタカナ（フリガナ照合用）
        cond = []
        for pat in dict.fromkeys([name, nf, _zenkaku(nf), kata]):
            cond.append("(商号 LIKE ? OR フリガナ LIKE ?)")
            args.extend([f"%{pat}%", f"%{pat}%"])
        where.append("(" + " OR ".join(cond) + ")")

    try:
        path = _houjin_local()
        if path is None:
            return JSONResponse(status_code=503, content={"error": "_M_houjin がまだありません（GitHub Actions の collect-houjin を mode=full で実行してください）"})
        sql = f"SELECT * FROM read_parquet('{path}') WHERE " + " AND ".join(where)
        sql += " ORDER BY 都道府県コード NULLS LAST, 団体コード NULLS LAST, 法人番号"
        if limit:
            sql += f" LIMIT {int(limit)}"

        if fmt == "csv":
            con    = duckdb.connect()
            reader = con.execute(sql, args).fetch_record_batch(HOUJIN_CHUNK)

            def _stream():
                # 先頭だけBOM付きヘッダー。以降は HOUJIN_CHUNK 行ずつCSVにして送る（全件をメモリに載せない）
                try:
                    yield ("\ufeff" + ",".join(reader.schema.names) + "\n").encode("utf-8")
                    for batch in reader:
                        yield batch.to_pandas().to_csv(index=False, header=False).encode("utf-8")
                finally:
                    con.close()

            return StreamingResponse(_stream(), media_type="text/csv; charset=utf-8")

        con = duckdb.connect()
        n = con.execute(f"SELECT COUNT(*) FROM ({sql})", args).fetchone()[0]
        if n > HOUJIN_JSON_MAX:
            con.close()
            return _bad(f"結果が {n:,} 行あり、JSONの上限 {HOUJIN_JSON_MAX:,} 行を超えています",
                        "format=csv を使うか、city・status・kind・name で絞り込んでください。先頭だけ見るなら limit")
        df = con.execute(sql, args).df()
        con.close()
        data = df.to_dict(orient="records")
        return {"collection": "_M_houjin", "updated_at": str(datetime.now()), "count": len(data), "data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"{type(e).__name__}: {e}"})


@router.get("/{collection_name}", summary="マスタデータ取得")
async def get_master(collection_name: str, request: Request):
    """
    各種マスタデータを取得します。

    **利用可能な collection_name:**
    - `_M_pref` 都道府県マスタ（47都道府県）
    - `_M_city` 市区町村マスタ（全国市区町村）
    - `_M_calendar` カレンダーマスタ（祝日・平日判定、1950年〜）
    - `_M_country` 国名マスタ（財務省貿易統計 統計国名符号表ベース）
    - `_M_zairyu_shikaku` 在留資格マスタ（e-Stat 在留外国人統計 cat01ベース）
    - `_M_age` 年齢マスタ（各歳・5歳階級・10歳階級・3区分・4区分・労働力調査型・学齢区分）
    - `_M_sangyo` 産業分類マスタ（日本標準産業分類 第14回改定・令和5年、小分類536件）
    - `_M_shokugyo` 職業分類マスタ（日本標準職業分類 平成21年告示、小分類329件）
    - `_M_shokugyo_hw` 職業分類マスタ（厚生労働省編職業分類 令和4年改定、小分類440件）

    **_M_pref のレスポンスフィールド:**
    - `code` (string) 都道府県コード（2桁）
    - `code_5_digit` (string) 都道府県コード（5桁）
    - `code_6_digit` (string) 都道府県コード（6桁、検査数字付き）
    - `pref_name` (string) 都道府県名（例: 青森県）
    - `name_plain` (string) 都道府県名・接尾辞なし（例: 青森）
    - `region` (string) 地方区分（例: 東北、関東）

    **_M_city のレスポンスフィールド:**
    - `code_5_digit` (string) 市区町村コード（5桁）
    - `code_6_digit` (string) 市区町村コード（6桁、検査数字付き）
    - `name` (string) 市区町村名（同名の場合、他都道府県と重複あり）
    - `pref_code` (string) 都道府県コード（2桁）
    - `pref_name` (string) 都道府県名
    - `city_name` (string) 重複市区町村名に `.` を付加した一意名称

    **_M_calendar のレスポンスフィールド:**
    - `DATE` (string) 日付（YYYY-MM-DD）
    - `年` / `月` / `日` (int) 年月日
    - `年度` (int) 年度
    - `元号` (string) 元号付き年（例: 令和7年）
    - `曜日` (string) 曜日（月〜日）
    - `曜日コード` (int) 曜日コード（0=月 / 1=火 / 2=水 / 3=木 / 4=金 / 5=土 / 6=日）
    - `平日/休日` (string) 平日 or 休日
    - `祝日` (bool) 祝日フラグ
    - `祝日名` (string) 祝日名（祝日以外はnull）

    **_M_calendar のクエリパラメータ（_M_calendarのみ有効）:**
    - `year` (任意) 年で絞込。例: `2026`
    - `month` (任意) 月で絞込（1-12）。例: `5`
    - `from` (任意) 開始日（YYYY-MM-DD）。例: `2026-01-01`
    - `to` (任意) 終了日（YYYY-MM-DD）。例: `2026-12-31`
    - `holiday_only` (任意) `true` で祝日のみ取得
    - `weekday` (任意) 曜日コードで絞込（0=月〜6=日）。例: `0`

    **_M_country のレスポンスフィールド:**
    - `code` (string) 国名符号（3桁、財務省貿易統計ベース）
    - `country_name` (string) 国名（日本語）
    - `continent` (string) 所属エリア（大陸6区分）
    - `sub_region` (string) 地理圏の詳細区分（該当なしはnull）
    - `note` (string) 備考（該当なしはnull）

    **_M_zairyu_shikaku のレスポンスフィールド:**
    - `code` (string) 在留資格コード（4桁、e-Stat公式分類コード）
    - `zairyu_shikaku_name` (string) 在留資格正式名称
    - `name_base` (string) 集計用グルーピング名（技能実習／高度専門職／特定技能をまとめる）
    - `sub_type` (string) 号・区分（例: 1号イ）。区分なしはnull
    - `category_major` (string) 大分類（就労系／身分・地位系／非就労系／特定活動／特別永住者）
    - `sort_order` (int) 表示順（五十音順）
    - `betsuhyo_kubun` (string) 入管法上の区分（一の表／二の表／三の表／四の表／五の表／別表第二／特例法）
    - `landing_criteria` (bool) 上陸許可基準の適用有無
    - `katsudo_summary` (string) 活動内容を平易な言葉で要約した説明
    - `example` (string) 該当例（代表的な職業・立場）
    - `zairyu_kikan` (string) 在留期間の目安

    **_M_age のレスポンスフィールド:**
    - `age` (int) 年齢（0〜100、100は「100歳以上」）
    - `age_name` (string) 年齢表示（例: "0歳"、"100歳以上"）
    - `age5_code` / `age5_name` 5歳階級（例: "0～4歳"）
    - `age10_code` / `age10_name` 10歳階級（例: "0～9歳"）
    - `age3_code` / `age3_name` 年齢3区分（年少人口／生産年齢人口／老年人口）
    - `age4_code` / `age4_name` 年齢4区分（年少人口／生産年齢人口／前期高齢者／後期高齢者）
    - `age_labor_code` / `age_labor_name` 労働力調査型区分（15歳未満〜65歳以上の7区分）
    - `age_school_code` / `age_school_name` 学齢区分（未就学〜18歳以上の5区分）

    **_M_sangyo のレスポンスフィールド（小分類1件につき1行）:**
    - `code` (string) 小分類コード（3桁）
    - `name` (string) 小分類名
    - `chu_code` / `chu_name` 中分類コード（2桁）・中分類名
    - `dai_code` / `dai_name` 大分類コード（A〜T）・大分類名（総務省の正式表記）
    - `is_kanri` (bool) 「管理，補助的経済活動を行う事業所」（コード末尾0）か
    - 出典: ハローワークインターネットサービス 産業分類コード一覧

    **_M_shokugyo のレスポンスフィールド（小分類1件につき1行）:**
    - `code` (string) 小分類コード（3桁）
    - `name` (string) 小分類名
    - `chu_code` / `chu_name` 中分類コード（2桁）・中分類名
    - `dai_code` / `dai_name` 大分類コード（A〜L）・大分類名
    - 出典: 総務省 日本標準職業分類（平成21年12月告示）分類項目名

    **_M_shokugyo_hw のレスポンスフィールド（小分類1件につき1行）:**
    - `code` (string) 小分類コード（例: 067-01）
    - `name` (string) 小分類名
    - `chu_code` / `chu_name` 中分類コード（3桁）・中分類名
    - `dai_code` / `dai_name` 大分類コード（01〜15）・大分類名
    - `jsco_chu_code` / `jsco_chu_name` 対応する日本標準職業分類の中分類（対応表による。対応なしはnull）
    - `examples` / `not_examples` 例示職業名（〇該当する例／☓該当しない例）
    - 出典: ハローワークインターネットサービス 厚生労働省編職業分類（令和4年改定）

**_M_houjin のレスポンスフィールド（法人1件につき1行、全国・閉鎖法人を含む、約580万件）:**
    - `基準日` 全件データの作成時点（毎月末）/ `法人番号`（13桁、文字列）/ `商号` / `フリガナ` / `英語商号`
    - `法人種別コード` / `法人種別`（101 国の機関 / 201 地方公共団体 / 301 株式会社 / 302 有限会社 / 303 合名会社 /
      304 合資会社 / 305 合同会社 / 399 その他の設立登記法人 / 401 外国会社等 / 499 その他）
    - `都道府県コード` / `団体コード`（5桁、_M_city の code_5_digit と結合可。政令市は区のコード）/ `都道府県` / `市区町村` /
      `丁目番地等` / `郵便番号` / `国外所在地`
    - `状態`（存続・閉鎖）/ `閉鎖年月日` / `閉鎖事由コード` / `閉鎖事由`（01 清算の結了等 / 11 合併による解散等 /
      21 登記官による閉鎖 / 31 その他の清算の結了等）/ `承継先法人番号`
    - `法人番号指定年月日` / `変更年月日` / `更新年月日` / `最終処理区分コード` / `最終処理区分` / `変更事由の詳細` / `検索対象除外`
    - 法人番号は本店（主たる事務所）の所在地で登録されている。毎月3日に前月末時点の全件データで作り直す
    - 出典: 国税庁法人番号公表サイト「基本3情報ダウンロード」（全件データ）を加工して作成

    **_M_houjin のクエリパラメータ（_M_houjinのみ有効。pref・city・number のどれかは必須）:**
    - `pref` 都道府県コード2桁。カンマ区切り可。99=国外。例: `20`
    - `city` 団体コード5桁。カンマ区切り可。例: `20202`（松本市）
    - `number` 法人番号13桁。カンマ区切り可
    - `status` all（既定）/ active=存続のみ / closed=閉鎖のみ
    - `kind` 法人種別コード3桁。カンマ区切り可。例: `301,305`
    - `name` 商号・フリガナの部分一致（全角半角の違いを吸収、ひらがなはカタカナでも照合）
    - `limit` 取得件数の上限
    - `format` json（既定、10万行まで）/ csv（件数無制限、ストリーミング。東京都は100万行超）

        **URL例:**
    - `/master/_M_pref` 都道府県一覧
    - `/master/_M_city` 市区町村一覧
    - `/master/_M_calendar?year=2026` 2026年のカレンダー
    - `/master/_M_calendar?year=2026&holiday_only=true` 2026年の祝日一覧
    - `/master/_M_calendar?from=2026-04-01&to=2026-06-30` 期間指定
    - `/master/_M_calendar?weekday=0&year=2026` 2026年の月曜日一覧
    - `/master/_M_country` 国名マスタ一覧
    - `/master/_M_zairyu_shikaku` 在留資格マスタ一覧
    - `/master/_M_age` 年齢マスタ一覧
    - `/master/_M_sangyo` 産業分類マスタ一覧
    - `/master/_M_shokugyo` 職業分類マスタ（日本標準）一覧
    - `/master/_M_shokugyo_hw` 職業分類マスタ（厚労省編）一覧
    - `/master/_M_houjin?pref=20&format=csv` 長野県の法人（全件、CSV）
    - `/master/_M_houjin?city=20202&status=active&kind=301` 松本市の存続している株式会社
    """
    if collection_name == "_M_houjin":
        return _get_houjin(dict(request.query_params))

    tmp_path = None
    try:
        tmp_path = _download_parquet(collection_name)

        # _M_calendar のみ絞り込みパラメータを処理する
        conditions = []
        if collection_name == "_M_calendar":
            params       = dict(request.query_params)
            year         = params.get("year")
            month        = params.get("month")
            from_        = params.get("from")
            to_          = params.get("to")
            holiday_only = params.get("holiday_only", "").lower() == "true"
            weekday      = params.get("weekday")

            if year:
                conditions.append(f"年 = {int(year)}")
            if month:
                conditions.append(f"月 = {int(month)}")
            if from_:
                conditions.append(f"DATE >= '{from_}'")
            if to_:
                conditions.append(f"DATE <= '{to_}'")
            if holiday_only:
                conditions.append("祝日 = TRUE")
            if weekday is not None:
                conditions.append(f"曜日コード = {int(weekday)}")

        # ORDER BY句をcollectionごとに決定（存在しない列を指定するとBinder Errorになる.）
        order_map = {
            "_M_calendar": "DATE",
            "_M_city":     "code_5_digit",
            "_M_pref":     "code",
            "_M_country":  "code",
            "_M_zairyu_shikaku": "sort_order",
            "_M_age": "age",
            "_M_sangyo": "code",
            "_M_shokugyo": "code",
            "_M_shokugyo_hw": "code",
        }
        order_col = order_map.get(collection_name)
        order_by  = f"ORDER BY {order_col}" if order_col else ""

        where  = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql    = f"SELECT * FROM read_parquet('{tmp_path}') {where} {order_by}"
        result = duckdb.query(sql).df()
        data   = result.to_dict(orient="records")
        return {"collection": collection_name, "updated_at": str(datetime.now()), "count": len(data), "data": data}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
