# 在留外国人統計（出入国在留管理庁）テーブルデータ「国籍・地域別 在留資格別 市区町村別」（t2）の収集
# e-StatのDBには 国籍×在留資格×市区町村 の表がないため、ファイル提供のExcelを取り込み、
# 全時点を縦持ちの1つのParquetにしてGCSへ保存する。
# 実行は GitHub Actions（scripts/collect_zairyu.py）から。
#
# 元ファイルは時点により形式が3通りある:
#   2023-12        通常のシートに明細（国籍・在留資格は名称のみ）
#   2024-06        通常のシートに明細（「01_023：中国」「35：永住者」のようにコード付き）
#   2024-12〜      明細は Power Pivot のデータモデル（xl/model/item.data）の中だけ。年齢・性別が加わる
#
# GCS:
#   zairyu/t2/data.parquet       本体（全時点）
#   zairyu/t2/_periods.parquet   収録時点の一覧（/zairyu/periods 用）
#   zairyu/_state.json           取込済みファイルの更新日（差分更新用）

import calendar
import io
import json
import os
import re
import tempfile
import time
import zipfile

import httpx
import openpyxl
import pandas as pd
from google.cloud import storage

API_URL       = "https://api.e-stat.go.jp/rest/3.0/app/json/getDataCatalog"
STATS_CODE    = "00250012"   # 在留外国人統計（旧登録外国人統計）
BUCKET_NAME   = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
PREFIX        = "zairyu"
DATA_PATH     = f"{PREFIX}/t2/data.parquet"
PERIODS_PATH  = f"{PREFIX}/t2/_periods.parquet"
STATE_PATH    = f"{PREFIX}/_state.json"
SHIKAKU_PATH  = "master/_M_zairyu_shikaku/data.parquet"
KOKUSEKI_CSV  = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "zairyu_kokuseki.csv")
API_INTERVAL  = 30   # カタログAPIの呼出間隔（秒）。e-StatはIP単位で429になるため直列・間隔あり
FILE_INTERVAL = 3    # Excelダウンロードの間隔（秒）

# 州（国籍・地域コードの上2桁）
SHU = {"01": "アジア", "02": "ヨーロッパ", "03": "アフリカ", "04": "北米", "05": "南米", "06": "オセアニア", "07": "無国籍"}

# 政令指定都市（5桁コード → 市名）。区のコードは「同じ上3桁で、区のコード以下の最大の市コード」の市に属する
# 例: 14131（川崎区）→ 14130 川崎市 / 14118（泉区）→ 14100 横浜市。東京23区（131xx）は政令市ではないので含めない
SEIREI = {
    "01100": "札幌市", "04100": "仙台市", "11100": "さいたま市", "12100": "千葉市", "14100": "横浜市",
    "14130": "川崎市", "14150": "相模原市", "15100": "新潟市", "22100": "静岡市", "22130": "浜松市",
    "23100": "名古屋市", "26100": "京都市", "27100": "大阪市", "27140": "堺市", "28100": "神戸市",
    "33100": "岡山市", "34100": "広島市", "40100": "北九州市", "40130": "福岡市", "43100": "熊本市",
}

# 特殊な地域。元ファイルでは時点によりコードが違う（未定・不詳: 2024-06まで48000、2024-12から99999 /
# その他: 2024-06まで99999）ため、ここでそろえる
CODE_FUSHO = "99999"   # 未定・不詳
CODE_SONOTA = "99998"  # その他（総数10人以下の市区町村の合計。2023-12・2024-06のみ）
HITOKU = "秘匿"        # 元ファイルの「-」（秘匿処理）
ZEN2HAN = str.maketrans("０１２３４５６７８９", "0123456789")

OUT_COLS = ["調査年月", "基準日", "都道府県コード", "都道府県", "市区町村コード", "市区町村", "地域区分",
            "政令市コード", "政令市", "州コード", "州", "国籍・地域コード", "国籍・地域",
            "在留資格コード", "在留資格", "年齢5歳階級コード", "年齢5歳階級", "年齢", "性別コード", "性別", "人数"]
KEY_COLS = OUT_COLS[:-1]
PERIOD_COLS = ["調査年月", "基準日", "年齢性別", "表番号", "statInfId", "更新日", "行数", "総数", "市区町村数"]


# ── e-Stat カタログ ─────────────────────────────────────────

def _as_list(x):
    # e-StatのJSONは1件だとdict、複数だとlistで返るため揃える
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def _text(x) -> str:
    # 値が文字列 / {"$": ...} / {"NAME": ...} のいずれでも文字列にする
    if isinstance(x, dict):
        return str(x.get("NAME") or x.get("$") or json.dumps(x, ensure_ascii=False))
    return "" if x is None else str(x)


def fetch_catalog() -> list[dict]:
    # getDataCatalogでテーブルデータの一覧を取り、市区町村別（表番号 yy-mm-t2）のExcelだけを返す
    params = {"appId": os.environ["ESTAT_APP_ID"], "statsCode": STATS_CODE,
              "searchWord": "テーブルデータ", "limit": 100}
    items, start = {}, 1
    with httpx.Client(timeout=120) as client:
        while True:
            params["startPosition"] = start
            r = client.get(API_URL, params=params)
            print(f"[estat] getDataCatalog start={start} status={r.status_code} bytes={len(r.content)}", flush=True)
            r.raise_for_status()
            root   = r.json()["GET_DATA_CATALOG"]
            status = str(root.get("RESULT", {}).get("STATUS"))
            if status == "1":   # 該当データなし
                break
            if status != "0":
                raise RuntimeError(f"getDataCatalog エラー: {root.get('RESULT')}")

            inf = root.get("DATA_CATALOG_LIST_INF", {})
            for cat in _as_list(inf.get("DATA_CATALOG_INF")):
                for res in _as_list(cat.get("RESOURCES", {}).get("RESOURCE")):
                    name = _text(res.get("TITLE"))
                    url  = str(res.get("URL", ""))
                    m = re.search(r"(\d{2})-(\d{2})-t2_", name)
                    if not m or "市区町村" not in name:
                        continue
                    sid = re.search(r"statInfId=(\d+)", url)
                    period = f"20{m.group(1)}{m.group(2)}"
                    items[period] = {
                        "period": period, "name": name, "url": url,
                        "table": f"{m.group(1)}-{m.group(2)}-t2", "statInfId": sid.group(1) if sid else None,
                        "modified": str(res.get("LAST_MODIFIED_DATE", "")),
                    }

            nxt = inf.get("RESULT_INF", {}).get("NEXT_KEY")
            if not nxt:
                break
            start = int(nxt)
            time.sleep(API_INTERVAL)
    return [items[p] for p in sorted(items)]


# ── Excelの読込 ────────────────────────────────────────────

def _patch_pbixray():
    # pbixray 0.15.5 の不具合回避: テーブル定義（.tbl.xml）を探す正規表現が [^H$R$] のため、
    # テーブルIDが H か R で始まると読み飛ばされ、0行になる（この統計は「R712 テーブルデータ用BD_…」）。
    # 本来の意図（H$…・R$… の補助ファイルを除く）どおりの正規表現に差し替える
    from pbixray.meta import xml_source
    from pbixray.xldm.xmobject import XMObjectDocument

    def _extract_tbl_metadata(self):
        self._load_xml_collection(
            re.compile(r"^(?!H\$|R\$)([^$]+?)\.(\d+)\.tbl\.xml$"),
            lambda s: XMObjectDocument.from_xml_string(s).root_object,
            self._tbl_objects, "table",
        )
    xml_source.XmlMetadataSource._extract_tbl_metadata = _extract_tbl_metadata


def _read_model(content: bytes) -> pd.DataFrame:
    # Power Pivot のデータモデルから明細テーブルを取り出す
    _patch_pbixray()
    from pbixray import PBIXRay
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(content)
        path = tmp.name
    try:
        model = PBIXRay(path)
        schema = model.schema
        tables = [t for t in model.tables if "在留外国人数" in set(schema.loc[schema["TableName"] == t, "ColumnName"])]
        if len(tables) != 1:
            raise ValueError(f"明細テーブルを特定できません: {list(model.tables)}")
        df = model.get_table(tables[0])
        model.close()
    finally:
        os.remove(path)
    return df.drop(columns=["__XL_RowNumber"], errors="ignore")


def _read_sheet(content: bytes) -> pd.DataFrame:
    # 通常のシートから明細を取り出す（注意事項・PVT以外で、在留外国人数の列があるシート）
    book = pd.ExcelFile(io.BytesIO(content))
    for sheet in book.sheet_names:
        if sheet in ("注意事項", "PVT"):
            continue
        df = book.parse(sheet, dtype=str)
        if {"市区町村コード", "国籍・地域", "在留資格", "在留外国人数"} <= set(df.columns):
            return df
    raise ValueError(f"明細シートが見つかりません: {book.sheet_names}")


def read_raw(content: bytes) -> pd.DataFrame:
    # Excelの明細を元の列名のまま返す
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        has_model = "xl/model/item.data" in z.namelist()
    return _read_model(content) if has_model else _read_sheet(content)


def pivot_total(content: bytes):
    # PVTシートの「総計」（検算用）。見つからなければ None
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
    try:
        if "PVT" not in wb.sheetnames:
            return None
        for row in wb["PVT"].iter_rows(values_only=True):
            if row and "総計" in row[:5]:
                nums = [v for v in row if isinstance(v, (int, float))]
                return int(nums[-1]) if nums else None
    finally:
        wb.close()
    return None


# ── 整形 ───────────────────────────────────────────────────

def _strip_code(s: pd.Series) -> pd.Series:
    # 「01：北海道」「35：永住者」「1:男」→ 名称だけ
    return s.str.replace(r"^\d+[：:]", "", regex=True)


def _seirei_of(code: str):
    # 政令市の区なら所属する市のコードを返す（区でなければ None）
    if code in SEIREI:
        return None
    cands = [c for c in SEIREI if c[:3] == code[:3] and c <= code]
    return max(cands) if cands else None


def load_shikaku_map() -> dict:
    # 在留資格名 → 4桁コード（_M_zairyu_shikaku）
    blob = storage.Client().bucket(BUCKET_NAME).blob(SHIKAKU_PATH)
    m = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
    return dict(zip(m["zairyu_shikaku_name"], m["code"]))


def load_kokuseki_map() -> dict:
    # 名称だけの時点（2023-12）用: 国籍・地域名（別名を含む）→（州コード, 国籍・地域コード, 正式名）
    k = pd.read_csv(KOKUSEKI_CSV, dtype=str, encoding="utf-8").fillna("")
    out = {}
    for r in k.itertuples(index=False):
        out[r[2]] = (r[0], r[1], r[2])
        if r[3]:
            out[r[3]] = (r[0], r[1], r[2])
    return out


def normalize(raw: pd.DataFrame, period: str, shikaku_map: dict, kokuseki_map: dict) -> pd.DataFrame:
    # 元の明細（時点ごとに列・表記が違う）を標準スキーマにそろえる
    df  = raw.copy()
    for c in df.columns:
        if c != "在留外国人数":
            df[c] = df[c].astype("string").str.strip()
    out = pd.DataFrame(index=df.index)
    y, m = int(period[:4]), int(period[4:])
    out["調査年月"] = period
    out["基準日"]   = f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}"

    # 地域。特殊な地域は名称で判定する（コードは時点で違う）
    city  = df["市区町村"]
    code  = df["市区町村コード"].str.zfill(5)
    fusho = city.eq("未定・不詳")
    other = city.eq("その他")
    out["市区町村コード"] = code.mask(fusho, CODE_FUSHO).mask(other, CODE_SONOTA)
    out["市区町村"]       = city
    out["都道府県コード"] = code.str[:2].mask(fusho, "48").mask(other, "99")
    out["都道府県"]       = _strip_code(df["都道府県"]).mask(fusho, "未定・不詳").mask(other, "その他")
    seirei = out["市区町村コード"].map(_seirei_of)
    out["政令市コード"] = seirei
    out["政令市"]       = seirei.map(SEIREI)
    kubun = pd.Series("市町村", index=df.index)
    kubun = kubun.mask(out["市区町村コード"].str.match(r"^131\d\d$"), "特別区")
    kubun = kubun.mask(seirei.notna(), "政令市の区")
    kubun = kubun.mask(fusho, "未定・不詳").mask(other, "その他（総数10人以下の市区町村の合計）")
    out["地域区分"] = kubun

    # 国籍・地域。「01_023：中国」→ 州コード01・国籍コード023。名称だけの時点は対応表で補う
    kk = df["国籍・地域"]
    hit = kk.str.extract(r"^(\d{2})_(\d{3})[：:](.+)$")
    plain = hit[0].isna() & kk.ne("-")
    if plain.any():
        unknown = sorted(set(kk[plain]) - set(kokuseki_map))
        if unknown:
            raise ValueError(f"{period}: 国籍・地域の対応表（app/data/zairyu_kokuseki.csv）にない名称: {unknown}")
        mapped = kk[plain].map(kokuseki_map)
        hit.loc[plain, 0] = mapped.str[0]
        hit.loc[plain, 1] = mapped.str[1]
        hit.loc[plain, 2] = mapped.str[2]
    out["州コード"]         = hit[0]
    out["州"]               = hit[0].map(SHU)
    out["国籍・地域コード"] = hit[1]
    out["国籍・地域"]       = hit[2].where(kk.ne("-"), HITOKU)

    # 在留資格。番号を外した名称で _M_zairyu_shikaku の4桁コードを付ける
    sk = _strip_code(df["在留資格"])
    unknown = sorted(set(sk[sk.ne("-")]) - set(shikaku_map))
    if unknown:
        raise ValueError(f"{period}: _M_zairyu_shikaku にない在留資格: {unknown}")
    out["在留資格コード"] = sk.map(shikaku_map)
    out["在留資格"]       = sk.where(sk.ne("-"), HITOKU)

    # 年齢・性別（2024-12から）。「06_25～29歳」「1:男」。「-」は秘匿
    if "性別" in df.columns:
        a5 = df["年齢（５歳階級）"].str.extract(r"^(\d{2})_(.+)$")
        out["年齢5歳階級コード"] = a5[0]
        # 「０～４歳」の全角数字を半角に（_M_age の age5_name「0～4歳」とそろえる。～ は全角のまま）
        out["年齢5歳階級"]       = a5[1].str.translate(ZEN2HAN).where(df["年齢（５歳階級）"].ne("-"), HITOKU)
        out["年齢"]              = df["年齢"].mask(df["年齢"].eq("-"), HITOKU)
        sx = df["性別"].str.extract(r"^(\d+)[：:](.+)$")
        out["性別コード"] = sx[0]
        out["性別"]       = sx[1].where(df["性別"].ne("-"), HITOKU)
    else:
        for c in ["年齢5歳階級コード", "年齢5歳階級", "年齢", "性別コード", "性別"]:
            out[c] = pd.NA

    v = pd.to_numeric(df["在留外国人数"], errors="coerce")
    if v.isna().any():
        raise ValueError(f"{period}: 在留外国人数が数値でない行 {int(v.isna().sum())}件")
    out["人数"] = v.astype("int64")

    out = out[OUT_COLS]
    out[KEY_COLS] = out[KEY_COLS].astype("string")
    # 同じ組み合わせの行は合計する。2023-12の「その他」は元の市区町村ごとの行のまま並んでいるため、ここで1行にまとめる
    dup = out.duplicated(KEY_COLS, keep=False)
    if dup.any():
        where = out.loc[dup, "市区町村"].value_counts().to_dict()
        print(f"ℹ️ {period}: 同じ組み合わせの行 {int(dup.sum())}件を合計 {where}", flush=True)
        out = out.groupby(KEY_COLS, dropna=False, observed=True, sort=False)["人数"].sum().reset_index()
    return out


def parse_file(content: bytes, period: str, shikaku_map: dict, kokuseki_map: dict) -> pd.DataFrame:
    # 1ファイルを読み込んで整形し、PVTシートの総計と検算する
    df = normalize(read_raw(content), period, shikaku_map, kokuseki_map)
    total, pvt = int(df["人数"].sum()), pivot_total(content)
    if pvt is not None and pvt != total:
        raise ValueError(f"{period}: 明細の合計 {total:,} がPVTシートの総計 {pvt:,} と一致しません")
    print(f"[{period}] {len(df):,}行 総数 {total:,}（PVT総計 {pvt if pvt is None else f'{pvt:,}'}）", flush=True)
    return df


def period_row(df: pd.DataFrame, item: dict) -> dict:
    return {
        "調査年月": item["period"], "基準日": df["基準日"].iloc[0], "年齢性別": bool(df["性別"].notna().any()),
        "表番号": item["table"], "statInfId": item["statInfId"], "更新日": item["modified"],
        "行数": len(df), "総数": int(df["人数"].sum()), "市区町村数": int(df["市区町村コード"].nunique()),
    }


# ── GCS ────────────────────────────────────────────────────

def _bucket():
    return storage.Client().bucket(BUCKET_NAME)


def _save_df(df: pd.DataFrame, path: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    _bucket().blob(path).upload_from_file(buf, content_type="application/octet-stream")
    print(f"✅ gs://{BUCKET_NAME}/{path} ({len(df):,}件, {buf.getbuffer().nbytes / 1e6:.1f}MB)", flush=True)


def _load_df(path: str):
    blob = _bucket().blob(path)
    if not blob.exists():
        return None
    return pd.read_parquet(io.BytesIO(blob.download_as_bytes()))


def _load_json(path: str) -> dict:
    blob = _bucket().blob(path)
    return json.loads(blob.download_as_text()) if blob.exists() else {}


def _save_json(obj: dict, path: str):
    _bucket().blob(path).upload_from_string(json.dumps(obj, ensure_ascii=False), content_type="application/json")


def _download(client: httpx.Client, url: str) -> bytes:
    r = client.get(url)
    r.raise_for_status()
    if not r.content.startswith(b"PK"):
        raise ValueError(f"Excel（zip）ではありません: {r.headers.get('content-type')}")
    return r.content


# ── 実行 ───────────────────────────────────────────────────

def run(mode: str = "dryrun") -> int:
    # mode: dryrun=一覧と最新1ファイルの整形結果を表示のみ / full=全時点を取り直す / update=更新されたファイルだけ取り込む
    if mode not in ("dryrun", "full", "update"):
        raise ValueError(f"mode は dryrun / full / update: {mode}")

    items = fetch_catalog()
    print(f"📋 市区町村別テーブルデータ {len(items)}件: {[it['period'] for it in items]}", flush=True)
    if not items:
        raise RuntimeError("カタログに t2（市区町村別）が見つかりません")
    shikaku_map, kokuseki_map = load_shikaku_map(), load_kokuseki_map()

    if mode == "dryrun":
        it = items[-1]
        print("サンプル:", it)
        with httpx.Client(timeout=300, follow_redirects=True) as client:
            df = parse_file(_download(client, it["url"]), it["period"], shikaku_map, kokuseki_map)
        print(df.head(10).to_string())
        print(df.groupby("地域区分")["人数"].agg(["size", "sum"]).to_string())
        return 0

    state = {} if mode == "full" else _load_json(STATE_PATH)
    targets = [it for it in items if state.get(it["url"]) != it["modified"]]
    print(f"🎯 取込対象 {len(targets)}件: {[it['period'] for it in targets]}", flush=True)
    if not targets:
        return 0

    parts, rows = [], []
    with httpx.Client(timeout=300, follow_redirects=True) as client:
        for i, it in enumerate(targets, 1):
            print(f"[{i}/{len(targets)}] {it['name']}", flush=True)
            df = parse_file(_download(client, it["url"]), it["period"], shikaku_map, kokuseki_map)
            parts.append(df)
            rows.append(period_row(df, it))
            time.sleep(FILE_INTERVAL)

    new = pd.concat(parts, ignore_index=True)
    periods = pd.DataFrame(rows, columns=PERIOD_COLS)
    if mode == "update":
        old = _load_df(DATA_PATH)
        if old is not None:
            new = pd.concat([old[~old["調査年月"].isin(periods["調査年月"])], new], ignore_index=True)
        old_p = _load_df(PERIODS_PATH)
        if old_p is not None:
            periods = pd.concat([old_p[~old_p["調査年月"].isin(periods["調査年月"])], periods], ignore_index=True)

    new[KEY_COLS] = new[KEY_COLS].astype("string")
    new = new.sort_values(["調査年月", "市区町村コード", "国籍・地域コード", "在留資格コード", "年齢5歳階級コード", "年齢", "性別コード"],
                          na_position="last").reset_index(drop=True)
    _save_df(new, DATA_PATH)
    _save_df(periods.sort_values("調査年月").reset_index(drop=True), PERIODS_PATH)
    for it in targets:
        state[it["url"]] = it["modified"]
    _save_json(state, STATE_PATH)
    print(f"🏁 完了: {len(targets)}時点を取込 / 全{new['調査年月'].nunique()}時点 {len(new):,}行", flush=True)
    return len(targets)
