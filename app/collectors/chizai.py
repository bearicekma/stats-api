# 地方財政状況調査（総務省）調査表CSVの収集
# e-StatのDBは2017年度で更新停止のため、ファイル提供のCSV（1989年度〜）を取り込み、
# 表番号ごとの縦持ちParquetにしてGCSへ保存する。
# 実行は GitHub Actions（scripts/collect_chizai.py）から。初回は約1,800ファイルあり
# Cloud Runの時間制限を超えるため、Cloud Run上では動かさない。
#
# GCS:
#   chizai/{kubun}/{表番号}/data.parquet  本体（全年度）
#   chizai/{kubun}/_tables.parquet         表の一覧（/chizai/tables 用）
#   chizai/{kubun}/_meta.parquet           行・列の一覧（/chizai/meta 用）
#   chizai/{kubun}/_state.json             取込済みCSVの更新日（差分更新用）

import io
import json
import os
import re
import shutil
import tempfile
import time
import unicodedata

import httpx
import pandas as pd
from google.cloud import storage

API_URL       = "https://api.e-stat.go.jp/rest/3.0/app/json/getDataCatalog"
STATS_CODE    = "00200251"
BUCKET_NAME   = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
PREFIX        = "chizai"
API_INTERVAL  = 30   # カタログAPIの呼出間隔（秒）。e-StatはIP単位で429になるため直列・間隔あり
FILE_INTERVAL = 2    # CSVダウンロードの間隔（秒）

# 区分 → カタログ検索語。市町村分は後で "city": "市町村分" を追加する
KUBUN_WORD = {"pref": "都道府県分"}

# CSV先頭10列の標準名（年代で「県名」等の表記揺れ・末尾空白があるため位置で付け直す）
HEAD10   = ["決算年度", "業務コード", "団体コード", "都道府県名", "団体名", "団体区分", "表番号", "表名称", "行番号", "行名称"]
KEY_COLS = ["決算年度", "団体コード", "市区町村コード", "都道府県名", "団体名", "団体区分", "表番号", "表名称", "行番号", "行名称"]
OUT_COLS = KEY_COLS + ["列番号", "列名称", "値"]
STR_COLS = [c for c in OUT_COLS if c not in ("決算年度", "値")]
SORT_COLS = ["決算年度", "団体コード", "行番号", "列番号"]


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


def fetch_catalog(kubun: str) -> list[dict]:
    # getDataCatalogで調査表CSVの一覧（年×表）を取得する
    word   = KUBUN_WORD[kubun]
    params = {"appId": os.environ["ESTAT_APP_ID"], "statsCode": STATS_CODE,
              "searchWord": word, "dataType": "CSV", "limit": 100}
    items, start = [], 1
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
                ds_text = json.dumps(cat.get("DATASET", {}), ensure_ascii=False)
                # 同じ検索語で別分類（決算状況調など）が混ざらないよう「調査表」を含むものに限る
                if word not in ds_text or "調査表" not in ds_text:
                    continue
                cat_id = str(cat.get("@id", ""))
                for res in _as_list(cat.get("RESOURCES", {}).get("RESOURCE")):
                    if str(res.get("FORMAT", "")).upper() != "CSV":
                        continue
                    items.append({
                        "catalog":  cat_id,
                        "name":     _text(res.get("TITLE")),
                        "url":      str(res.get("URL", "")),
                        "modified": str(res.get("LAST_MODIFIED_DATE", "")),
                    })

            nxt = inf.get("RESULT_INF", {}).get("NEXT_KEY")
            if not nxt:
                break
            start = int(nxt)
            time.sleep(API_INTERVAL)
    return items


# ── CSVの整形 ──────────────────────────────────────────────

def _split_col(col: str, pos: int) -> tuple[str, str]:
    # 列見出し「001:歳入総額」→（"001", "歳入総額"）。番号がなければ位置で振る
    m = re.match(r"^(\d+)\s*[:：]\s*(.*)$", col)
    if m:
        return m.group(1).zfill(3), m.group(2).strip()
    return str(pos).zfill(3), col


def normalize(raw: bytes) -> pd.DataFrame:
    # 調査表CSV（Shift-JIS・横持ち）を縦持ちの標準スキーマに変換する
    df = pd.read_csv(io.BytesIO(raw), encoding="cp932", encoding_errors="replace", dtype=str)
    df.columns = [str(c).strip() for c in df.columns]   # 全角を含む前後の空白を除去
    if len(df.columns) < 11:
        raise ValueError(f"列数不足: {list(df.columns)}")

    val_cols   = list(df.columns[10:])
    df.columns = HEAD10 + val_cols
    df[HEAD10] = df[HEAD10].apply(lambda s: s.str.strip())

    # 行番号の塊ごとに混入する見出し行を除外（決算年度が4桁数字の行だけ残す）
    df = df[df["決算年度"].fillna("").str.fullmatch(r"\d{4}")].copy()

    # 全国計（団体コード欄が「合計(全国)」）
    zenkoku = df["団体コード"].fillna("").str.contains("合計")
    df.loc[zenkoku, "団体コード"] = "000000"
    df.loc[zenkoku, "都道府県名"] = "全国"
    df.loc[zenkoku, "団体名"]     = "全国"

    # 古い年度は先頭の0が落ちているため0埋め
    df["団体コード"]     = df["団体コード"].str.zfill(6)
    df["市区町村コード"] = df["団体コード"].str[:5]
    df["表番号"]         = df["表番号"].str.zfill(2)
    df["行番号"]         = df["行番号"].str.zfill(2)

    # 列を縦持ちにし、列見出しを番号と名称に分ける
    col_map = {c: _split_col(c, i) for i, c in enumerate(val_cols, 1)}
    long = df.melt(id_vars=KEY_COLS, value_vars=val_cols, var_name="列見出し", value_name="値")
    long["列番号"] = long["列見出し"].map(lambda c: col_map[c][0])
    long["列名称"] = long["列見出し"].map(lambda c: col_map[c][1])

    # 値は数値化（"-"・空欄・カンマ区切りに対応。変換できないものは null）
    v = long["値"].str.replace(",", "", regex=False).str.strip()
    long["値"]       = pd.to_numeric(v, errors="coerce")
    long["決算年度"] = long["決算年度"].astype("int32")
    long[STR_COLS]   = long[STR_COLS].astype("string")   # 全て空の列もParquetで文字列型にする
    return fix_continuation(long[OUT_COLS])


def fix_continuation(long: pd.DataFrame) -> pd.DataFrame:
    # 用紙の「続きの行」の誤った列名称を消す。
    # 表04などは1つの様式を複数の行に折り返しており、行02以降は行01と別の項目が並ぶが、
    # CSVの見出しは行01の項目名しか持たないため、行02以降に行01の列名称が付いてしまう。
    # 同じ年度・同じ表で、2つ以上の行がすべて同じ行名称（例:「決算額」）なら続きの行とみなし、
    # 最初の行以外の列名称を null にする（正しい項目名は app/data/chizai_items.csv で付ける）
    out = long.copy()
    for (y, hyo), g in out.groupby(["決算年度", "表番号"]):
        if g["行番号"].nunique() > 1 and g["行名称"].nunique(dropna=False) == 1:
            first = g["行番号"].min()
            out.loc[g.index[g["行番号"] != first], "列名称"] = pd.NA
    return out


# ── GCS ────────────────────────────────────────────────────

def _bucket():
    return storage.Client().bucket(BUCKET_NAME)


def _save_df(df: pd.DataFrame, path: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    _bucket().blob(path).upload_from_file(buf, content_type="application/octet-stream")
    print(f"✅ gs://{BUCKET_NAME}/{path} ({len(df)}件)", flush=True)


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


# ── 索引（表一覧・行列一覧） ───────────────────────────────
# 同じ表番号・行番号・列番号でも、年度によって別の表・項目に使われていることがある。
# 名称を正規化して「同じもの」の塊にまとめ、塊ごとに収録年度を持たせる。

TABLE_COLS = ["表番号", "表名称", "決算年度_最初", "決算年度_最新", "年度数", "件数", "表記ゆれ", "同番号の別表"]
META_COLS  = ["表番号", "区分", "番号", "名称", "決算年度_最初", "決算年度_最新", "年度数", "表記ゆれ"]


def _norm(name) -> str:
    # 名称の比較用キー：全角半角・空白・区切り記号の違いと、名称中の年度（令和6年度・元年度など）を無視する
    s = unicodedata.normalize("NFKC", "" if name is None or pd.isna(name) else str(name))
    s = re.sub(r"(令和|平成|昭和)?(\d+|元)年度", "〇年度", s)
    return re.sub(r"[\s・,，、]", "", s)


def _group_names(rows: pd.DataFrame, merge_contained: bool) -> list[dict]:
    # rows: 決算年度・名称・件数（1つの番号分）。正規化名称ごとにまとめる
    # merge_contained=True のとき、名称が別の名称に含まれるもの（例:「その1歳入内訳」）も同じ塊にまとめる
    rows = rows.assign(キー=rows["名称"].map(_norm))
    groups = {}
    for key, g in rows.groupby("キー", sort=False):
        g = g.sort_values("決算年度")
        groups[key] = {"years": set(g["決算年度"]), "count": int(g["件数"].sum()), "key": key,
                       "variants": {}, "latest": int(g["決算年度"].max()), "latest_name": g["名称"].iloc[-1]}
    if merge_contained:
        for key in sorted(groups, key=len):
            if key not in groups or len(key) < 4:
                continue
            host = [k for k in groups if k != key and key in k]
            if host:
                h = groups[max(host, key=len)]
                g = groups.pop(key)
                h["years"] |= g["years"]
                h["count"] += g["count"]
                h["variants"][g["key"]] = g["latest_name"]
                h["variants"].update(g["variants"])
                if g["latest"] > h["latest"]:
                    h["variants"][h["key"]] = h["latest_name"]
                    h["latest"], h["latest_name"], h["key"] = g["latest"], g["latest_name"], g["key"]
                    h["variants"].pop(g["key"], None)
    out = []
    for g in groups.values():
        # 表記ゆれ：省略形など、空白・記号・年度以外が違う表記だけを出す（年度違いまで並べると読めないため）
        others = list(g["variants"].values())
        out.append({"名称": g["latest_name"], "決算年度_最初": int(min(g["years"])), "決算年度_最新": int(max(g["years"])),
                    "年度数": len(g["years"]), "件数": g["count"], "表記ゆれ": " / ".join(others) if others else None})
    return sorted(out, key=lambda r: r["決算年度_最初"])


def _table_groups(df: pd.DataFrame) -> list[dict]:
    # 1つの表番号の中で、表名称を「同じ表」の塊にまとめる。次のどちらかなら同じ表とみなす
    #   (1) 正規化した名称が一致する、一方が他方に含まれる（省略形）、または「その○」の番号だけが違う
    #   (2) 行・列の項目名が半分以上重なる（「その3」→「その2」の繰り上げなど、改名だけのもの）
    nm = df.groupby(["決算年度", "表名称"], dropna=False).size().reset_index(name="件数")
    nm["キー"] = nm["表名称"].map(_norm)
    keys = list(dict.fromkeys(nm["キー"]))
    parent = {k: k for k in keys}

    def find(k):
        while parent[k] != k:
            parent[k] = parent[parent[k]]
            k = parent[k]
        return k

    def union(a, b):
        parent[find(a)] = find(b)

    def drop_sono(k):
        return re.sub(r"その\d+", "", k)

    for a in keys:
        for b in keys:
            if a == b:
                continue
            # 省略形（一方が他方に含まれる）、または「その3」→「その2」のような番号の繰り上げだけの違い
            if (len(a) >= 4 and a in b) or (drop_sono(a) and drop_sono(a) == drop_sono(b)):
                union(a, b)

    it = df[["表名称", "行名称", "列名称"]].drop_duplicates()
    it = it.assign(キー=it["表名称"].map(_norm))
    items = {k: {"行:" + _norm(x) for x in g["行名称"]} | {"列:" + _norm(x) for x in g["列名称"]} for k, g in it.groupby("キー")}
    for i, a in enumerate(keys):
        for b in keys[i + 1:]:
            A, B = items.get(a, set()), items.get(b, set())
            if A and B and len(A & B) / len(A | B) >= 0.5:
                union(a, b)

    nm["塊"] = nm["キー"].map(find)
    out = []
    for _, g in nm.groupby("塊"):
        g = g.sort_values("決算年度")
        named = g[g["表名称"].notna()]
        latest = named["表名称"].iloc[-1] if len(named) else None
        lk = _norm(latest)
        # 表記ゆれ：空白・記号・年度だけの違いは除き、名称ごとに最後に使われた表記を1つずつ出す
        others = list(dict.fromkeys(named.loc[named["キー"] != lk].drop_duplicates("キー", keep="last")["表名称"]))
        years = set(g["決算年度"])
        out.append({"表名称": latest, "決算年度_最初": int(min(years)), "決算年度_最新": int(max(years)), "年度数": len(years),
                    "件数": int(g["件数"].sum()), "表記ゆれ": " / ".join(others) if others else None})
    return sorted(out, key=lambda r: r["決算年度_最初"])


def _index_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # 1つの表番号のデータから、表一覧と行・列一覧を作る
    hyo = df["表番号"].iloc[0]
    tables = _table_groups(df)
    for r in tables:
        r["表番号"] = hyo
        r["同番号の別表"] = len(tables) > 1
    table = pd.DataFrame(tables, columns=TABLE_COLS)

    parts = []
    for kind, no, name in [("行", "行番号", "行名称"), ("列", "列番号", "列名称")]:
        u = df[df[name].notna()].groupby([no, "決算年度", name]).size().reset_index(name="件数").rename(columns={name: "名称"})
        for num, g in u.groupby(no):
            for r in _group_names(g, merge_contained=False):
                r.pop("件数")
                parts.append({"表番号": hyo, "区分": kind, "番号": num, **r})
    meta = pd.DataFrame(parts, columns=META_COLS)
    meta[["決算年度_最初", "決算年度_最新", "年度数"]] = meta[["決算年度_最初", "決算年度_最新", "年度数"]].astype("int32")
    return table, meta


def _save_index(kubun: str, tables: list, metas: list):
    t = pd.concat(tables, ignore_index=True).sort_values(["表番号", "決算年度_最初"]).reset_index(drop=True)
    m = pd.concat(metas, ignore_index=True).sort_values(["表番号", "区分", "番号", "決算年度_最初"]).reset_index(drop=True)
    _save_df(t, f"{PREFIX}/{kubun}/_tables.parquet")
    _save_df(m, f"{PREFIX}/{kubun}/_meta.parquet")
    return t


def reindex(kubun: str) -> int:
    # 保存済みの表データ（data.parquet）から索引だけを作り直す。CSVの再取得はしない
    cols = ["決算年度", "表番号", "表名称", "行番号", "行名称", "列番号", "列名称"]
    paths = sorted(b.name for b in _bucket().client.list_blobs(BUCKET_NAME, prefix=f"{PREFIX}/{kubun}/") if b.name.endswith("/data.parquet"))
    tables, metas = [], []
    for p in paths:
        df = pd.read_parquet(io.BytesIO(_bucket().blob(p).download_as_bytes()), columns=cols)
        t, m = _index_rows(df)
        tables.append(t)
        metas.append(m)
        print(f"[reindex] {p} 表名称{len(t)}種 行列{len(m)}件", flush=True)
    t = _save_index(kubun, tables, metas)
    multi = t[t["同番号の別表"]]
    print(f"🔀 同じ表番号に別の表があるもの: {multi['表番号'].nunique()}表番号", flush=True)
    print(multi[["表番号", "決算年度_最初", "決算年度_最新", "年度数", "表名称"]].to_string(index=False), flush=True)
    yure = t[t["表記ゆれ"].notna()]
    print(f"📝 表記ゆれをまとめたもの: {len(yure)}件", flush=True)
    print(yure[["表番号", "表名称", "表記ゆれ"]].to_string(index=False), flush=True)
    return len(paths)


def fix_saved_continuation(kubun: str, write: bool) -> int:
    # 保存済みの表データに fix_continuation を当てる。write=False なら件数の確認だけ
    paths = sorted(b.name for b in _bucket().client.list_blobs(BUCKET_NAME, prefix=f"{PREFIX}/{kubun}/") if b.name.endswith("/data.parquet"))
    changed = 0
    for p in paths:
        df = pd.read_parquet(io.BytesIO(_bucket().blob(p).download_as_bytes()))
        fixed = fix_continuation(df)
        n = int((df["列名称"].notna() & fixed["列名称"].isna()).sum())
        if n:
            yrs = sorted(fixed.loc[df["列名称"].notna() & fixed["列名称"].isna(), "決算年度"].unique().tolist())
            print(f"[fixcont] {p}: 列名称を消す {n}件 年度 {yrs[0]}-{yrs[-1]}（{len(yrs)}年）", flush=True)
            changed += 1
            if write:
                fixed[STR_COLS] = fixed[STR_COLS].astype("string")
                _save_df(fixed, p)
    if write and changed:
        reindex(kubun)
    print(f"[fixcont] 対象 {changed}表 / write={write}", flush=True)
    return changed


# ── 実行 ───────────────────────────────────────────────────

def run(kubun: str = "pref", mode: str = "dryrun") -> int:
    # mode: dryrun=一覧と1ファイルの整形結果を表示のみ / full=全期間を取り直す / update=更新されたCSVだけ取り込む
    #       reindex=保存済みデータから表一覧・行列一覧だけ作り直す
    if mode not in ("dryrun", "full", "update", "reindex", "fixcont_dryrun", "fixcont"):
        raise ValueError(f"mode は dryrun / full / update / reindex / fixcont_dryrun / fixcont: {mode}")
    if mode == "reindex":
        return reindex(kubun)
    if mode in ("fixcont_dryrun", "fixcont"):
        return fix_saved_continuation(kubun, write=(mode == "fixcont"))

    items = fetch_catalog(kubun)
    print(f"📋 CSVリソース {len(items)}件 / カタログ {len({it['catalog'] for it in items})}件", flush=True)

    if mode == "dryrun":
        per_cat = pd.Series([it["catalog"] for it in items]).value_counts().sort_index()
        print(per_cat.to_string())
        sample = items[-1]
        print("サンプル:", sample)
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            r = client.get(sample["url"])
            r.raise_for_status()
        df = normalize(r.content)
        print(df.shape)
        print(df.head(12).to_string())
        print(df.groupby(["決算年度", "表番号", "表名称"]).size())
        return 0

    state_path = f"{PREFIX}/{kubun}/_state.json"
    state = {} if mode == "full" else _load_json(state_path)

    # 更新のあった年（カタログ）は、その年の全CSVを取り直す（表の一部だけ差し替わるのを防ぐ）
    changed = {it["catalog"] for it in items if state.get(it["url"]) != it["modified"]}
    targets = [it for it in items if it["catalog"] in changed]
    print(f"🎯 取込対象 {len(targets)}件（{len(changed)}カタログ）", flush=True)
    if not targets:
        return 0

    # 全期間分をメモリに載せると大きいため、整形結果を表番号ごとに一時Parquetへ書き出す
    work = tempfile.mkdtemp(prefix="chizai_")
    errors = []
    try:
        with httpx.Client(timeout=120, follow_redirects=True) as client:
            for i, it in enumerate(targets, 1):
                try:
                    r = client.get(it["url"])
                    r.raise_for_status()
                    df = normalize(r.content)
                    for hyo, g in df.groupby("表番号"):
                        os.makedirs(f"{work}/{hyo}", exist_ok=True)
                        g.to_parquet(f"{work}/{hyo}/{i:05d}.parquet", index=False)
                    state[it["url"]] = it["modified"]
                    print(f"[{i}/{len(targets)}] OK {it['name']} 年度={sorted(df['決算年度'].unique().tolist())} {len(df)}件", flush=True)
                except Exception as e:
                    errors.append(f"{it['name']} {it['url']}: {type(e).__name__}: {e}")
                    print(f"[{i}/{len(targets)}] NG {errors[-1]}", flush=True)
                time.sleep(FILE_INTERVAL)

        # 表番号ごとに結合 → 既存分とマージ → 保存
        tables = _load_df(f"{PREFIX}/{kubun}/_tables.parquet") if mode == "update" else None
        meta   = _load_df(f"{PREFIX}/{kubun}/_meta.parquet") if mode == "update" else None
        new_tables, new_meta, done = [], [], []
        for hyo in sorted(os.listdir(work)):
            add  = pd.concat([pd.read_parquet(f"{work}/{hyo}/{f}") for f in sorted(os.listdir(f"{work}/{hyo}"))], ignore_index=True)
            path = f"{PREFIX}/{kubun}/{hyo}/data.parquet"
            old  = _load_df(path) if mode == "update" else None
            if old is not None:
                old = old[~old["決算年度"].isin(add["決算年度"].unique())]
                add = pd.concat([old, add], ignore_index=True)
            # 完全に同一の行だけ除く。キー重複（同じ年度・団体・行・列で値が別）は消さずに警告する
            add = add.drop_duplicates()
            dup = add.duplicated(subset=["決算年度", "団体コード", "行番号", "列番号"], keep=False)
            if dup.any():
                yrs = sorted(add.loc[dup, "決算年度"].unique().tolist())
                print(f"⚠️ 表{hyo}: キー重複 {int(dup.sum())}件 年度={yrs}", flush=True)
            add = add.sort_values(SORT_COLS).reset_index(drop=True)
            _save_df(add, path)
            t, m = _index_rows(add)
            new_tables.append(t)
            new_meta.append(m)
            done.append(hyo)

        # 索引は今回触った表だけ差し替える
        if tables is not None:
            new_tables.append(tables[~tables["表番号"].isin(done)])
        if meta is not None:
            new_meta.append(meta[~meta["表番号"].isin(done)])
        _save_index(kubun, new_tables, new_meta)
        _save_json(state, state_path)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print(f"🏁 完了: 表{len(done)}件 / エラー{len(errors)}件", flush=True)
    for e in errors:
        print("  ❌", e)
    if errors:
        raise RuntimeError(f"{len(errors)}件のCSVが取り込めませんでした（上記ログ参照）")
    return len(done)
