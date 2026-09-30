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
    return long[OUT_COLS]


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

def _index_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # 1つの表から、表一覧の1行と行・列一覧を作る（名称は最新年度のもの）
    latest = df.sort_values("決算年度")
    table = pd.DataFrame([{
        "表番号": latest["表番号"].iloc[-1],
        "表名称": latest["表名称"].iloc[-1],
        "決算年度_最初": int(df["決算年度"].min()),
        "決算年度_最新": int(df["決算年度"].max()),
        "件数": len(df),
    }])
    parts = []
    for kind, no, name in [("行", "行番号", "行名称"), ("列", "列番号", "列名称")]:
        g = latest.groupby(no).agg(名称=(name, "last"), 決算年度_最初=("決算年度", "min"), 決算年度_最新=("決算年度", "max")).reset_index()
        g = g.rename(columns={no: "番号"})
        g.insert(0, "区分", kind)
        g.insert(0, "表番号", table["表番号"].iloc[0])
        parts.append(g)
    return table, pd.concat(parts, ignore_index=True)


# ── 実行 ───────────────────────────────────────────────────

def run(kubun: str = "pref", mode: str = "dryrun") -> int:
    # mode: dryrun=一覧と1ファイルの整形結果を表示のみ / full=全期間を取り直す / update=更新されたCSVだけ取り込む
    if mode not in ("dryrun", "full", "update"):
        raise ValueError(f"mode は dryrun / full / update: {mode}")

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
        _save_df(pd.concat(new_tables, ignore_index=True).sort_values("表番号").reset_index(drop=True), f"{PREFIX}/{kubun}/_tables.parquet")
        _save_df(pd.concat(new_meta, ignore_index=True).sort_values(["表番号", "区分", "番号"]).reset_index(drop=True), f"{PREFIX}/{kubun}/_meta.parquet")
        _save_json(state, state_path)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print(f"🏁 完了: 表{len(done)}件 / エラー{len(errors)}件", flush=True)
    for e in errors:
        print("  ❌", e)
    if errors:
        raise RuntimeError(f"{len(errors)}件のCSVが取り込めませんでした（上記ログ参照）")
    return len(done)
