# 国税庁 法人番号公表サイト「基本3情報ダウンロード」全件データ（全国・閉鎖法人を含む）の収集
# 都道府県別＋国外の全件ファイル（CSV・Unicode、ZIP）を取得し、都道府県ごとのParquetにしてGCSへ保存する。
# 実行は GitHub Actions（scripts/collect_houjin.py）から。全件ファイルは毎月末時点で作成され、翌月1日に公開される。
#
# ダウンロードの仕組み（ページはJavaScriptで動くが、中身は単純なフォームPOST）:
#   1. 全件ダウンロードのページをGETしてセッション（Cookie）とトークン（hidden項目）を得る
#   2. 各ファイルのリンクの onclick にファイル番号が入っている
#   3. トークン＋selDlFileNo（ファイル番号）＋event=download をPOSTすると ZIP が返る
#
# GCS:
#   houjin/zenken/pref=01.parquet … pref=47.parquet, pref=99.parquet（国外）
#   houjin/zenken/_meta.json   基準日・取得日時・都道府県別の件数（/houjin/meta 用）
#
# 元CSVはヘッダー行なし・30列（リソース定義書の順）。列数が違えば処理を止める。

import io
import json
import os
import re
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage

PAGE_URL     = "https://www.houjin-bangou.nta.go.jp/download/zenken/index.html"
TOKEN_NAME   = "jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token"
BUCKET_NAME  = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
PREFIX       = "houjin/zenken"
META_PATH    = f"{PREFIX}/_meta.json"
FILE_INTERVAL = 5   # ダウンロードの間隔（秒）。短時間の大量アクセスを避けるため直列で間隔を空ける
JST = timezone(timedelta(hours=9))

# 元CSVの列（リソース定義書の順）
RAW_COLS = [
    "一連番号", "法人番号", "処理区分", "訂正区分", "更新年月日", "変更年月日", "商号", "商号イメージID",
    "法人種別", "都道府県", "市区町村", "丁目番地等", "所在地イメージID", "都道府県コード", "市区町村コード",
    "郵便番号", "国外所在地", "国外所在地イメージID", "閉鎖年月日", "閉鎖事由", "承継先法人番号",
    "変更事由の詳細", "法人番号指定年月日", "最新履歴", "英語商号", "英語都道府県", "英語市区町村丁目番地等",
    "英語国外所在地", "フリガナ", "検索対象除外",
]

# 保存する列（この順でParquetに出す）
OUT_COLS = [
    "法人番号", "商号", "フリガナ", "英語商号", "法人種別コード", "法人種別",
    "都道府県コード", "団体コード", "都道府県", "市区町村", "丁目番地等", "郵便番号", "国外所在地",
    "状態", "閉鎖年月日", "閉鎖事由コード", "閉鎖事由", "承継先法人番号",
    "法人番号指定年月日", "変更年月日", "更新年月日", "最終処理区分コード", "最終処理区分", "変更事由の詳細",
    "検索対象除外",
]

KIND = {
    "101": "国の機関", "201": "地方公共団体", "301": "株式会社", "302": "有限会社", "303": "合名会社",
    "304": "合資会社", "305": "合同会社", "399": "その他の設立登記法人", "401": "外国会社等", "499": "その他",
}
CLOSE_CAUSE = {"01": "清算の結了等", "11": "合併による解散等", "21": "登記官による閉鎖", "31": "その他の清算の結了等"}
PROCESS = {
    "01": "新規", "11": "商号又は名称の変更", "12": "国内所在地の変更", "13": "国外所在地の変更",
    "21": "登記記録の閉鎖等", "22": "登記記録の閉鎖等の取消し", "71": "吸収合併", "72": "吸収合併無効",
    "81": "商号の登記の抹消", "99": "削除",
}

PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "茨城県", "栃木県", "群馬県",
    "埼玉県", "千葉県", "東京都", "神奈川県", "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "福岡県",
    "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
PREF_CODE = {name: f"{i + 1:02d}" for i, name in enumerate(PREFS)}
CODE_KOKUGAI = "99"   # 国外


# ── ダウンロードページ ─────────────────────────────────────

def _client() -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(60.0, read=600.0),
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (stats-api collector; personal statistics use)"},
    )


def _label_of(row_text: str):
    # 行のテキスト（都道府県名・国外・全国）から都道府県コードを返す。対象外なら None
    # 表の先頭列は地域（東北・関東…、rowspan）なので、行のどこかに都道府県名があるかで判定する
    t = re.sub(r"\s", "", row_text)
    if "全国" in t:
        return None
    if "国外" in t:
        return CODE_KOKUGAI
    hits = [code for name, code in PREF_CODE.items() if name in t]
    return hits[0] if len(hits) == 1 else None


def fetch_page(client: httpx.Client):
    # ページを取得し、トークンと「CSV・Unicode」のファイル一覧 {都道府県コード: [ファイル番号, …]} を返す
    # ほかに、確認用の生の一覧（形式・見出し・番号）も返す
    r = client.get(PAGE_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    tok = soup.find("input", {"name": TOKEN_NAME})
    if tok is None:
        raise RuntimeError("トークン（hidden項目）が見つかりません。ページの構造が変わった可能性があります")
    token = tok.get("value", "")

    listing = []   # (形式の見出し, 行の見出し, ファイル番号, 行のテキスト)
    for a in soup.find_all("a", onclick=True):
        m = re.search(r"(\d{3,})", a.get("onclick", ""))
        if not m:
            continue
        tr = a.find_parent("tr")
        row_text = tr.get_text(" ", strip=True) if tr else a.get_text(" ", strip=True)
        # 形式（CSV Shift-JIS / CSV Unicode / XML）は、表の見出し・キャプション・直前の見出しのどれかに書かれている
        table = a.find_parent("table")
        fmt = ""
        if table is not None:
            cap = table.find("caption")
            head = table.find_previous(["h2", "h3", "h4", "h5"])
            fmt = " ".join(x.get_text(" ", strip=True) for x in [cap, head] if x is not None)
            th = table.find("th")
            if th is not None:
                fmt += " " + th.get_text(" ", strip=True)
        cells = tr.find_all(["th", "td"]) if tr else []
        label = cells[0].get_text(" ", strip=True) if cells else row_text
        listing.append((fmt, label, m.group(1), row_text))

    # CSV・Unicode の行だけを都道府県コードに対応付ける（東京都など1つの都道府県が複数ファイルに分かれることがある）。
    # 形式が見出しで判別できない場合に備え、1行に複数リンク（形式別の列）があるときは列の位置でも判別する
    files = {}
    for fmt, label, no, row_text in listing:
        f = re.sub(r"\s", "", fmt)
        if "Unicode" in f and "CSV" in f.upper():
            code = _label_of(row_text)
            if code and no not in files.setdefault(code, []):
                files[code].append(no)
    if len(files) < 48:
        files = _files_by_column(soup) or files
    return token, files, listing


def _files_by_column(soup: BeautifulSoup) -> dict:
    # 表の列が「CSV(Shift-JIS) / CSV(Unicode) / XML」の並びになっている場合の判別
    files = {}
    for table in soup.find_all("table"):
        header = [c.get_text(" ", strip=True) for c in (table.find("tr") or soup.new_tag("tr")).find_all(["th", "td"])]
        idx = next((i for i, h in enumerate(header) if "Unicode" in h and "CSV" in h.upper()), None)
        if idx is None:
            continue
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["th", "td"])
            if len(cells) <= idx:
                continue
            code = _label_of(tr.get_text(" ", strip=True))
            a = cells[idx].find("a", onclick=True)
            m = re.search(r"(\d{3,})", a.get("onclick", "")) if a else None
            if code and m and m.group(1) not in files.setdefault(code, []):
                files[code].append(m.group(1))
    return files


def download(client: httpx.Client, file_no: str):
    # 1ファイルをダウンロードして (ZIPのバイト列, ファイル名) を返す。トークンは毎回ページから取り直す
    token, _, _ = fetch_page(client)
    r = client.post(PAGE_URL, data={TOKEN_NAME: token, "selDlFileNo": file_no, "event": "download"})
    r.raise_for_status()
    if not r.content.startswith(b"PK"):
        raise ValueError(f"ZIPではありません（file_no={file_no}, content-type={r.headers.get('content-type')}）")
    cd = r.headers.get("content-disposition", "")
    m = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", cd)
    fname = re.sub(r"^[^']*'[^']*'", "", m.group(1)) if m else ""   # utf-8'jp'01_hokkaido_all_….zip → 01_hokkaido_all_….zip
    return r.content, fname


# ── 変換 ───────────────────────────────────────────────────

def read_zip(content: bytes) -> pd.DataFrame:
    # ZIPの中のCSV（ヘッダーなし・30列）を読む。Unicode版は UTF-8。念のため cp932 にも対応する
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise ValueError(f"ZIPにCSVがありません: {z.namelist()}")
        raw = z.read(names[0])
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp932")
    df = pd.read_csv(io.StringIO(text), header=None, dtype=str, keep_default_na=False)
    if df.shape[1] != len(RAW_COLS):
        raise ValueError(f"列数が {df.shape[1]} です（想定 {len(RAW_COLS)}）。リソース定義書の変更を確認してください")
    df.columns = RAW_COLS
    return df


def normalize(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.mask(raw == "")   # 空文字を欠損にする
    out = pd.DataFrame({
        "法人番号": df["法人番号"],
        "商号": df["商号"],
        "フリガナ": df["フリガナ"],
        "英語商号": df["英語商号"],
        "法人種別コード": df["法人種別"],
        "法人種別": df["法人種別"].map(KIND),
        "都道府県コード": df["都道府県コード"].str.zfill(2),
        "都道府県": df["都道府県"],
        "市区町村": df["市区町村"],
        "丁目番地等": df["丁目番地等"],
        "郵便番号": df["郵便番号"],
        "国外所在地": df["国外所在地"],
        "閉鎖年月日": df["閉鎖年月日"],
        "閉鎖事由コード": df["閉鎖事由"],
        "閉鎖事由": df["閉鎖事由"].map(CLOSE_CAUSE),
        "承継先法人番号": df["承継先法人番号"],
        "法人番号指定年月日": df["法人番号指定年月日"],
        "変更年月日": df["変更年月日"],
        "更新年月日": df["更新年月日"],
        "最終処理区分コード": df["処理区分"],
        "最終処理区分": df["処理区分"].map(PROCESS),
        "変更事由の詳細": df["変更事由の詳細"],
        "検索対象除外": df["検索対象除外"].fillna("0") == "1",
    })
    # 団体コード（5桁、検査数字なし）= 都道府県コード2桁 + 市区町村コード3桁。_M_city の code_5_digit と結合できる
    city = df["市区町村コード"].str.zfill(3)
    out["団体コード"] = (out["都道府県コード"] + city).where(out["都道府県コード"].notna() & city.notna())
    out["状態"] = out["閉鎖年月日"].notna().map({True: "閉鎖", False: "存続"})
    return out[OUT_COLS].sort_values("法人番号").reset_index(drop=True)


# ── GCS ────────────────────────────────────────────────────

def _bucket():
    return storage.Client().bucket(BUCKET_NAME)


def _base_date(filename: str):
    # ファイル名の8桁の日付（例: 20_nagano_all_20260930.zip）を基準日にする
    m = re.search(r"(20\d{6})", filename or "")
    return f"{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:]}" if m else None


def _summary(code: str, df: pd.DataFrame) -> dict:
    return {
        "都道府県コード": code,
        "都道府県": "国外" if code == CODE_KOKUGAI else PREFS[int(code) - 1],
        "件数": int(len(df)),
        "存続": int((df["状態"] == "存続").sum()),
        "閉鎖": int((df["状態"] == "閉鎖").sum()),
    }


# ── 実行 ───────────────────────────────────────────────────

def run(mode: str = "dryrun") -> int:
    # dryrun: ファイル一覧を確認し、小さい1ファイル（鳥取県）だけ読んで列と件数を表示する。GCSには書かない
    # full  : 全48ファイルを取得・変換し、すべて成功したときだけGCSへ保存する
    if mode not in ("dryrun", "full"):
        raise ValueError("mode は dryrun または full")
    client = _client()
    try:
        token, files, listing = fetch_page(client)
        print(f"🔎 トークン: {'あり' if token else 'なし'} / リンク {len(listing)}件 / CSV・Unicode 対応付け {len(files)}件", flush=True)

        if mode == "dryrun":
            print("── CSV・Unicode の対応付け（都道府県コード → ファイル番号）──", flush=True)
            print("  " + ", ".join(f"{c}:{'+'.join(files[c])}" for c in sorted(files)), flush=True)
            uni = [x for x in listing if "Unicode" in x[0] and "CSV" in x[0].upper()]
            print(f"── CSV・Unicode のリンク {len(uni)}件（行テキスト | ファイル番号）──", flush=True)
            for fmt, label, no, row_text in uni:
                print(f"  {row_text[:40]} | {no}", flush=True)
            missing = [c for c in list(PREF_CODE.values()) + [CODE_KOKUGAI] if c not in files]
            print(f"対応付けできなかった都道府県コード: {missing or 'なし'}", flush=True)
            target = "31" if "31" in files else next(iter(files), None)
            if target is None:
                print("⚠️ CSV・Unicode のファイルを特定できませんでした。上の一覧で形式の見出しを確認してください", flush=True)
                return 0
            time.sleep(FILE_INTERVAL)
            content, fname = download(client, files[target][0])
            raw = read_zip(content)
            df = normalize(raw)
            print(f"📦 {fname}（{len(content) / 1e6:.1f}MB）基準日={_base_date(fname)}", flush=True)
            print(f"   行数 {len(df):,} / {_summary(target, df)}", flush=True)
            print(f"   都道府県コードの値: {sorted(df['都道府県コード'].dropna().unique().tolist())}", flush=True)
            print(f"   法人種別: {df['法人種別コード'].value_counts().to_dict()}", flush=True)
            print(f"   名称に対応しないコード: 種別 {sorted(set(df.loc[df['法人種別'].isna(), '法人種別コード'].dropna()))}"
                  f" / 閉鎖事由 {sorted(set(df.loc[df['閉鎖事由'].isna(), '閉鎖事由コード'].dropna()))}"
                  f" / 処理区分 {sorted(set(df.loc[df['最終処理区分'].isna(), '最終処理区分コード'].dropna()))}", flush=True)
            print("   先頭1行:", df.head(1).to_dict(orient="records"), flush=True)
            return 1

        # full
        need = list(PREF_CODE.values()) + [CODE_KOKUGAI]
        missing = [c for c in need if c not in files]
        if missing:
            raise RuntimeError(f"ファイル番号を特定できない都道府県があります: {missing}（dryrun で一覧を確認してください）")
        workdir = tempfile.mkdtemp()
        summaries, base_dates = [], set()
        for code in need:
            parts, fnames = [], []
            for no in files[code]:
                time.sleep(FILE_INTERVAL)
                content, fname = download(client, no)
                parts.append(read_zip(content))
                fnames.append(fname)
                del content
            df = normalize(pd.concat(parts, ignore_index=True))
            del parts
            if df["法人番号"].duplicated().any():
                raise RuntimeError(f"{code}: 法人番号が重複しています（{int(df['法人番号'].duplicated().sum())}件）")
            if code != CODE_KOKUGAI:
                other = set(df["都道府県コード"].dropna()) - {code}
                if other:
                    print(f"  ⚠️ {code}: ほかの都道府県コードが混在 {sorted(other)}", flush=True)
            df.to_parquet(os.path.join(workdir, f"pref={code}.parquet"), index=False)
            s = _summary(code, df)
            summaries.append(s)
            base_dates.update(bd for bd in map(_base_date, fnames) if bd)
            print(f"  ✔ {code} {s['都道府県']}: {s['件数']:,}件（存続 {s['存続']:,} / 閉鎖 {s['閉鎖']:,}）{' + '.join(fnames)}", flush=True)
            del df

        if len(base_dates) > 1:
            raise RuntimeError(f"ファイルの基準日がそろっていません: {sorted(base_dates)}")

        # すべて成功したら保存する（途中で失敗したら何も書かない）
        bucket = _bucket()
        for code in need:
            bucket.blob(f"{PREFIX}/pref={code}.parquet").upload_from_filename(
                os.path.join(workdir, f"pref={code}.parquet"), content_type="application/octet-stream")
        meta = {
            "基準日": next(iter(base_dates), None),
            "取得日時": datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"),
            "出典": "国税庁法人番号公表サイト 基本3情報ダウンロード（全件データ）",
            "件数": sum(s["件数"] for s in summaries),
            "都道府県別": summaries,
        }
        bucket.blob(META_PATH).upload_from_string(json.dumps(meta, ensure_ascii=False), content_type="application/json")
        print(f"✅ gs://{BUCKET_NAME}/{PREFIX}/ に {len(need)}ファイル保存（計 {meta['件数']:,}件、基準日 {meta['基準日']}）", flush=True)
        return len(need)
    finally:
        client.close()
