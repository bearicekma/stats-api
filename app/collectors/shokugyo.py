# 職業分類マスタの作成
#   _M_shokugyo    : 日本標準職業分類（平成21年12月告示）大・中・小分類（総務省「分類項目名」CSV）
#   _M_shokugyo_hw : 厚生労働省編職業分類（令和4年改定）大・中・小分類（ハローワークインターネットサービスの職業分類ページ）
#                    中分類ごとに、対応する日本標準職業分類の中分類（app/data/shokugyo_hw_jsco.csv、人が確認する対応表）を付ける
#
# 使い方（ops/run.py から）:
#   save_jsco_master(dry_run=True) / save_hw_master(dry_run=True) で内容を確認し、問題なければ dry_run=False で GCS に保存する

import io
import os
import re
import time
import unicodedata

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import storage

from app.collectors.hellowork import BUCKET_NAME, USER_AGENT, _write_parquet

JSCO_CSV_URL = "https://www.soumu.go.jp/main_content/000587432.csv"
HW_TOP_URL   = "https://www.hellowork.mhlw.go.jp/info/mhlw_job_dictionary.html"
HW_PAGE_URL  = "https://www.hellowork.mhlw.go.jp/info/mhlw_job_dictionary_{}.html"
CROSSWALK    = os.path.join(os.path.dirname(__file__), "..", "data", "shokugyo_hw_jsco.csv")
JSCO_PATH    = "master/_M_shokugyo/data.parquet"
HW_PATH      = "master/_M_shokugyo_hw/data.parquet"
PAGE_INTERVAL = 2.0

JSCO_COLUMNS = ["code", "name", "chu_code", "chu_name", "dai_code", "dai_name"]
HW_COLUMNS   = ["code", "name", "chu_code", "chu_name", "dai_code", "dai_name",
                "jsco_chu_code", "jsco_chu_name", "examples", "not_examples"]


def _client() -> httpx.Client:
    return httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60, follow_redirects=True)


def _nfkc(s) -> str:
    return unicodedata.normalize("NFKC", str(s or "")).strip()


# ---------- 日本標準職業分類（_M_shokugyo） ----------

def build_jsco_master(client: httpx.Client | None = None) -> pd.DataFrame:
    # 総務省CSV（Shift_JIS、列: 大分類コード,中分類コード,小分類コード,項目名）から小分類1件1行の表を作る
    # 中分類コードは小分類コードの上2桁から導く（元CSVには「029 その他の法人・団体役員」の中分類が 3 になっている誤りがあるため）
    own = client is None
    client = client or _client()
    try:
        raw = client.get(JSCO_CSV_URL).content.decode("cp932")
    finally:
        if own:
            client.close()
    df = pd.read_csv(io.StringIO(raw), dtype=str).rename(columns=lambda c: c.strip())
    df.columns = ["dai", "chu", "sho", "name"]
    df["name"] = df["name"].str.strip()
    dai = df[(df["chu"] == "0") & (df["sho"] == "0")].set_index("dai")["name"].to_dict()
    chu_rows = df[(df["chu"] != "0") & (df["sho"] == "0")]
    chu = {c.zfill(2): n for c, n in zip(chu_rows["chu"], chu_rows["name"])}
    sho = df[df["sho"] != "0"].copy()
    sho["code"]     = sho["sho"].str.zfill(3)
    sho["chu_code"] = sho["code"].str[:2]
    sho["chu_name"] = sho["chu_code"].map(chu)
    sho["dai_code"] = sho["dai"]
    sho["dai_name"] = sho["dai_code"].map(dai)
    fixed = sho[sho["chu"].str.zfill(2) != sho["chu_code"]]

    problems = []
    if (len(dai), len(chu), len(sho)) != (12, 74, 329):
        problems.append(f"大 {len(dai)} / 中 {len(chu)} / 小 {len(sho)}（想定 12 / 74 / 329）")
    if sho[["chu_name", "dai_name"]].isna().any().any():
        problems.append(f"中分類・大分類に対応しない小分類: {sho.loc[sho['chu_name'].isna() | sho['dai_name'].isna(), 'code'].tolist()[:10]}")
    if sho["code"].duplicated().any():
        problems.append(f"小分類コードの重複: {sho.loc[sho['code'].duplicated(), 'code'].tolist()}")
    if problems:
        raise RuntimeError("日本標準職業分類CSVの形式が想定と違います: " + " / ".join(problems))
    print(f"_M_shokugyo: 大分類 {len(dai)} / 中分類 {len(chu)} / 小分類 {len(sho)} 件"
          f" / 元CSVの中分類を補正した行: {fixed[['code', 'chu', 'chu_code']].values.tolist()}")
    return sho[JSCO_COLUMNS].sort_values("code").reset_index(drop=True)


# ---------- 厚生労働省編職業分類（_M_shokugyo_hw） ----------

def _parse_hw_page(html: str) -> tuple[tuple[str, str], dict[str, str], list[dict]]:
    # 大分類ページ1枚から (大分類コード, 名称)、{中分類コード: 名称}、小分類のリストを取り出す
    soup = BeautifulSoup(html, "html.parser")
    m = re.match(r"(\d{2})\s*(.+)", _nfkc(soup.find("h1").get_text(" ", strip=True)))
    dai = (m.group(1), m.group(2).strip())
    chu, rows, cur = {}, [], None
    for tr in soup.find_all("tr"):
        for td in tr.find_all(["td", "th"]):
            text = td.get_text(" ", strip=True)
            norm = _nfkc(text)
            if norm in ("小分類", "") or re.match(r"^[〇○]\s*例示職業名", norm):
                continue  # 表の見出し行（「小分類」「〇例示職業名、☓例示職業名」）は読み飛ばす
            sm =re.match(r"^(\d{3}-\d{2})\s*(.+)$", norm)
            cm = re.match(r"^(\d{3})\s+(.+)$", norm)
            if sm:
                cur = {"code": sm.group(1), "name": re.sub(r"^\S+\s*", "", text.strip()), "examples": None, "not_examples": None}
                rows.append(cur)
            elif cm and cur is None:
                chu[cm.group(1)] = re.sub(r"^\S+\s*", "", text.strip())
            elif cur is not None and norm.startswith(("〇", "○")):
                cur["examples"] = re.sub(r"^[〇○]\s*", "", text.strip())
            elif cur is not None and norm.startswith(("☓", "×", "✕")):
                cur["not_examples"] = re.sub(r"^[☓×✕]\s*", "", text.strip())
    return dai, chu, rows


def build_hw_master(client: httpx.Client | None = None) -> pd.DataFrame:
    # ハローワークの職業分類ページ（大分類15枚）から小分類1件1行の表を作り、日本標準職業分類の中分類との対応を付ける
    own = client is None
    client = client or _client()
    try:
        top = client.get(HW_TOP_URL).content.decode("utf-8")
        pages = sorted(set(re.findall(r"mhlw_job_dictionary_(\d{2})\.html", top)))
        dai, chu, rows = {}, {}, []
        for p in pages:
            time.sleep(PAGE_INTERVAL)
            d, c, r = _parse_hw_page(client.get(HW_PAGE_URL.format(p)).content.decode("utf-8"))
            dai[d[0]] = d[1]
            chu.update({k: (v, d[0]) for k, v in c.items()})
            rows += r
    finally:
        if own:
            client.close()
    df = pd.DataFrame(rows).drop_duplicates("code")
    df["chu_code"] = df["code"].str[:3]
    df["chu_name"] = df["chu_code"].map(lambda c: chu.get(c, (None, None))[0])
    df["dai_code"] = df["chu_code"].map(lambda c: chu.get(c, (None, None))[1])
    df["dai_name"] = df["dai_code"].map(dai)

    # 対応表（→ 日本標準職業分類の中分類）。hw_code が小分類（NNN-NN）の行を優先し、なければ中分類（3桁）の行を使う
    df["jsco_chu_code"] = None
    df["jsco_chu_name"] = None
    if os.path.exists(CROSSWALK):
        cw = pd.read_csv(CROSSWALK, dtype=str).set_index("hw_code")
        key = df["code"].where(df["code"].isin(cw.index), df["chu_code"])
        df["jsco_chu_code"] = key.map(cw["jsco_chu_code"])
        df["jsco_chu_name"] = key.map(cw["jsco_chu_name"])
        unknown = sorted(set(cw.index) - set(df["code"]) - set(df["chu_code"]))
        if unknown:
            print(f"対応表にあってマスタにないコード: {unknown}")

    problems = []
    if (len(dai), len(chu), len(df)) != (15, 99, 439):
        problems.append(f"大 {len(dai)} / 中 {len(chu)} / 小 {len(df)}（想定 15 / 99 / 439）")
    if df[["chu_name", "dai_name"]].isna().any().any():
        problems.append(f"中分類・大分類に対応しない小分類: {df.loc[df['chu_name'].isna() | df['dai_name'].isna(), 'code'].tolist()[:10]}")
    if problems:
        raise RuntimeError("厚労省編職業分類ページの形式が想定と違います: " + " / ".join(problems))
    print(f"_M_shokugyo_hw: 大分類 {len(dai)} / 中分類 {len(chu)} / 小分類 {len(df)} 件"
          f" / 例示なし {df['examples'].isna().sum()} 件 / 対応表あり {df['jsco_chu_code'].notna().sum()} 件")
    return df[HW_COLUMNS].sort_values("code").reset_index(drop=True)


# ---------- 保存 ----------

def save_jsco_master(dry_run: bool = True) -> pd.DataFrame:
    # _M_shokugyo を作成して GCS に保存する（dry_run=True なら保存しない）
    df = build_jsco_master()
    if not dry_run:
        _write_parquet(storage.Client().bucket(BUCKET_NAME), JSCO_PATH, df)
        print(f"保存: {JSCO_PATH}")
    return df


def save_hw_master(dry_run: bool = True) -> pd.DataFrame:
    # _M_shokugyo_hw を作成して GCS に保存する（dry_run=True なら保存しない）
    df = build_hw_master()
    if not dry_run:
        _write_parquet(storage.Client().bucket(BUCKET_NAME), HW_PATH, df)
        print(f"保存: {HW_PATH}")
    return df
