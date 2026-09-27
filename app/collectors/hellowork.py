# ハローワークインターネットサービスから長野県の新着求人（詳細ページ）を取得してGCSに保存する
#
# 処理の流れ:
#   1. 検索ページを開き「長野県＋新着（直近3日）」で一般求人・障害者求人を検索し、詳細ページのリンク一覧を作る
#      （一覧はその日の最初の呼び出しで hellowork/_list/{YYYYMMDD}.parquet に保存し、同じ日の2回目以降は使い回す）
#   2. 当月・前月の保存済みParquetにある求人番号を除外し、残りの詳細ページを最大 max_details 件取得する
#   3. 対応表（FIELD_MAP）で日本語の列名に変換し、派生列を付けて hellowork/kyujin/{YYYYMM}.parquet に追記する
#
# アクセス間隔: 一覧 2秒 / 詳細 1.5秒、並列なし

import io
import json
import os
import re
import unicodedata
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup
from google.cloud import storage

BUCKET_NAME     = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
DATA_PREFIX     = "hellowork/kyujin"
LIST_PREFIX     = "hellowork/_list"
CITY_PATH       = "master/_M_city/data.parquet"
SANGYO_PATH     = "master/_M_sangyo/data.parquet"
BASE_URL        = "https://www.hellowork.mhlw.go.jp/kensaku/"
SEARCH_URL      = BASE_URL + "GECA110010.do"
PREF_CODE       = "20"
USER_AGENT      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
LIST_INTERVAL   = 2.0
DETAIL_INTERVAL = 1.5
MAX_PAGES       = 60
TIME_BUDGET     = 480
JST             = timezone(timedelta(hours=9))

# 検索の種類: (求人種別, 求人区分ラジオのラベル, 就業形態チェックボックスのid)
SEARCH_KINDS = [
    ("一般",   "一般求人",                 ["ID_ippanCKBox1", "ID_ippanCKBox2"]),
    ("障害者", "障害のある方のための求人", ["ID_sGSYACKBox1", "ID_sGSYACKBox2"]),
]
FULLPART = {"1": "フルタイム", "2": "パート"}

# 保存しない項目（担当者の個人情報、画面上の見出し・部品）
EXCLUDE_IDS     = {"ttsTts", "ttsTtsKana", "ttsEmail", "mapcontainer", "qr_modal_pc"}
EXCLUDE_PATTERN = re.compile(r"^GECZ")

# 番号付きで繰り返す項目は1列に「／」で連結する
MULTI_FIELDS = [
    ("支店等",     re.compile(r"^shtn\d+$")),
    ("主要取引先", re.compile(r"^stsk\d+$")),
    ("就業時間",   re.compile(r"^shgJn\d*$")),
]
NENSHO_PATTERN = re.compile(r"^nensho(\d+)(Nen)?$")

# 詳細ページの項目ID → 列名（並び順がそのまま列順になる）
FIELD_MAP = [
    ("uktkYmd", "受付年月日"), ("shkiKigenHi", "紹介期限日"), ("juriAtsh", "受理安定所"), ("kjKbn", "求人区分"),
    ("onlinJishuOboUktkKahi", "オンライン自主応募"), ("sngBrui", "産業分類"),
    ("*産業分類_大分類コード", "産業分類_大分類コード"), ("*産業分類_中分類コード", "産業分類_中分類コード"),
    ("*産業分類_小分類コード", "産業分類_小分類コード"), ("tryKoyoKibo", "トライアル雇用併用"),
    ("jgshNo", "事業所番号"), ("jgshMei", "事業所名"), ("jgshMeiKana", "事業所名カナ"), ("szciYbn", "所在地_郵便番号"),
    ("szci", "所在地"), ("hoNinNo", "法人番号"), ("yshk", "代表者役職"), ("dhshaMei", "代表者名"),
    ("setsuritsuNen", "設立年"), ("shkn", "資本金"), ("rodoKumiai", "労働組合"), ("jigyoNy", "事業内容"),
    ("kaishaNoTokucho", "会社の特長"), ("jgisKigyoZentai", "従業員数_企業全体"), ("jgisShgBs", "従業員数_就業場所"),
    ("jgisUchiJosei", "従業員数_うち女性"), ("jgisUchiPart", "従業員数_うちパート"),
    ("fltmShgKisoku", "就業規則_フルタイム"), ("partShgKisoku", "就業規則_パート"),
    ("ikujiKyugyoStkJisseki", "育児休業取得実績"), ("kaigoKyugyoStkJisseki", "介護休業取得実績"),
    ("kangoKyukaStkJisseki", "看護休暇取得実績"), ("gkjnKoyoJisseki", "外国人雇用実績"), ("shtnKssu", "支店等の数"),
    ("*支店等", "支店等"), ("*年商", "年商"), ("*主要取引先", "主要取引先"), ("jgshTkjk", "事業所の特記事項"),
    ("sksu", "職種"), ("shigotoNy", "仕事内容"), ("koyoKeitai", "雇用形態"),
    ("koyoKeitaiSsinIgaiNoMeisho", "雇用形態_正社員以外の名称"), ("koyoKeitaiSsinNoUmu", "正社員登用制度"),
    ("koyoKeitaiSsinJisseki", "正社員登用実績"), ("koyoKikan", "雇用期間"), ("koyoKikanSu", "雇用期間_期間"),
    ("koyoKikanYMD", "雇用期間_年月日"), ("koyoKikanKeiyakuKsnNoKnsi", "契約更新の可能性"),
    ("koyoKikanKeiyakuKsnNoJkn", "契約更新の条件"), ("hakenUkeoiToShgKeitai", "派遣・請負等"),
    ("shgBs", "就業場所_補足"), ("shgBsYubinNo", "就業場所_郵便番号"), ("shgBsJusho", "就業場所_住所"),
    ("*就業場所_市区町村", "就業場所_市区町村"), ("*就業場所_市区町村コード", "就業場所_市区町村コード"),
    ("shgBsMyorEki", "就業場所_最寄り駅"), ("shgBsKotsuShudan", "就業場所_交通手段"), ("shgBsShyoJn", "就業場所_所要時間"),
    ("shgBsKitsuTsak", "受動喫煙対策"), ("shgBsKitsuTsakTkjk", "受動喫煙対策_特記"), ("mycarTskn", "マイカー通勤"),
    ("mycarTsknChushaUmu", "駐車場の有無"), ("tenkinNoKnsi", "転勤の可能性"), ("tenkinNoKnsiTenkinHanni", "転勤範囲"),
    ("uIJTurn", "UIJターン歓迎"), ("nenrei", "年齢"), ("nenreiSegn", "年齢制限"), ("nenreiSegnHanni", "年齢制限_範囲"),
    ("nenreiSegnGaitoJiyu", "年齢制限_該当事由"), ("nenreiSegnNoRy", "年齢制限_理由"), ("grki", "学歴"),
    ("grkiIjo", "学歴_以上"), ("hynaKiknt", "必要な経験等"), ("hynaKikntShsi", "必要な経験等_詳細"),
    ("hynaMenkyoSkku", "必要な免許資格"), ("FutsuMenkyo", "普通自動車運転免許"), ("MenkyoSkkuMeisho", "免許資格_名称"),
    ("MenkyoSkkuSel", "免許資格_必須か"), ("MenkyoSkkuNyuryoku", "免許資格_その他"), ("hynaPc", "必要なPCスキル"),
    ("trialKikan", "試用期間"), ("trialKikanKikan", "試用期間_期間"), ("trialKikanChuuNoRodoJkn", "試用期間中の労働条件"),
    ("trialKikanChuuNoRodoJknNoNy", "試用期間中の労働条件_内容"), ("chgn", "賃金"), ("*賃金下限", "賃金下限"),
    ("*賃金上限", "賃金上限"), ("chgnKeitaiToKbn", "賃金形態"), ("chgnKeitaiTo", "賃金形態等"), ("khky", "基本給"),
    ("tgktNiShwrTat", "定額的に支払われる手当"), ("koteiZngyKbn", "固定残業代の有無"), ("koteiZngy", "固定残業代"),
    ("koteiZngyTkjk", "固定残業代_特記"), ("sntaTatFukiJk", "その他の手当等付記事項"), ("thkinRodoNissu", "月平均労働日数"),
    ("tsknTat", "通勤手当"), ("tsknTatTsuki", "通勤手当_単位"), ("tsknTatKingaku", "通勤手当_金額"),
    ("chgnSkbi", "賃金締切日"), ("chgnSkbiMitk", "賃金締切日_日"), ("chgnSrbi", "賃金支払日"),
    ("chgnSrbiTsuki", "賃金支払日_月"), ("chgnSrbiHi", "賃金支払日_日"), ("shokyuSd", "昇給制度"),
    ("shokyuMaeNendoJisseki", "昇給_前年度実績"), ("sokkgSkrt", "昇給_金額または率"), ("shoyoSdNoUmu", "賞与制度"),
    ("shoyoMaeNendoUmu", "賞与_前年度実績"), ("shoyoMaeNendKaisu", "賞与_回数"), ("shoyoKingaku", "賞与_金額"),
    ("*就業時間", "就業時間"), ("shgJnOr", "就業時間_又は"), ("henkeiRdTani", "変形労働時間制"),
    ("shgJiknTkjk", "就業時間_特記"), ("jkgiRodoJn", "時間外労働"), ("thkinJkgiRodoJn", "月平均時間外労働時間"),
    ("tkbsNaJijo", "時間外労働_特別な事情"), ("sanrokuKyotei", "36協定の特別条項"), ("kyukeiJn", "休憩時間"),
    ("shuRdNisu", "週所定労働日数"), ("shuRdNisuSodanKa", "労働日数の相談"), ("kyjs", "休日"),
    ("shukFtskSei", "週休二日制"), ("kyjsSnta", "休日等_その他"), ("nenkanKjsu", "年間休日数"),
    ("nenjiYukyu", "年次有給休暇"), ("knyHoken", "加入保険等"), ("kigyoNenkin", "企業年金"), ("tskinKsi", "退職金共済"),
    ("tskinSd", "退職金制度"), ("tskinSdKinzokuNensu", "退職金制度_勤続年数"), ("tnsei", "定年制"),
    ("tnseiTeinenNenrei", "定年年齢"), ("saiKoyoSd", "再雇用制度"), ("saiKoyoSdJgnNenrei", "再雇用_上限年齢"),
    ("kmec", "勤務延長"), ("nkj", "入居可能住宅"), ("riyoKanoTkjShst", "利用可能託児施設"), ("tkjShstTkjk", "託児施設_特記"),
    ("shokumuKyuSd", "職務給制度"), ("fukushokuSd", "復職制度"), ("fukushokuSdNoNy", "復職制度_内容"),
    ("fukuriKoseiNoNy", "福利厚生の内容"), ("knsSdNy", "研修制度"), ("knsSdNoSsinIgaiNoRiyo", "研修制度_正社員以外の利用"),
    ("rrtShienNy", "両立支援の内容"), ("saiyoNinsu", "採用人数"), ("boshuRy", "募集理由"), ("sntaNoBoshuRy", "募集理由_その他"),
    ("selectHoho", "選考方法"), ("selectKekkaTsuch", "選考結果通知"), ("shoruiSelectKekka", "書類選考結果通知"),
    ("mensetsuSelectKekka", "面接選考結果通知"), ("ksshEnoTsuchiHoho", "求職者への通知方法"),
    ("selectNichijiTo", "選考日時等"), ("selectBsYubinNo", "選考場所_郵便番号"), ("selectBsJusho", "選考場所_住所"),
    ("selectBsMyorEki", "選考場所_最寄り駅"), ("selectBsMyorEkiKotsuShudan", "選考場所_交通手段"),
    ("selectBsShyoJn", "選考場所_所要時間"), ("selectTkjk", "選考に関する特記事項"), ("oboShoruitou", "応募書類等"),
    ("oboShoruiNoSofuHoho", "応募書類の送付方法"), ("sntaNoSofuHoho", "応募書類_その他送付方法"),
    ("yusoNoSofuBsYubinNo", "郵送先_郵便番号"), ("yusoNoSofuBsJusho", "郵送先_住所"), ("obohen", "応募書類の返戻"),
    ("ttsYkm", "担当者_課係名"), ("ttsTel", "担当者_電話番号"), ("ttsFax", "担当者_FAX"),
    ("kjTkjk", "求人に関する特記事項"), ("jgshKaraNoMsg", "事業所からのメッセージ"),
    ("kigyoZiskKataJobUmu", "企業在籍型ジョブコーチ"), ("elevator", "エレベーター"), ("tenjiSetsubi", "点字設備"),
    ("kaidanTesuri", "階段の手すり"), ("tesuriSechi", "手すりの設置"), ("barrierFree", "バリアフリー対応トイレ"),
    ("tatemonoKrmIsuIdo", "建物内の車いす移動"), ("kyukeiShitsu", "休憩室"),
]
ID_TO_COL = {k: v for k, v in FIELD_MAP if not k.startswith("*")}

# 列順と型（キー・取得管理列を先頭、その他項目を末尾）
HEAD_COLS = ["求人番号", "求人種別", "就業形態", "取得日", "取得月"]
COLUMNS   = HEAD_COLS + [v for _, v in FIELD_MAP] + ["その他項目"]
DATE_COLS = {"取得日", "受付年月日", "紹介期限日"}
INT_COLS  = {"従業員数_企業全体", "従業員数_就業場所", "従業員数_うち女性", "従業員数_うちパート", "年間休日数", "賃金下限", "賃金上限"}
SCHEMA    = pa.schema([(c, pa.date32() if c in DATE_COLS else pa.int64() if c in INT_COLS else pa.string()) for c in COLUMNS])


# ---------- 共通 ----------

def _now() -> datetime:
    return datetime.now(JST)


def _digits(s) -> str:
    # 求人番号の比較用（「20010-14180061」と「2001014180061」を同一視する）
    return re.sub(r"\D", "", str(s or ""))


def _month_path(d: date) -> str:
    return f"{DATA_PREFIX}/{d:%Y%m}.parquet"


def _read_parquet(bucket, path: str) -> pd.DataFrame | None:
    blob = bucket.blob(path)
    if not blob.exists():
        return None
    return pd.read_parquet(io.BytesIO(blob.download_as_bytes()))


def _write_parquet(bucket, path: str, df: pd.DataFrame, schema: pa.Schema | None = None):
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    bucket.blob(path).upload_from_file(buf, content_type="application/octet-stream")


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # 列順と型をスキーマに揃える（整数は欠損ありの Int64、日付は datetime.date、それ以外は文字列）
    df = df.reindex(columns=COLUMNS)
    for c in COLUMNS:
        if c in INT_COLS:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        elif c in DATE_COLS:
            d = pd.to_datetime(df[c], errors="coerce")
            df[c] = pd.Series([v.date() if pd.notna(v) else None for v in d], index=df.index, dtype=object)
        else:
            df[c] = df[c].astype("string")
    return df


# ---------- 一覧の取得 ----------

def _form_state(form) -> list[tuple[str, str]]:
    # フォームの現在の入力状態を送信データにする（チェックボックス・ラジオは選択済みのものだけ）
    data = []
    for e in form.find_all("input"):
        name = e.get("name")
        kind = (e.get("type") or "text").lower()
        if not name or kind in ("submit", "button", "image"):
            continue
        if kind in ("checkbox", "radio") and not e.has_attr("checked"):
            continue
        data.append((name, e.get("value", "")))
    for s in form.find_all("select"):
        opt = s.find("option", selected=True) or s.find("option")
        if s.get("name") and opt is not None:
            data.append((s["name"], opt.get("value", "")))
    return data


def _as_form(pairs: list[tuple[str, str]]) -> dict[str, list[str]]:
    # httpx はタプルのリストを受け付けないため、同名項目を値のリストにまとめた辞書に変換する
    form: dict[str, list[str]] = {}
    for k, v in pairs:
        form.setdefault(k, []).append(v)
    return form


def _label_input(soup, text: str):
    # ラベルの文字列から対応する入力要素を探す
    lab = next((l for l in soup.find_all("label") if text in l.get_text()), None)
    return soup.find(id=lab.get("for")) if lab is not None else None


def _detail_links(res) -> list[dict]:
    out = []
    for a in res.find_all("a", href=True):
        if "dispDetailBtn" not in a["href"]:
            continue
        url = urljoin(SEARCH_URL, a["href"])
        q = parse_qs(urlparse(url).query)
        out.append({"kjno": _digits(q.get("kJNo", [""])[0]), "fullPart": q.get("fullPart", [""])[0],
                    "shogaiKbn": q.get("shogaiKbn", [""])[0], "url": url})
    return out


def _next_button(res):
    # 「次へ」ボタン（無効化されていれば None）
    btn = res.find(lambda t: t.name in ("input", "button") and "次へ" in (t.get("value") or t.get_text() or ""))
    if btn is None or btn.has_attr("disabled") or not btn.get("name"):
        return None
    return btn["name"], btn.get("value", "")


def _search_kind(client: httpx.Client, soup, kind: str, radio_label: str, box_ids: list[str]) -> list[dict]:
    # 1種類（一般／障害者）の検索を行い、全ページの詳細リンクを集める
    form   = soup.find("form", id="ID_form_1")
    radio  = _label_input(soup, radio_label)
    newbox = _label_input(soup, "新着求人")
    boxes  = [soup.find(id=i) for i in box_ids]
    if radio is None or newbox is None or any(b is None for b in boxes):
        raise RuntimeError(f"検索フォームの構造が変わった可能性があります（{kind}）")

    override = {"todohukenHidden", radio["name"], newbox["name"]} | {b["name"] for b in boxes}
    data = [t for t in _form_state(form) if t[0] not in override]
    data += [("todohukenHidden", PREF_CODE), (radio["name"], radio["value"]), (newbox["name"], newbox["value"])]
    data += [(b["name"], b["value"]) for b in boxes]
    data += [("searchBtn", soup.find(id="ID_searchBtn").get("value", ""))]

    time.sleep(LIST_INTERVAL)
    res = BeautifulSoup(client.post(SEARCH_URL, data=_as_form(data)).text, "html.parser")
    errs = [e.get_text(" ", strip=True) for e in res.find_all(class_=re.compile("err", re.I)) if e.get_text(strip=True)]
    if errs:
        raise RuntimeError(f"検索エラー（{kind}）: {errs[:2]}")
    total = int((res.find(id="ID_kyujinkensu") or {}).get("value") or 0)

    links, seen = [], set()
    for page in range(1, MAX_PAGES + 1):
        new = [l for l in _detail_links(res) if l["kjno"] and l["kjno"] not in seen]
        seen.update(l["kjno"] for l in new)
        links += [dict(l, kind=kind) for l in new]
        nxt = _next_button(res)
        if not new or nxt is None or len(links) >= total:
            break
        page_form = res.find("form", id="ID_form_1") or res.find("form")
        time.sleep(LIST_INTERVAL)
        res = BeautifulSoup(client.post(SEARCH_URL, data=_as_form(_form_state(page_form) + [nxt])).text, "html.parser")

    print(f"hellowork: {kind} 一覧 {len(links)}件 / 検索件数 {total}件 / {page}ページ")
    return links


def fetch_list(client: httpx.Client) -> pd.DataFrame:
    # 一般・障害者の両方を検索して、詳細リンクの一覧を返す
    soup = BeautifulSoup(client.get(SEARCH_URL, params={"action": "initDisp", "screenId": "GECA110010"}).text, "html.parser")
    links = []
    for kind, radio_label, box_ids in SEARCH_KINDS:
        links += _search_kind(client, soup, kind, radio_label, box_ids)
    return pd.DataFrame(links, columns=["kind", "kjno", "fullPart", "shogaiKbn", "url"]).drop_duplicates("kjno")


# ---------- 詳細ページの解析 ----------

def parse_detail(html: str) -> dict:
    # 'ID_' で始まる要素の文字列を {項目ID: 値} で返す
    soup = BeautifulSoup(html, "html.parser")
    raw = {}
    for e in soup.find_all(id=re.compile(r"^ID_")):
        if e.name in ("input", "form", "button", "select", "script", "a"):
            continue
        text = e.get_text(" ", strip=True)
        if text:
            raw.setdefault(e["id"][3:], text)
    return raw


def _first_int(s):
    m = re.search(r"\d[\d,]*", str(s or ""))
    return int(m.group(0).replace(",", "")) if m else None


def _wage_range(s):
    nums = [int(n.replace(",", "")) for n in re.findall(r"\d[\d,]*", str(s or ""))]
    return (nums[0], nums[1] if len(nums) > 1 else nums[0]) if nums else (None, None)


def _jp_date(s):
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", str(s or ""))
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


def to_record(raw: dict, link: dict, today: date) -> dict:
    # 項目ID→列名に変換し、連結項目・派生列を作る。対応表にない項目は「その他項目」へ
    rec, used = {}, set()
    for k, v in raw.items():
        if k in EXCLUDE_IDS or EXCLUDE_PATTERN.match(k):
            used.add(k)
        elif k in ID_TO_COL:
            rec[ID_TO_COL[k]] = v
            used.add(k)

    for col, pat in MULTI_FIELDS:
        keys = sorted([k for k in raw if pat.match(k)], key=lambda k: _first_int(k) or 0)
        if keys:
            rec[col] = "／".join(raw[k] for k in keys)
            used.update(keys)

    nen = sorted({int(m.group(1)) for k in raw for m in [NENSHO_PATTERN.match(k)] if m})
    if nen:
        rec["年商"] = "／".join(f"{raw.get(f'nensho{i}Nen', '')}:{raw.get(f'nensho{i}', '')}" for i in nen)
        used.update(k for k in raw if NENSHO_PATTERN.match(k))

    rest = {k: v for k, v in raw.items() if k not in used and k != "kjNo"}
    rec["その他項目"] = json.dumps(rest, ensure_ascii=False) if rest else None

    rec["求人番号"] = raw.get("kjNo")
    rec["求人種別"] = link["kind"]
    rec["就業形態"] = FULLPART.get(str(link["fullPart"]))
    rec["取得日"]   = today
    rec["取得月"]   = f"{today:%Y%m}"
    rec["賃金下限"], rec["賃金上限"] = _wage_range(rec.get("賃金"))
    for c in ("受付年月日", "紹介期限日"):
        rec[c] = _jp_date(rec.get(c))
    for c in ("従業員数_企業全体", "従業員数_就業場所", "従業員数_うち女性", "従業員数_うちパート", "年間休日数"):
        rec[c] = _first_int(rec.get(c))
    return rec


# ---------- 市区町村コードの付与 ----------

def load_city_master(bucket) -> list[tuple[str, str, str]]:
    # 長野県の市区町村を (照合用の名前, 市区町村名, 5桁コード) で、名前の長い順に返す
    df = _read_parquet(bucket, CITY_PATH)
    if df is None:
        return []
    df = df[df["pref_code"].astype(str).str.zfill(2) == PREF_CODE]
    rows = [(re.sub(r"^.+?郡", "", str(n)), str(n), str(c).zfill(5)) for n, c in zip(df["name"], df["code_5_digit"])]
    return sorted(rows, key=lambda r: -len(r[0]))


def match_city(address, cities) -> tuple[str | None, str | None]:
    # 住所の先頭（都道府県名・郡名を除いた部分）が一致する市区町村を探す
    s = re.sub(r"\s", "", str(address or ""))
    s = re.sub(r"^長野県", "", s)
    s = re.sub(r"^[^市町村]+?郡", "", s)
    for key, name, code in cities:
        if key and s.startswith(key):
            return name, code
    return None, None


# ---------- 産業分類マスタ（_M_sangyo）とコードの付与 ----------
# 元データ: ハローワークの産業分類コード一覧（日本標準産業分類 第14回改定・令和5年 準拠）
SANGYO_LIST_URL = "https://www.hellowork.mhlw.go.jp/info/industry_list{:02d}.html"

# 大分類（総務省の正式表記）と、所属する中分類コードの範囲
DAI_RANGES = [
    ("A", "農業，林業", 1, 2), ("B", "漁業", 3, 4), ("C", "鉱業，採石業，砂利採取業", 5, 5), ("D", "建設業", 6, 8),
    ("E", "製造業", 9, 32), ("F", "電気・ガス・熱供給・水道業", 33, 36), ("G", "情報通信業", 37, 41),
    ("H", "運輸業，郵便業", 42, 49), ("I", "卸売業，小売業", 50, 61), ("J", "金融業，保険業", 62, 67),
    ("K", "不動産業，物品賃貸業", 68, 70), ("L", "学術研究，専門・技術サービス業", 71, 74),
    ("M", "宿泊業，飲食サービス業", 75, 77), ("N", "生活関連サービス業，娯楽業", 78, 80), ("O", "教育，学習支援業", 81, 82),
    ("P", "医療，福祉", 83, 85), ("Q", "複合サービス事業", 86, 87), ("R", "サービス業（他に分類されないもの）", 88, 96),
    ("S", "公務（他に分類されるものを除く）", 97, 98), ("T", "分類不能の産業", 99, 99),
]
SANGYO_COLUMNS = ["code", "name", "chu_code", "chu_name", "dai_code", "dai_name", "is_kanri"]


def _sangyo_key(s) -> str:
    # 照合用に表記を揃える（全角→半角、空白と「，」「、」を除去）
    return re.sub(r"[\s、,]", "", unicodedata.normalize("NFKC", str(s or "")))


def _code_rows(html: str, width: int) -> list[tuple[str, str]]:
    # ページ内の全ての表から「コード, 名称」の行を抜き出す（見出し行は除く）
    rows = []
    for tr in BeautifulSoup(html, "html.parser").find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
        code = unicodedata.normalize("NFKC", cells[0]).strip() if len(cells) >= 2 else ""
        if re.fullmatch(r"\d{1,%d}" % width, code) and cells[1]:
            rows.append((code.zfill(width), cells[1]))
    return rows


def build_sangyo_master(client: httpx.Client | None = None) -> pd.DataFrame:
    # ハローワークの中分類・小分類ページから _M_sangyo を組み立て、件数と整合性を検証して返す
    own = client is None
    client = client or httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60, follow_redirects=True)
    try:
        pages = {lv: client.get(SANGYO_LIST_URL.format(lv)).content.decode("utf-8") for lv in (2, 3)}
    finally:
        if own:
            client.close()
    chu = dict(_code_rows(pages[2], 2))
    sho = pd.DataFrame(_code_rows(pages[3], 3), columns=["code", "name"]).drop_duplicates("code")
    sho["chu_code"] = sho["code"].str[:2]
    sho["chu_name"] = sho["chu_code"].map(chu)
    dai = {f"{n:02d}": (c, name) for c, name, lo, hi in DAI_RANGES for n in range(lo, hi + 1)}
    sho["dai_code"] = sho["chu_code"].map(lambda c: dai.get(c, (None, None))[0])
    sho["dai_name"] = sho["chu_code"].map(lambda c: dai.get(c, (None, None))[1])
    sho["is_kanri"] = sho["name"].str.startswith("管理，補助的経済活動を行う事業所")

    problems = []
    if len(chu) != 99:
        problems.append(f"中分類が {len(chu)} 件（想定 99）")
    if sho["chu_name"].isna().any() or sho["dai_code"].isna().any():
        problems.append(f"中分類・大分類に対応しない小分類: {sho.loc[sho['chu_name'].isna() | sho['dai_code'].isna(), 'code'].tolist()[:10]}")
    if sho["dai_code"].nunique() != 20 or len(sho) < 500:
        problems.append(f"大分類 {sho['dai_code'].nunique()} 件 / 小分類 {len(sho)} 件（想定 20 / 500以上）")
    if problems:
        raise RuntimeError("産業分類コード表の形式が想定と違います: " + " / ".join(problems))
    print(f"_M_sangyo: 大分類 {sho['dai_code'].nunique()} / 中分類 {sho['chu_code'].nunique()} / 小分類 {len(sho)} 件")
    return sho[SANGYO_COLUMNS].sort_values("code").reset_index(drop=True)


def save_sangyo_master(dry_run: bool = True) -> pd.DataFrame:
    # _M_sangyo を作成して GCS に保存する（dry_run=True なら保存しない）
    df = build_sangyo_master()
    if not dry_run:
        _write_parquet(storage.Client().bucket(BUCKET_NAME), SANGYO_PATH, df)
        print(f"✅ gs://{BUCKET_NAME}/{SANGYO_PATH} に保存（{len(df)}件）")
    return df


def load_sangyo_master(bucket) -> pd.DataFrame | None:
    return _read_parquet(bucket, SANGYO_PATH)


def match_sangyo(name, master: pd.DataFrame | None) -> tuple[str | None, str | None, str | None]:
    # 産業分類名 → (大分類, 中分類, 小分類コード)。完全一致を優先し、なければ前方一致が1件のときだけ採用
    # （ハローワークの求人票は長い産業分類名を30字前後で切って表示するため）
    if master is None or not name:
        return None, None, None
    key  = _sangyo_key(name)
    keys = master["name"].map(_sangyo_key)
    hit  = master[keys == key]
    if hit.empty:
        hit = master[keys.str.startswith(key)] if len(key) >= 8 else hit
    if len(hit) != 1:
        return None, None, None
    r = hit.iloc[0]
    return r["dai_code"], r["chu_code"], r["code"]


def add_sangyo_codes(df: pd.DataFrame, master: pd.DataFrame | None) -> pd.DataFrame:
    # データフレームの「産業分類」から3つのコード列を付け直す
    codes = [match_sangyo(n, master) for n in df["産業分類"]]
    df = df.copy()
    df["産業分類_大分類コード"] = [c[0] for c in codes]
    df["産業分類_中分類コード"] = [c[1] for c in codes]
    df["産業分類_小分類コード"] = [c[2] for c in codes]
    return df


def backfill_sangyo(yyyymm: str, dry_run: bool = True) -> pd.DataFrame:
    # 保存済みの月ファイルに産業分類コードを付け直す（dry_run=True なら保存しない）
    bucket = storage.Client().bucket(BUCKET_NAME)
    master = load_sangyo_master(bucket)
    if master is None:
        raise RuntimeError("_M_sangyo がありません。先に save_sangyo_master(dry_run=False) を実行してください")
    path = f"{DATA_PREFIX}/{yyyymm}.parquet"
    df = _read_parquet(bucket, path)
    if df is None:
        raise RuntimeError(f"{path} がありません")
    df = normalize(add_sangyo_codes(normalize(df), master))
    print(f"{path}: {len(df)}件 / 小分類コード付与 {df['産業分類_小分類コード'].notna().sum()}件 / 未付与の産業分類: {df.loc[df['産業分類_小分類コード'].isna(), '産業分類'].dropna().unique().tolist()[:10]}")
    if not dry_run:
        _write_parquet(bucket, path, df, schema=SCHEMA)
        print(f"✅ gs://{BUCKET_NAME}/{path} に保存")
    return df


# ---------- メイン ----------

def collect_hellowork(max_details: int = 200, dry_run: bool = False):
    # dry_run=True: GCSは読むだけで書き込まず、取得したデータフレームを返す（Colab確認用）
    # dry_run=False: 当月ファイルに追記し、今回追加した件数を返す
    started = time.monotonic()
    today   = _now().date()
    bucket  = storage.Client().bucket(BUCKET_NAME)
    client  = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=60, follow_redirects=True)
    try:
        # 1. 一覧（その日の2回目以降は保存済みのものを使う）
        list_path = f"{LIST_PREFIX}/{today:%Y%m%d}.parquet"
        links = _read_parquet(bucket, list_path)
        if links is None:
            links = fetch_list(client)
            if not dry_run:
                _write_parquet(bucket, list_path, links)

        # 2. 当月・前月に保存済みの求人番号を除外
        this_path = _month_path(today)
        prev_path = _month_path(today.replace(day=1) - timedelta(days=1))
        current   = _read_parquet(bucket, this_path)
        previous  = _read_parquet(bucket, prev_path)
        saved = {_digits(x) for df in (current, previous) if df is not None for x in df["求人番号"]}
        todo  = links[~links["kjno"].isin(saved)]
        print(f"hellowork: 一覧 {len(links)}件 / 保存済み {len(links) - len(todo)}件 / 未取得 {len(todo)}件")

        # 3. 詳細ページを取得（件数上限・時間上限・連続失敗で打ち切り）
        cities  = load_city_master(bucket)
        sangyo  = load_sangyo_master(bucket)
        records, fails = [], 0
        for link in todo.head(max_details).to_dict("records"):
            if time.monotonic() - started > TIME_BUDGET or fails >= 5:
                print("hellowork: 時間上限または連続失敗のため打ち切り")
                break
            time.sleep(DETAIL_INTERVAL)
            try:
                r = client.get(link["url"])
                r.raise_for_status()
                raw = parse_detail(r.text)
                if not raw.get("kjNo"):
                    raise ValueError("求人番号が見つかりません（掲載終了の可能性）")
                rec = to_record(raw, link, today)
                rec["就業場所_市区町村"], rec["就業場所_市区町村コード"] = match_city(rec.get("就業場所_住所"), cities)
                rec["産業分類_大分類コード"], rec["産業分類_中分類コード"], rec["産業分類_小分類コード"] = match_sangyo(rec.get("産業分類"), sangyo)
                records.append(rec)
                fails = 0
            except Exception as e:
                fails += 1
                print(f"hellowork: 詳細取得失敗 {link['kjno']}: {e}")

        new_df = normalize(pd.DataFrame(records))
        print(f"hellowork: 詳細 {len(new_df)}件取得 / 残り {max(len(todo) - len(new_df), 0)}件 / {time.monotonic() - started:.0f}秒")
        if dry_run:
            return new_df
        if new_df.empty:
            return 0

        # 4. 当月ファイルに追記して保存
        merged = new_df if current is None else pd.concat([normalize(current), new_df], ignore_index=True)
        merged = merged.drop_duplicates("求人番号", keep="first")
        _write_parquet(bucket, this_path, merged, schema=SCHEMA)
        print(f"hellowork: ✅ gs://{BUCKET_NAME}/{this_path} に保存（{len(merged)}件）")
        return len(new_df)
    finally:
        client.close()
