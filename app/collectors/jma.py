# 気象庁 非公式JSON API から長野県の天気予報を取得してGCSに保存する
#
# データソース構造:
#   raw[0]: 3日予報（今日・明日・明後日）
#     timeSeries[0]: 天気コード・天気テキスト・風 ← ゾーン別（北部/中部/南部）
#     timeSeries[1]: 降水確率（6時間単位）       ← ゾーン別
#     timeSeries[2]: 気温                        ← 地点別（長野/松本/諏訪/飯田/軽井沢）
#   raw[1]: 週間予報（明日〜7日後）
#     timeSeries[0]: 天気コード・降水確率・信頼度 ← 長野県全体（全地点共通）
#     timeSeries[1]: 最高・最低気温              ← 長野のみ

import io
import os
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH    = "jma/nagano/data.parquet"
JMA_URL     = "https://www.jma.go.jp/bosai/forecast/data/forecast/200000.json"
JST         = timezone(timedelta(hours=9))

# 対象5地点の設定
LOCATIONS = {
    "長野":   {"wind_zone": "北部"},
    "松本":   {"wind_zone": "中部"},
    "諏訪":   {"wind_zone": "中部"},
    "飯田":   {"wind_zone": "南部"},
    "軽井沢": {"wind_zone": "北部"},
}

# 風ゾーンのAPIエリアコード
WIND_ZONE_CODE = {"北部": "200010", "中部": "200020", "南部": "200030"}
ZONE_CODE_NAME = {v: k for k, v in WIND_ZONE_CODE.items()}

# 天気コード → テキスト変換（週間予報はweathersフィールドがないため使用）
WEATHER_CODE_MAP = {
    "100": "晴れ",           "101": "晴れ時々曇り",   "102": "晴れ一時雨",
    "103": "晴れ時々雨",     "104": "晴れ一時雪",     "105": "晴れ時々雪",
    "110": "晴れのち曇り",   "111": "晴れのち曇り",   "112": "晴れのち一時雨",
    "113": "晴れのち時々雨", "114": "晴れのち雨",     "115": "晴れのち一時雪",
    "116": "晴れのち時々雪", "117": "晴れのち雪",     "118": "晴れのち雨か雪",
    "119": "晴れのち雷雨",   "120": "晴れ朝夕一時雨", "122": "晴れ夕方一時雨",
    "126": "晴れ昼頃から雨", "127": "晴れ夕方から雨", "128": "晴れ夜は雨",
    "130": "朝霧のち晴れ",   "140": "晴れ時々雷雨",   "160": "晴れ一時雪か雨",
    "170": "晴れ時々雪か雨", "181": "晴れのち雪か雨",
    "200": "曇り",           "201": "曇り時々晴れ",   "202": "曇り一時雨",
    "203": "曇り時々雨",     "204": "曇り一時雪",     "205": "曇り時々雪",
    "206": "曇り一時雨か雪", "207": "曇り時々雨か雪", "208": "曇り一時雷雨",
    "209": "霧",             "210": "曇りのち晴れ",   "211": "曇りのち晴れ",
    "212": "曇りのち一時雨", "213": "曇りのち時々雨", "214": "曇りのち雨",
    "215": "曇りのち一時雪", "216": "曇りのち時々雪", "217": "曇りのち雪",
    "218": "曇りのち雨か雪", "219": "曇りのち雷雨",   "220": "曇り朝夕一時雨",
    "222": "曇り夕方一時雨", "224": "曇り昼頃から雨", "225": "曇り夕方から雨",
    "226": "曇り夜は雨",     "228": "曇り昼頃から雪", "240": "曇り時々雷雨",
    "281": "曇りのち雪か雨",
    "300": "雨",             "301": "雨時々晴れ",     "302": "雨時々止む",
    "303": "雨時々雪",       "304": "雨か雪",         "306": "大雨",
    "308": "暴風雨",         "309": "雨一時雪",       "311": "雨のち晴れ",
    "313": "雨のち曇り",     "314": "雨のち時々雪",   "315": "雨のち雪",
    "316": "雨か雪のち晴れ", "317": "雨か雪のち曇り", "320": "朝の内雨のち晴れ",
    "321": "朝の内雨のち曇り", "322": "雨朝晩一時雪", "328": "雨一時強く降る",
    "340": "雪か雨",         "350": "雨で雷を伴う",
    "400": "雪",             "401": "雪時々晴れ",     "402": "雪時々止む",
    "403": "雪時々雨",       "405": "大雪",           "406": "風雪強い",
    "407": "暴風雪",         "409": "雪一時雨",       "411": "雪のち晴れ",
    "413": "雪のち曇り",     "414": "雪のち雨",       "420": "朝の内雪のち晴れ",
    "421": "朝の内雪のち曇り", "425": "雪一時強く降る", "450": "雪で雷を伴う",
}

# 天気コード → 絵文字（LINE通知用）
WEATHER_EMOJI = {
    "100": "☀️",  "101": "🌤",  "102": "🌦",  "103": "🌦",
    "110": "⛅",  "111": "⛅",  "112": "🌦",  "114": "🌧",
    "200": "☁️",  "201": "⛅",  "202": "🌦",  "203": "🌦",
    "210": "🌤",  "211": "🌤",  "212": "🌦",  "214": "🌧",
    "300": "🌧",  "301": "🌦",  "302": "🌧",  "306": "⛈",
    "308": "⛈",  "350": "⛈",
    "400": "❄️",  "401": "🌨",  "402": "🌨",  "406": "🌨",  "407": "🌨",
}
EMOJI_FALLBACK = {"1": "☀️", "2": "☁️", "3": "🌧", "4": "❄️"}


def _to_float(val: str):
    """空文字・Noneを受け取りfloatまたはNoneを返す"""
    return float(val) if val and val.strip() else None


def _to_int(val: str):
    """空文字・Noneを受け取りintまたはNoneを返す"""
    return int(val) if val and val.strip() else None


def _clean_wind(val: str):
    """全角スペースを半角に変換し空文字はNoneにする"""
    if not val:
        return None
    cleaned = val.replace("\u3000", " ").strip()
    return cleaned if cleaned else None


def _fetch() -> list:
    """気象庁APIから長野県のデータを取得する"""
    with httpx.Client(timeout=30) as client:
        res = client.get(JMA_URL)
    res.raise_for_status()
    return res.json()


def _parse(raw: list) -> pd.DataFrame:
    """3日予報と週間予報を組み合わせて5地点×7日分のDataFrameを作成する"""
    now_jst      = datetime.now(JST)
    today_str    = now_jst.date().isoformat()
    published_at = raw[1]["reportDatetime"]
    retrieved_at = now_jst.isoformat()

    # ── 3日予報を整理する ──────────────────────────────────────────
    s0       = raw[0]["timeSeries"][0]   # 天気コード・テキスト・風（ゾーン別）
    s1       = raw[0]["timeSeries"][1]   # 降水確率（6時間単位、ゾーン別）
    s2       = raw[0]["timeSeries"][2]   # 気温（地点別）
    s0_dates = [d[:10] for d in s0["timeDefines"]]  # ['今日', '明日', '明後日']

    # ゾーン別に天気・風を辞書化する
    short_zone = {area["area"]["name"]: area for area in s0["areas"]}

    # ゾーン別の降水確率（今日・明日）を集計する
    # 6時間スロット: [今日12-18h, 今日18-24h, 明日00-06h, 明日06-12h, 明日12-18h, 明日18-24h]
    short_pop = {}
    for area in s1["areas"]:
        zname   = ZONE_CODE_NAME.get(area["area"]["code"], "")
        pops    = area["pops"]
        today_p = max([int(p) for p in pops[:2] if p.strip()], default=None)
        tmrw_p  = max([int(p) for p in pops[2:6] if p.strip()], default=None)
        short_pop[zname] = {"today": today_p, "tomorrow": tmrw_p}

    # 地点別気温を整理する（4時刻: 今日high, 今日low, 明日low, 明日high）
    short_temps = {}
    for area in s2["areas"]:
        t = area.get("temps", [])
        short_temps[area["area"]["name"]] = {
            "today_max": _to_float(t[1]) if len(t) > 1 else None,  # t[1]=今日の最高
            "today_min": _to_float(t[0]) if len(t) > 0 else None,  # t[0]=今日の最低
            "tmrw_min":  _to_float(t[2]) if len(t) > 2 else None,  # t[2]=明日の最低
            "tmrw_max":  _to_float(t[3]) if len(t) > 3 else None,  # t[3]=明日の最高
        }

    # ── 週間予報を整理する ──────────────────────────────────────────
    w0      = raw[1]["timeSeries"][0]   # 天気コード・降水確率・信頼度（長野県全体）
    w1      = raw[1]["timeSeries"][1]   # 最高・最低気温（長野のみ）
    w_dates = [d[:10] for d in w0["timeDefines"]]  # 7日分（明日〜）
    w_area  = w0["areas"][0]
    w_codes = w_area["weatherCodes"]
    w_pops  = w_area["pops"]
    w_rels  = w_area["reliabilities"]

    nagano_w = w1["areas"][0]
    w_tmax   = nagano_w["tempsMax"]
    w_tmin   = nagano_w["tempsMin"]

    # ── レコードを組み立てる ────────────────────────────────────────
    records  = []
    tmrw_str = s0_dates[1] if len(s0_dates) > 1 else ""
    daft_str = s0_dates[2] if len(s0_dates) > 2 else ""

    def make_record(date_str, loc, code, text, tmax, tmin, pop, rel, wind):
        return {
            "target_date":  date_str,
            "location":     loc,
            "weather_code": code,
            "weather":      text,
            "temp_max":     tmax,
            "temp_min":     tmin,
            "precip_prob":  pop,
            "reliability":  rel,
            "wind":         wind,
            "published_at": published_at,
            "retrieved_at": retrieved_at,
        }

    # 今日（3日予報 index=0 のみ）
    for loc, info in LOCATIONS.items():
        zname = info["wind_zone"]
        zarea = short_zone.get(zname, {})
        code  = zarea.get("weatherCodes", [""])[0]
        text  = zarea.get("weathers",     [""])[0] or WEATHER_CODE_MAP.get(code, "")
        wind  = _clean_wind(zarea.get("winds", [None])[0])
        pop   = short_pop.get(zname, {}).get("today")
        st    = short_temps.get(loc, {})
        records.append(make_record(
            today_str, loc, code, text,
            st.get("today_max"), st.get("today_min"),
            pop, None, wind
        ))

    # 明日〜今日+6（週間予報 index=0〜5）
    for i, date_str in enumerate(w_dates[:6]):
        code     = w_codes[i]
        text     = WEATHER_CODE_MAP.get(code, code)
        w_pop    = _to_int(w_pops[i])
        rel      = w_rels[i].strip() or None if w_rels[i] else None
        w_nagano_max = _to_float(w_tmax[i])
        w_nagano_min = _to_float(w_tmin[i])

        for loc, info in LOCATIONS.items():
            zname = info["wind_zone"]
            zarea = short_zone.get(zname, {})

            # 風: 3日予報の範囲内なら取得、それ以外はNull
            wind = None
            if date_str in s0_dates:
                si    = s0_dates.index(date_str)
                winds = zarea.get("winds", [])
                wind  = _clean_wind(winds[si] if si < len(winds) else None)

            # 降水確率: 明日はゾーン別を優先、それ以外は県全体
            if date_str == tmrw_str:
                pop = short_pop.get(zname, {}).get("tomorrow") or w_pop
            else:
                pop = w_pop

            # 気温: 明日は地点別（3日予報）を優先
            #        明後日以降は長野のみ週間予報、他地点はNull
            if date_str == tmrw_str:
                st   = short_temps.get(loc, {})
                tmax = st.get("tmrw_max") or (w_nagano_max if loc == "長野" else None)
                tmin = st.get("tmrw_min") or (w_nagano_min if loc == "長野" else None)
            else:
                tmax = w_nagano_max if loc == "長野" else None
                tmin = w_nagano_min if loc == "長野" else None

            records.append(make_record(date_str, loc, code, text, tmax, tmin, pop, rel, wind))

    df = pd.DataFrame(records)
    # NaN→Noneに変換する（JSONシリアライズ対策）
    # pandasはNullをfloat('nan')で表現するが、JSONにnanは存在しないためNoneに統一する
    return df.where(pd.notna(df), None)


def _upsert_to_gcs(new_df: pd.DataFrame) -> None:
    """(target_date, location)をキーにupsertしてGCSに保存する（蓄積型）"""
    gcs    = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    blob   = bucket.blob(GCS_PATH)

    if blob.exists():
        # 既存データを読み込む
        buf = io.BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        existing = pd.read_parquet(buf)

        # 新データと重複するキーを既存から除外する
        new_keys = set(zip(new_df["target_date"].astype(str), new_df["location"]))
        existing["_key"] = existing["target_date"].astype(str) + "_" + existing["location"]
        kept     = existing[~existing["_key"].isin(
            {f"{d}_{l}" for d, l in new_keys}
        )].drop("_key", axis=1)
        result   = pd.concat([kept, new_df], ignore_index=True)
    else:
        result = new_df

    result = result.sort_values(["target_date", "location"]).reset_index(drop=True)

    # GCSに書き戻す
    table = pa.Table.from_pandas(result, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")


def _format_line_message(df: pd.DataFrame) -> str:
    """長野地点の7日分予報をLINE通知用テキストにフォーマットする"""
    nagano  = df[df["location"] == "長野"].sort_values("target_date")
    pub     = df["published_at"].iloc[0]
    pub_str = datetime.fromisoformat(pub).strftime("%Y/%m/%d（%a）%H:%M")
    wd_ja   = ["月", "火", "水", "木", "金", "土", "日"]

    lines = ["☀️ 長野の1週間天気予報", f"{pub_str}発表", ""]

    for _, row in nagano.iterrows():
        td      = pd.to_datetime(row["target_date"])
        wd      = wd_ja[td.weekday()]
        date_s  = td.strftime(f"%m/%d({wd})")
        code    = str(row["weather_code"])
        emoji   = WEATHER_EMOJI.get(code, EMOJI_FALLBACK.get(code[:1], "🌀"))
        weather = row["weather"] or ""

        # 気温（一桁の場合にスペースで右揃えする）
        tmax = row["temp_max"]
        tmin = row["temp_min"]
        if pd.notna(tmax) and pd.notna(tmin):
            temp_s = f"{int(tmax):>2}/{int(tmin):>2}℃"
        elif pd.notna(tmax):
            temp_s = f"{int(tmax):>2}/--℃"
        else:
            temp_s = "--/--℃"

        # 降水確率
        pop   = row["precip_prob"]
        pop_s = f"💧{int(pop):>3}%" if pd.notna(pop) else "💧 --%"

        # 確度（今日・明日はデータなしのため省略）
        rel   = row["reliability"]
        rel_s = f"  確度{rel}" if pd.notna(rel) and rel else ""

        lines.append(f"{date_s} {emoji} {weather}  {temp_s}  {pop_s}{rel_s}")

    return "\\n".join(lines)


def _send_line(message: str) -> None:
    """LINE Messaging APIにメッセージを同期送信する。失敗時は例外を投げる"""
    token   = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    user_id = os.environ.get("LINE_USER_ID", "")
    if not token or not user_id:
        raise RuntimeError(f"LINE認証情報が未設定: token={bool(token)}, user_id={bool(user_id)}")
    with httpx.Client(timeout=30) as client:
        res = client.post(
            "https://api.line.me/v2/bot/message/push",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"to": user_id, "messages": [{"type": "text", "text": message}]},
        )
    if res.status_code != 200:
        raise RuntimeError(f"LINE送信失敗: status={res.status_code} body={res.text[:200]}")
    print("✅ LINE通知を送信しました")


def collect_jma_nagano() -> int:
    """気象庁APIから取得・GCS保存・LINE通知を一括実行するメイン関数"""
    raw    = _fetch()
    df     = _parse(raw)
    _upsert_to_gcs(df)
    msg    = _format_line_message(df)
    _send_line(msg)
    print(f"✅ jma/nagano 収集完了: {len(df)}件")
    return len(df)
