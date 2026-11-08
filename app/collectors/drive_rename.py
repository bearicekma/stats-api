# Google Drive PDF 自動リネーム処理
#  - 個人Drive(マイドライブ)の対象フォルダ内PDFを走査
#  - 数日以内アップロード & 未処理（処理済みの目印なし、かつ YYYYMMDD_ 形式でない）のものを抽出
#  - PDFを直接Geminiに渡し、1回の呼び出しで「発行日」「見出し」「全文テキスト」をJSONで取得
#  - 「YYYYMMDD_【シリーズ名（回）】見出し」にリネーム（シリーズ・定番コラムでなければ【】なし）。日付は 紙面の発行日 → ファイル名の8桁 → Drive保存日(JST) の順で採用
#  - リネームしたファイルには appProperties renamed=1 を付けて処理済みとする
#  - 画像PDFなら、ページごとに自動判定して圧縮（文字主体=グレースケール／写真入り=元画像を再圧縮）し、
#    全文を不可視テキスト層として埋め込んで差し替え（縮まなければ元画像のまま文字のみ、失敗時はリネームのみ）
#  - 認証は OAuth 2.0 ユーザー委任(refresh_token)、値は環境変数から取得
#  - Geminiは候補モデルを順に試すフォールバック方式（廃止/日次上限は即次へ）
#  - 候補モデルは環境変数 GEMINI_MODELS（カンマ区切り）で上書き可能
#  - お気に入り状態はGCSにJSONで永続化（drive_rename/favorites.json）

import os
import re
import io
import time
import json
from datetime import datetime, timezone, timedelta

# ---- 設定値 ----
TARGET_FOLDER_ID  = "141cGbdt8MalPRPP15tHjlED7DlZd24z9"  # 対象フォルダ
RECENT_DAYS       = 3       # アップロードからこの日数以内のみ対象
MAX_HEADLINES     = 3       # 連結する見出しの最大本数
MAX_NAME_LEN      = 100     # ファイル名(日付含む)のおおよその上限
MAX_FILES_PER_RUN = 20      # 1回のrunで処理する最大件数（無料枠20/日の安全弁）
RUN_TIME_BUDGET   = 600     # 秒。これを超えたら新しいファイルに着手しない（Cloud Runの900秒タイムアウト対策）
OPT_MIN_TEXT_CHARS = 50     # 埋め込み用に必要な最低文字数（これ未満なら圧縮しない）

# Geminiの候補モデル既定値（先頭から試す）。環境変数 GEMINI_MODELS で上書き可
_DEFAULT_GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]


def _load_gemini_models():
    # 環境変数 GEMINI_MODELS（例: "gemini-2.5-flash,gemini-2.5-flash-lite"）があればそれを使う
    raw = os.environ.get("GEMINI_MODELS", "")
    models = [m.strip() for m in raw.split(",") if m.strip()]
    return models or list(_DEFAULT_GEMINI_MODELS)


GEMINI_MODELS     = _load_gemini_models()
MAX_RPM_RETRY     = 2       # 分次(RPM)一時超過時のリトライ回数
RPM_RETRY_WAIT    = 8       # リトライ前の待機秒数
MAX_UNAVAIL_RETRY = 2       # 503(一時的な過負荷)時のリトライ回数
UNAVAIL_WAIT      = 5       # 503リトライ前の待機秒数

# お気に入り情報のGCS保存先
GCS_BUCKET_NAME    = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
FAVORITES_GCS_PATH = "drive_rename/favorites.json"

# 処理済み判定：リネーム後の名前（YYYYMMDD_〜）
_DONE_PATTERN = re.compile(r"^\d{8}_")
# ファイル名先頭の8桁日付（予備の日付として使う）
_NAME_DATE_PATTERN = re.compile(r"^(\d{8})")
# 採用する日付の下限（これより古い日付は読み間違いとみなす）
MIN_VALID_DATE = "20000101"
JST = timezone(timedelta(hours=9))


def _get_drive_service():
    # 環境変数のOAuth資格情報からDriveクライアントを構築して返す
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    creds = Credentials(
        token=None,  # アクセストークンは無し→refresh_tokenから自動取得
        refresh_token=os.environ["DRIVE_OAUTH_REFRESH_TOKEN"],
        client_id=os.environ["DRIVE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["DRIVE_OAUTH_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_target_pdfs(service):
    # フォルダ内のPDFを全件取得
    q = (
        f"'{TARGET_FOLDER_ID}' in parents "
        f"and mimeType = 'application/pdf' and trashed = false"
    )
    files = []
    page_token = None
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, createdTime, size, appProperties)",
            pageSize=100,
            pageToken=page_token,
            orderBy="createdTime desc",
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def _is_processed(f):
    # 処理済みか：目印(appProperties)がある、または既に YYYYMMDD_〜 形式の名前
    props = f.get("appProperties") or {}
    if props.get("renamed") == "1" or props.get("optimized") == "1":
        return True
    base = re.sub(r"\.pdf$", "", f["name"], flags=re.IGNORECASE)
    return _DONE_PATTERN.match(base) is not None


def _valid_date(s):
    # "YYYYMMDD" が実在する日付で、2000年以降かつ明日(JST)までなら そのまま返す。不正なら None
    if not s or not re.fullmatch(r"\d{8}", s):
        return None
    try:
        d = datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None
    tomorrow = (datetime.now(JST) + timedelta(days=1)).date()
    if s < MIN_VALID_DATE or d > tomorrow:
        return None
    return s


def _normalize_date(raw):
    # Geminiが返した日付文字列（例 "2026-11-01" "2026年11月1日"）を "YYYYMMDD" に。読めなければ None
    m = re.search(r"(\d{4})\D{1,3}(\d{1,2})\D{1,3}(\d{1,2})", str(raw or ""))
    if not m:
        m = re.search(r"(\d{4})(\d{2})(\d{2})", str(raw or ""))
    if not m:
        return None
    y, mo, d = m.groups()
    return _valid_date(f"{int(y):04d}{int(mo):02d}{int(d):02d}")


def _decide_date(paper_date, name, created_time):
    # 採用する日付と出どころを返す：紙面の発行日 → ファイル名の8桁 → Drive保存日(JST)
    if paper_date:
        return paper_date, "紙面"
    base = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
    m = _NAME_DATE_PATTERN.match(base)
    if m and _valid_date(m.group(1)):
        return m.group(1), "ファイル名"
    dt = datetime.fromisoformat(created_time.replace("Z", "+00:00")).astimezone(JST)
    return dt.strftime("%Y%m%d"), "保存日"


def _within_recent(created_time):
    # createdTime(RFC3339, UTC)が RECENT_DAYS 以内か
    dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
    threshold = datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)
    return dt >= threshold


def _download_pdf_bytes(service, file_id):
    # PDF本体をバイト列でダウンロード
    from googleapiclient.http import MediaIoBaseDownload
    buf = io.BytesIO()
    req = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()


def _clean_headline(s):
    # 見出し1本の前後の空白・中黒を除去
    return str(s).strip(" 　・\t\r\n")


def _clean_series(s):
    # シリーズ名・回の前後の空白や括弧、【】を除去（【】はファイル名側で付けるため）
    s = re.sub(r"[【】\[\]]", "", str(s or ""))
    return s.strip(" 　・\t\r\n")


def _compose_title(heads, series, part):
    # ファイル名の日付より後ろを組み立てる：「【シリーズ名（回）】見出し1・見出し2」
    # 見出しに同じシリーズ名（と回）が入っていたら取り除き、空になった見出しは捨てる
    part = part.strip("()（）") if part else ""
    if not series:
        return "・".join(heads)

    def _loose(s):
        # 空白の有無を問わず一致する正規表現にする
        return r"\s*".join(map(re.escape, re.sub(r"\s", "", s)))

    # シリーズ名の全体と、前の語を省いた形（例「迫真 さまよう夫婦」→「さまよう夫婦」）の両方を消す
    tokens = series.split()
    names = [" ".join(tokens[i:]) for i in range(len(tokens))] or [series]
    part_pat = (r"(?:\s*[（(]?\s*" + _loose(part) + r"\s*[)）]?)?") if part else ""
    stripped = []
    for h in heads:
        for nm in names:
            h = re.sub(_loose(nm) + part_pat, "", h)
        h = h.strip(" 　・、，,：:\t")
        if len(h) >= 2:
            stripped.append(h)
    label = f"【{series}（{part}）】" if part else f"【{series}】"
    return label + "・".join(stripped)   # 見出しが全部シリーズ名だった場合は【】だけになる


def _parse_analysis(raw):
    # Geminiの応答(JSON文字列)から (見出しリスト, 全文テキスト, 発行日YYYYMMDD or None, シリーズ名, 回) を取り出す。
    # 出力が途中で切れてJSONが壊れていた場合は、日付・シリーズ・見出しだけ正規表現で救出する（全文は空）。
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            heads = [_clean_headline(h) for h in (data.get("headlines") or [])]
            heads = [h for h in heads if h][:MAX_HEADLINES]
            text = str(data.get("text") or "")
            return (heads, text, _normalize_date(data.get("date")),
                    _clean_series(data.get("series")), _clean_series(data.get("series_part")))
    except Exception:
        pass

    def _field(key):
        # 壊れたJSONから "key": "値" を取り出す
        fm = re.search(r'"' + key + r'"\s*:\s*"((?:[^"\\]|\\.)*)"', raw)
        if not fm:
            return ""
        try:
            return json.loads('"' + fm.group(1) + '"')
        except Exception:
            return fm.group(1)

    paper_date = _normalize_date(_field("date"))
    series, part = _clean_series(_field("series")), _clean_series(_field("series_part"))
    heads = []
    m = re.search(r'"headlines"\s*:\s*\[(.*?)\]', raw, re.S)
    if m:
        for item in re.findall(r'"((?:[^"\\]|\\.)*)"', m.group(1)):
            try:
                item = json.loads('"' + item + '"')  # エスケープ(\" や \uXXXX)を戻す
            except Exception:
                pass
            item = _clean_headline(item)
            if item:
                heads.append(item)
    return heads[:MAX_HEADLINES], "", paper_date, series, part


def _analyze_pdf(pdf_bytes):
    # PDFを直接Geminiに渡し、1回の呼び出しで発行日・見出し・全文テキストを取得する。
    # 戻り値: ("【シリーズ名（回）】見出し1・見出し2・見出し3", 全文テキスト(取れなければ空文字), 発行日YYYYMMDD(取れなければNone))
    #         シリーズ・定番コラムでなければ【】は付かない
    # 候補モデルを順に試す。廃止(limit:0)・日次上限(PerDay)は即次へ、
    # 分次(PerMinute)は短い待機後にリトライ、503(過負荷)も短い待機後にリトライ。
    from google import genai
    from google.genai import types

    # 接続先は v1beta に固定（google-genai 3.0 から既定が v1 になり、v1 は JSON モード非対応で 400 になるため）
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"],
                          http_options=types.HttpOptions(api_version="v1beta"))

    prompt = (
        "これは日本経済新聞の紙面、または電子版記事のPDFです。次のJSONだけを出力してください。\n"
        '{"date": "YYYY-MM-DD", "series": "", "series_part": "", "headlines": ["見出し1", "見出し2"], "text": "紙面の全文"}\n'
        "条件:\n"
        "- date: 紙面上部の日付欄、または記事に記載された発行日（掲載日）を西暦で。"
        "和暦表記なら西暦に直す。PDFの印刷日時・ダウンロード日時は使わない。読み取れなければ空文字\n"
        "- series: 主要な記事（headlinesの1本目）が、連載・シリーズ・定番コラムの一部である場合、その名称。"
        "ロゴやカット、見出し脇の小さな表記に示されることが多い"
        "（例: データで読む地域再生、迫真 さまよう夫婦、きょうのことば、経済教室、大機小機、春秋、私見卓見）。"
        "該当しなければ空文字\n"
        "- series_part: その連載の回の表記（例: 上、中、下、①、3、第3回）。括弧は付けない。無ければ空文字\n"
        f"- headlines: 主要な記事の見出しを重要な順に最大{MAX_HEADLINES}本（記事が1本だけならその見出し1本）。"
        "各20文字程度まで、記号・改行・番号は付けない。シリーズ名・回の表記は含めない\n"
        "- text: 紙面に掲載された記事の見出しと本文を、読み順にできるだけ漏れなく文字起こししたプレーンテキスト"
        "（広告は不要）\n"
        "- JSON以外の文字は一切出力しない"
    )
    parts = [
        types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        prompt,
    ]
    config = types.GenerateContentConfig(response_mime_type="application/json")

    last_error = None

    for model in GEMINI_MODELS:
        rpm_attempt = 0
        unavail_attempt = 0
        while True:
            try:
                resp = client.models.generate_content(model=model, contents=parts, config=config)
                raw = (resp.text or "").strip()
                if not raw:
                    last_error = f"{model}: 空応答"
                    break  # 次の候補へ
                heads, text, paper_date, series, part = _parse_analysis(raw)
                if not heads:
                    last_error = f"{model}: 見出し抽出できず"
                    break  # 次の候補へ
                return _compose_title(heads, series, part), text, paper_date

            except Exception as e:
                msg = str(e)
                last_error = f"{model}: {msg}"

                is_429 = ("429" in msg) or ("RESOURCE_EXHAUSTED" in msg)
                is_503 = ("503" in msg) or ("UNAVAILABLE" in msg)

                # 503（一時的な過負荷）→ 短い待機で数回だけリトライ、だめなら次の候補へ
                if is_503 and not is_429:
                    if unavail_attempt < MAX_UNAVAIL_RETRY:
                        unavail_attempt += 1
                        time.sleep(UNAVAIL_WAIT)
                        continue
                    break

                # 429/503以外（404・認証など）→ このモデルでは無理、即次の候補へ
                if not is_429:
                    break

                # limit: 0（無料枠対象外/廃止）→ 即次の候補へ
                if "limit: 0" in msg or "limit:0" in msg:
                    break

                # 日次上限（PerDay）超過 → 今日は待っても無駄、即次の候補へ
                if "PerDay" in msg:
                    break

                # 分次（PerMinute）など一時的な超過 → 回数内なら待ってリトライ
                if rpm_attempt < MAX_RPM_RETRY:
                    rpm_attempt += 1
                    time.sleep(RPM_RETRY_WAIT)
                    continue
                else:
                    break  # リトライ尽きた → 次の候補へ

    raise RuntimeError(last_error or "見出し生成失敗（全候補）")


def _try_optimize(pdf_bytes, text):
    # 画像PDFなら「ページごとに自動判定して圧縮（文字主体=グレースケール／写真入り=元画像を再圧縮）＋全文の不可視テキスト層」を作る。
    # 戻り値: (新しいPDFバイト列, 方式の説明)=成功 / (None, 理由)=対象外または失敗（その場合はリネームのみ）
    if len((text or "").strip()) < OPT_MIN_TEXT_CHARS:
        return None, "全文テキスト未取得（圧縮せず）"
    try:
        from app.pdf_optimize import is_scanned_pdf, compress_with_text, verify_text_layer

        if not is_scanned_pdf(pdf_bytes):
            return None, "既にテキスト層あり（圧縮せず）"
        new_bytes, mode = compress_with_text(pdf_bytes, text)
        if not verify_text_layer(new_bytes):
            return None, "検証NG（テキスト層を抽出できず）"
        return new_bytes, f"圧縮: {mode}"
    except Exception as e:
        return None, f"圧縮失敗: {e}"


def _sanitize(title):
    # ファイル名に使えない文字を除去
    title = re.sub(r'[\\/:*?"<>|]', "", title)
    title = re.sub(r"[\r\n\t]", "", title)
    return title.strip()


def _unique_name(service, base):
    # 同名が既にあれば (2)(3)... を付けて衝突回避
    candidate = base
    n = 2
    while True:
        safe = candidate.replace("'", "\\'")
        q = (
            f"'{TARGET_FOLDER_ID}' in parents and trashed = false "
            f"and name = '{safe}'"
        )
        resp = service.files().list(q=q, fields="files(id)", pageSize=1).execute()
        if not resp.get("files"):
            return candidate
        candidate = f"{base}({n})"
        n += 1


def _update_file(service, file_id, new_name, new_bytes):
    # リネームし（処理済みの目印 renamed=1 を付与）、new_bytes があれば中身も差し替える（optimized=1 も付与）。
    # 差し替えに失敗したらリネームのみで再試行する。戻り値: 差し替え成功なら None、失敗理由があれば文字列
    rename_body = {"name": new_name, "appProperties": {"renamed": "1"}}
    if new_bytes:
        try:
            from googleapiclient.http import MediaIoBaseUpload
            media = MediaIoBaseUpload(io.BytesIO(new_bytes), mimetype="application/pdf", resumable=True)
            service.files().update(
                fileId=file_id,
                body={"name": new_name, "appProperties": {"renamed": "1", "optimized": "1"}},
                media_body=media,
            ).execute()
            return None
        except Exception as e:
            fail_reason = f"差し替え失敗（リネームのみ）: {e}"
            service.files().update(fileId=file_id, body=rename_body).execute()
            return fail_reason
    service.files().update(fileId=file_id, body=rename_body).execute()
    return None


def _get_gcs_bucket():
    # お気に入りJSON保存用のGCSバケットを返す
    from google.cloud import storage
    client = storage.Client()
    return client.bucket(GCS_BUCKET_NAME)


def _load_favorites() -> set:
    # GCSからお気に入りID一覧を読み込む（未作成なら空集合）
    bucket = _get_gcs_bucket()
    blob = bucket.blob(FAVORITES_GCS_PATH)
    if not blob.exists():
        return set()
    try:
        data = json.loads(blob.download_as_text())
        return set(data.get("favorite_ids", []))
    except Exception as e:
        print(f"⚠️ favorites.json 読み込み失敗: {e}")
        return set()


def _save_favorites(favorite_ids: set):
    # お気に入りID一覧をGCSに保存する
    bucket = _get_gcs_bucket()
    blob = bucket.blob(FAVORITES_GCS_PATH)
    blob.upload_from_string(
        json.dumps({"favorite_ids": sorted(favorite_ids)}, ensure_ascii=False),
        content_type="application/json",
    )


def toggle_favorite(file_id: str) -> bool:
    # 指定ファイルのお気に入りをトグルし、トグル後の状態(True=お気に入り)を返す
    favorites = _load_favorites()
    if file_id in favorites:
        favorites.discard(file_id)
        new_state = False
    else:
        favorites.add(file_id)
        new_state = True
    _save_favorites(favorites)
    return new_state


def run_drive_rename():
    # メイン処理：対象PDFを走査してリネーム（＋画像PDFは圧縮・全文埋め込み）。結果をdictで返す
    started = time.monotonic()
    service = _get_drive_service()
    files = _list_target_pdfs(service)

    renamed, skipped, failed = [], 0, []
    processed = 0  # 実際にAPIを消費した件数（成功・失敗とも）

    for f in files:
        name = f["name"]

        # 条件1: アップロードが数日以内
        if not _within_recent(f["createdTime"]):
            skipped += 1
            continue

        # 条件2: 未処理か（目印なし、かつ YYYYMMDD_〜 形式でない）
        if _is_processed(f):
            skipped += 1
            continue

        # 件数上限、または時間予算を超えたら、残りは未処理のまま次回へ
        if processed >= MAX_FILES_PER_RUN or (time.monotonic() - started) > RUN_TIME_BUDGET:
            skipped += 1
            continue

        try:
            pdf_bytes = _download_pdf_bytes(service, f["id"])
            headline, text, paper_date = _analyze_pdf(pdf_bytes)
            if not headline:
                failed.append({"name": name, "reason": "見出し生成失敗"})
                processed += 1
                continue

            # 日付：紙面の発行日 → ファイル名の8桁 → Drive保存日(JST)
            date_part, date_source = _decide_date(paper_date, name, f["createdTime"])
            new_base = f"{date_part}_{_sanitize(headline)}"[:MAX_NAME_LEN]
            new_name = _unique_name(service, new_base)

            # 圧縮＋全文埋め込み（失敗してもリネームは続行）
            new_bytes, note = _try_optimize(pdf_bytes, text)
            upload_note = _update_file(service, f["id"], new_name, new_bytes)
            if upload_note:
                note, new_bytes = upload_note, None

            item = {"from": name, "to": new_name, "date_source": date_source, "optimized": bool(new_bytes)}
            if new_bytes:
                item["size_before"] = len(pdf_bytes)
                item["size_after"] = len(new_bytes)
            if note:
                item["note"] = note
            renamed.append(item)
            processed += 1
        except Exception as e:
            failed.append({"name": name, "reason": str(e)})
            processed += 1  # 失敗もAPI消費とみなしカウント

    return {"renamed": renamed, "skipped": skipped, "failed": failed}


def list_drive_pdfs():
    # 一覧表示用：フォルダ内PDFをJSON向けに整形して返す（お気に入り状態を含む）
    service = _get_drive_service()
    files = _list_target_pdfs(service)
    favorites = _load_favorites()

    out = []
    for f in files:
        name = f["name"]
        out.append({
            "id":           f["id"],
            "name":         name,
            "created_time": f["createdTime"],
            "size":         int(f["size"]) if f.get("size") else None,
            "renamed":      _is_processed(f),
            "favorite":     f["id"] in favorites,
        })
    return out
