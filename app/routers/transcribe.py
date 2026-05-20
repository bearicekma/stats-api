# 音声・動画・YouTube文字起こしエンドポイント（Vertex AI Gemini 2.5 Flash）
#  GET  /transcribe                      : WebUI
#  POST /transcribe                      : 音声/動画ファイル or URL → 文字起こし
#                                          オプション: glossary（用語集）、with_timestamps（30秒毎タイムスタンプ）
#  POST /transcribe/summarize            : 要約（モデル・テンプレート切替可能）
#  GET  /transcribe/history              : 過去の文字起こし一覧（GCS保存分）
#  GET  /transcribe/history/{filename}   : 個別取得（再要約用）

import os
import re
import json
import uuid
import hashlib
import tempfile
import subprocess
from pathlib  import Path
from datetime import datetime, timezone, timedelta

from fastapi           import APIRouter, UploadFile, File, Form, Body
from fastapi.responses import JSONResponse, HTMLResponse
from google.cloud      import storage

router = APIRouter(tags=["Transcribe"])

# ---- 設定値 ----
GCP_PROJECT       = "stats-api-491107"
GCP_LOCATION      = "asia-northeast1"
GEMINI_MODEL      = "gemini-2.5-flash"
BUCKET_NAME       = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_RESULT_PREFIX = "transcribe"
GCS_TEMP_PREFIX   = "transcribe/_temp"
MAX_UPLOAD_BYTES  = 30 * 1024 * 1024
INLINE_LIMIT      = 18 * 1024 * 1024
HISTORY_SCAN_MAX  = 500    # 履歴一覧で走査する最大blob数（大量ファイル時の安全装置）

AUDIO_EXTS = {".mp3", ".m4a", ".wav", ".ogg", ".flac", ".opus", ".aac", ".wma"}
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".avi", ".mkv", ".m4v"}
MIME_MAP = {
    ".mp3":  "audio/mp3",  ".m4a":  "audio/mp4",  ".wav":  "audio/wav",
    ".ogg":  "audio/ogg",  ".flac": "audio/flac", ".opus": "audio/ogg",
    ".aac":  "audio/aac",
}

# 要約モデルの対応表
SUMMARY_MODELS = {
    "sonnet": "claude-sonnet-4-5",
    "haiku":  "claude-haiku-4-5",
    "gemini": GEMINI_MODEL,
}

# 用途別の要約テンプレート（仕事で使い分けたい想定）
SUMMARY_TEMPLATES = {
    "default": """以下のテキストを構造化して日本語で要約してください。

【出力形式】
1. **概要**（3〜5文）: 全体の要点
2. **主な論点・話題**: 箇条書き5〜10項目
3. **決定事項・アクション**: ある場合のみ箇条書き
4. **キーワード**: 5〜10語

原文の意味を変えずに端的にまとめてください。""",

    "meeting": """以下のテキストを議事録形式で日本語にまとめてください。

【出力形式】
## 議題
- 議論された議題を箇条書き

## 議論の要点
- 各論点と主な意見・対立点

## 決定事項
- 確定した結論を箇条書き（無ければ「なし」）

## アクション項目（宿題）
- 「誰が・何を・いつまでに」の形式で（不明な要素は「未定」と記載）

## 次回確認事項
- 次回に持ち越された事項""",

    "todo": """以下のテキストからアクション項目（やるべきこと）のみを抽出してください。

【出力形式】
- [ ] 担当者(分かる場合) | 内容 | 期限(分かる場合)

ToDoに該当しない雑談・前置き・状況説明は一切含めない。
該当項目がなければ「アクション項目なし」とだけ返してください。""",

    "email": """以下のテキストを基に、関係者向けの報告メール下書きを日本語で作成してください。

【出力形式】
件名: [簡潔な件名]

お疲れ様です。

[本文：要点を整理した報告。3〜5段落程度、ビジネス敬体]

以上、ご確認のほどよろしくお願いいたします。

【注意】
- 重要な数字・固有名詞・期限は省略しない
- 口語表現は書き言葉に変換""",

    "brief": """以下のテキストを日本語で3行に要約してください。
各行20〜40文字程度。最も重要な要点のみ抽出。前置きや補足は不要。""",
}

TRANSCRIBE_HTML = Path(__file__).parent.parent / "templates" / "transcribe.html"
JST = timezone(timedelta(hours=9))

# クライアントは遅延生成（import失敗・認証エラーでアプリ全体が落ちないように）
_genai_client     = None
_storage_client   = None
_anthropic_client = None


def _get_genai_client():
    # Vertex AI 上の Gemini クライアント（プロジェクト・リージョン固定）
    global _genai_client
    if _genai_client is None:
        from google import genai
        _genai_client = genai.Client(vertexai=True, project=GCP_PROJECT, location=GCP_LOCATION)
    return _genai_client


def _get_storage_client():
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        import anthropic
        _anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _anthropic_client


# =============================================================
# 入力処理ヘルパー
# =============================================================

def _extract_audio_from_video(video_bytes: bytes, src_ext: str) -> bytes:
    # 動画から ffmpeg で音声を抽出（16kHz mono Opus 32kbps）
    with tempfile.NamedTemporaryFile(suffix=src_ext, delete=False) as src:
        src.write(video_bytes)
        src_path = src.name
    out_path = src_path + ".opus"
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", src_path,
             "-vn", "-acodec", "libopus", "-b:a", "32k",
             "-ar", "16000", "-ac", "1",
             out_path],
            check=True, capture_output=True, timeout=300,
        )
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        for p in (src_path, out_path):
            if os.path.exists(p):
                os.remove(p)


def _extract_youtube_video_id(url: str) -> str | None:
    # YouTube URLから動画IDを抽出
    patterns = [
        r"youtu\.be/([\w-]+)",
        r"youtube\.com/watch\?v=([\w-]+)",
        r"youtube\.com/shorts/([\w-]+)",
        r"youtube\.com/embed/([\w-]+)",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def _try_youtube_transcript(video_id: str) -> str | None:
    # YouTube字幕APIで字幕取得。なければNone
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        try:
            entries = YouTubeTranscriptApi.get_transcript(video_id, languages=["ja", "en"])
        except Exception:
            return None
        return "\n".join(e["text"] for e in entries if e.get("text"))
    except Exception as e:
        print(f"字幕取得エラー: {e}")
        return None


def _download_audio_from_url(url: str) -> tuple[bytes, dict]:
    # yt-dlpで動画から音声のみダウンロード（16kHz mono Opus 32kbps）
    import yt_dlp
    with tempfile.TemporaryDirectory() as tmpdir:
        out_template = os.path.join(tmpdir, "audio.%(ext)s")
        opts = {
            "format":      "bestaudio/best",
            "outtmpl":     out_template,
            "quiet":       True,
            "no_warnings": True,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec":   "opus",
                "preferredquality": "32",
            }],
            "postprocessor_args": ["-ar", "16000", "-ac", "1"],
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            files = list(Path(tmpdir).glob("audio.*"))
            if not files:
                raise RuntimeError("ダウンロードファイルが見つかりません")
            with open(files[0], "rb") as f:
                audio = f.read()
        return audio, {
            "title":        info.get("title"),
            "uploader":     info.get("uploader"),
            "duration_sec": info.get("duration"),
            "webpage_url":  info.get("webpage_url"),
        }


# =============================================================
# Gemini呼出ヘルパー
# =============================================================

def _build_transcribe_prompt(glossary: str | None, with_timestamps: bool) -> str:
    # 用語集・タイムスタンプ指定に応じてプロンプトを動的に組み立てる
    parts = []

    if glossary and glossary.strip():
        parts.append(
            "【用語集（以下の語句は正確にこの表記で文字起こしすること）】\n"
            + glossary.strip()
            + "\n"
        )

    parts.append("以下の音声を日本語で正確に文字起こししてください。\n")
    parts.append("【整形ルール】")
    parts.append("- 適切な句読点・改行を付与し、読みやすい文章にしてください")
    parts.append("- 軽微なフィラー（「あー」「えー」「えーと」等）は除去してください")
    parts.append("- 話者の交代が明確な場合は「話者A:」「話者B:」のように区別してください")
    if with_timestamps:
        parts.append("- 約30秒ごとに `[MM:SS]` 形式でタイムスタンプを挿入してください（1時間超は `[HH:MM:SS]`）")
    parts.append("")
    parts.append("【厳守】")
    parts.append("- 発言内容は原文に忠実に。要約や意訳は禁止")
    parts.append("- 不明瞭な箇所は[聞き取り不能]と記載")
    parts.append("- 文字起こし結果以外の前置きやコメントは一切出力しない")

    return "\n".join(parts)


def _upload_temp_to_gcs(audio_bytes: bytes, mime_type: str) -> str:
    # Gemini送信用にGCSへ一時アップロード（インライン上限18MB超過時用）
    ext = "opus" if ("opus" in mime_type or "ogg" in mime_type) else mime_type.split("/")[-1]
    blob_name = f"{GCS_TEMP_PREFIX}/{uuid.uuid4().hex}.{ext}"
    bucket = _get_storage_client().bucket(BUCKET_NAME)
    bucket.blob(blob_name).upload_from_string(audio_bytes, content_type=mime_type)
    return f"gs://{BUCKET_NAME}/{blob_name}"


def _delete_gcs_uri(gcs_uri: str):
    # 一時アップロードの削除（処理後クリーンアップ用）
    try:
        if not gcs_uri.startswith("gs://"):
            return
        path = gcs_uri[5:]
        bucket_name, _, blob_name = path.partition("/")
        _get_storage_client().bucket(bucket_name).blob(blob_name).delete()
    except Exception as e:
        print(f"GCS一時ファイル削除失敗: {e}")


def _transcribe_with_gemini(audio_bytes: bytes, mime_type: str, prompt: str) -> str:
    # Gemini 2.5 Flash で音声→整形済テキスト（プロンプトは外から注入）
    from google.genai import types
    client = _get_genai_client()

    temp_gcs_uri = None
    try:
        if len(audio_bytes) <= INLINE_LIMIT:
            audio_part = types.Part.from_bytes(data=audio_bytes, mime_type=mime_type)
        else:
            temp_gcs_uri = _upload_temp_to_gcs(audio_bytes, mime_type)
            audio_part = types.Part.from_uri(file_uri=temp_gcs_uri, mime_type=mime_type)

        resp = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[audio_part, prompt],
        )
        return (resp.text or "").strip()
    finally:
        if temp_gcs_uri:
            _delete_gcs_uri(temp_gcs_uri)


def _summarize_with_claude(prompt: str, model: str) -> str:
    # Claude（Sonnet or Haiku）で要約生成
    client = _get_anthropic_client()
    msg = client.messages.create(
        model=model,
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


def _summarize_with_gemini(prompt: str) -> str:
    # Gemini で要約生成（プロバイダ統一したい時用）
    client = _get_genai_client()
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[prompt],
    )
    return (resp.text or "").strip()


# =============================================================
# 履歴保存
# =============================================================

def _save_transcript_to_gcs(payload: dict) -> str:
    # 文字起こし結果JSONをGCSに保存
    now = datetime.now(JST)
    text_hash = hashlib.md5((payload.get("text") or "").encode("utf-8")).hexdigest()[:8]
    blob_name = f"{GCS_RESULT_PREFIX}/{now.strftime('%Y%m%d_%H%M%S')}_{text_hash}.json"
    bucket = _get_storage_client().bucket(BUCKET_NAME)
    bucket.blob(blob_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json; charset=utf-8",
    )
    return f"gs://{BUCKET_NAME}/{blob_name}"


# =============================================================
# エンドポイント
# =============================================================

@router.get("/transcribe", response_class=HTMLResponse, summary="文字起こしWebUI")
def transcribe_page():
    return HTMLResponse(TRANSCRIBE_HTML.read_text(encoding="utf-8"))


@router.post("/transcribe", summary="音声・動画・URLの文字起こし実行")
async def transcribe_run(
    file:            UploadFile = File(None),
    url:             str        = Form(None),
    glossary:        str        = Form(None),     # 用語集（人名・社名・専門用語）
    with_timestamps: bool       = Form(False),    # 30秒ごとのタイムスタンプ挿入
):
    """
    音声/動画ファイル または 動画URL から文字起こしを生成します。

    処理ロジック:
    - 音声ファイル: そのままGemini送信
    - 動画ファイル: ffmpegで音声抽出後Gemini送信
    - YouTube URL: 字幕APIを優先取得（無料）。
      ただし glossary または with_timestamps 指定時は字幕APIをスキップしてGeminiで処理。
    - その他URL: yt-dlpで音声DL後Gemini送信
    - 結果は全てGCS保存（後から再要約・検索可能）

    入力（multipart/form-data・ファイルとURLはどちらか一方）:
    - file:            音声/動画ファイル（上限30MB）
    - url:             動画URL
    - glossary:        正確に表記したい人名・社名・専門用語等（任意）
    - with_timestamps: trueで30秒毎にタイムスタンプを挿入（既定false）

    レスポンス: source, source_info, text, transcription_method, model, options, saved_to, created_at
    """
    if not file and not url:
        return JSONResponse(status_code=400, content={"error": "ファイル または URL のどちらかを指定してください"})
    if file and url:
        return JSONResponse(status_code=400, content={"error": "ファイルとURLは同時に指定できません"})

    prompt = _build_transcribe_prompt(glossary, with_timestamps)

    try:
        # ===== Case 1: ファイルアップロード =====
        if file:
            name = (file.filename or "").lower()
            ext  = "." + name.rsplit(".", 1)[-1] if "." in name else ""
            if ext not in AUDIO_EXTS and ext not in VIDEO_EXTS:
                return JSONResponse(status_code=400, content={
                    "error": f"非対応の形式です: {ext or '不明'}",
                    "hint":  f"対応: 音声 {sorted(AUDIO_EXTS)} / 動画 {sorted(VIDEO_EXTS)}",
                })

            data = await file.read()
            if len(data) > MAX_UPLOAD_BYTES:
                return JSONResponse(status_code=400, content={
                    "error": f"ファイルが大きすぎます（{len(data)//1024//1024}MB）",
                    "hint":  f"上限は{MAX_UPLOAD_BYTES//1024//1024}MBです",
                })

            if ext in VIDEO_EXTS:
                audio_bytes = _extract_audio_from_video(data, ext)
                mime_type   = "audio/ogg"
                source_type = "video_file"
            else:
                audio_bytes = data
                mime_type   = MIME_MAP.get(ext, "audio/mp3")
                source_type = "audio_file"

            source_info = {
                "filename":       file.filename,
                "original_size":  len(data),
                "processed_size": len(audio_bytes),
            }

        # ===== Case 2: URL =====
        else:
            video_id = _extract_youtube_video_id(url)
            # glossary/with_timestamps 指定時は字幕APIをスキップしてGeminiへ
            use_captions = video_id and not (glossary and glossary.strip()) and not with_timestamps

            if use_captions:
                caption = _try_youtube_transcript(video_id)
                if caption:
                    result = {
                        "source":               "youtube_caption",
                        "source_info":          {"url": url, "video_id": video_id},
                        "text":                 caption,
                        "transcription_method": "youtube_caption",
                        "model":                None,
                        "options":              {"glossary": None, "with_timestamps": False},
                        "created_at":           datetime.now(JST).isoformat(),
                    }
                    result["saved_to"] = _save_transcript_to_gcs(result)
                    return result

            source_type = "youtube_audio" if video_id else "other_url"
            audio_bytes, dl_info = _download_audio_from_url(url)
            source_info = {"url": url, **dl_info}
            mime_type   = "audio/ogg"

        # ===== Gemini文字起こし =====
        text = _transcribe_with_gemini(audio_bytes, mime_type, prompt)

        result = {
            "source":               source_type,
            "source_info":          source_info,
            "text":                 text,
            "transcription_method": "gemini",
            "model":                GEMINI_MODEL,
            "options":              {
                "glossary":        glossary or None,
                "with_timestamps": with_timestamps,
            },
            "created_at":           datetime.now(JST).isoformat(),
        }
        result["saved_to"] = _save_transcript_to_gcs(result)
        return result

    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error":  "文字起こし処理に失敗しました",
            "detail": str(e),
        })


@router.post("/transcribe/summarize", summary="文字起こしテキストの要約")
def transcribe_summarize(payload: dict = Body(...)):
    """
    文字起こしテキストから日本語の要約を生成します（再要約何度でも可能）。

    リクエストボディ:
    - text     (string, 必須) 要約対象テキスト
    - model    (string, 任意) sonnet | haiku | gemini（既定: sonnet）
    - template (string, 任意) default | meeting | todo | email | brief（既定: default）

    テンプレート:
    - default : 概要・論点・決定事項・キーワードの構造化要約
    - meeting : 議事録形式（議題・要点・決定事項・宿題・次回確認）
    - todo    : アクション項目のみ抽出
    - email   : 関係者向け報告メール下書き
    - brief   : 3行サマリー
    """
    text         = payload.get("text", "")
    model_key    = payload.get("model", "sonnet").lower()
    template_key = payload.get("template", "default").lower()

    if not text:
        return JSONResponse(status_code=400, content={"error": "text が空です"})
    if model_key not in SUMMARY_MODELS:
        return JSONResponse(status_code=400, content={
            "error": f"不正なmodel指定: {model_key}",
            "hint":  f"有効な値: {list(SUMMARY_MODELS.keys())}",
        })
    if template_key not in SUMMARY_TEMPLATES:
        return JSONResponse(status_code=400, content={
            "error": f"不正なtemplate指定: {template_key}",
            "hint":  f"有効な値: {list(SUMMARY_TEMPLATES.keys())}",
        })

    prompt = SUMMARY_TEMPLATES[template_key] + "\n\n【元テキスト】\n" + text

    try:
        if model_key == "gemini":
            summary = _summarize_with_gemini(prompt)
        else:
            summary = _summarize_with_claude(prompt, SUMMARY_MODELS[model_key])
        return {
            "summary":       summary,
            "model_used":    SUMMARY_MODELS[model_key],
            "template_used": template_key,
            "created_at":    datetime.now(JST).isoformat(),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error":  "要約処理に失敗しました",
            "detail": str(e),
        })


@router.get("/transcribe/history", summary="文字起こし履歴一覧")
def transcribe_history(limit: int = 20):
    """
    GCSに保存された過去の文字起こしを新しい順に一覧返却。
    text本文は冒頭200文字のプレビューのみ。全文は /transcribe/history/{filename} で取得。

    クエリパラメータ:
    - limit (int, 任意) 返却件数（既定20、最大100）
    """
    limit = max(1, min(limit, 100))
    try:
        bucket  = _get_storage_client().bucket(BUCKET_NAME)
        blobs   = list(bucket.list_blobs(prefix=f"{GCS_RESULT_PREFIX}/", max_results=HISTORY_SCAN_MAX))
        # _temp / _logs を除外し、JSONのみ抽出
        entries = [
            b for b in blobs
            if not b.name.startswith(f"{GCS_RESULT_PREFIX}/_")
            and b.name.endswith(".json")
        ]
        # ファイル名がYYYYMMDD_HHMMSS_xxxなので、降順ソートで新しい順
        entries.sort(key=lambda b: b.name, reverse=True)
        entries = entries[:limit]

        results = []
        for blob in entries:
            try:
                data = json.loads(blob.download_as_text())
                text = data.get("text") or ""
                results.append({
                    "filename":             blob.name.split("/")[-1],
                    "created_at":           data.get("created_at"),
                    "source":               data.get("source"),
                    "source_info":          data.get("source_info"),
                    "transcription_method": data.get("transcription_method"),
                    "model":                data.get("model"),
                    "options":              data.get("options"),
                    "text_preview":         text[:200] + ("…" if len(text) > 200 else ""),
                    "text_length":          len(text),
                })
            except Exception as e:
                results.append({"filename": blob.name.split("/")[-1], "error": str(e)})

        return {"count": len(results), "data": results}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/transcribe/history/{filename}", summary="文字起こし履歴の個別取得")
def transcribe_history_one(filename: str):
    """
    保存済み文字起こしJSONを全文取得します（再要約用）。

    パスパラメータ:
    - filename: /transcribe/history で返ったfilenameをそのまま指定
    """
    # パストラバーサル防止
    if "/" in filename or ".." in filename or not filename.endswith(".json"):
        return JSONResponse(status_code=400, content={"error": "不正なファイル名"})

    blob_name = f"{GCS_RESULT_PREFIX}/{filename}"
    bucket    = _get_storage_client().bucket(BUCKET_NAME)
    blob      = bucket.blob(blob_name)

    if not blob.exists():
        return JSONResponse(status_code=404, content={"error": "見つかりません", "filename": filename})

    try:
        return json.loads(blob.download_as_text())
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
