# 目的：google-genai 3.0 の既定APIバージョン変更（v1）で失敗した PDF リネームを処理し直す
# 内容：drive_rename の通常処理（run_drive_rename）を、対象期間を「保存から5日以内」に広げて1回実行する
#       （11/5 保存分が通常の3日以内の条件から外れるため）。Gemini の無料枠を件数分使う
#       ログは公開されるため、ファイル名は出さず件数・方式・サイズだけ表示する

from app.collectors import drive_rename as dr

dr.RECENT_DAYS = 5
print("Geminiモデル候補:", dr.GEMINI_MODELS)
res = dr.run_drive_rename()
print(f"リネーム {len(res['renamed'])}件 / 対象外 {res['skipped']}件 / 失敗 {len(res['failed'])}件")
for n, r in enumerate(res["renamed"], 1):
    size = f"{r['size_before']:,}→{r['size_after']:,}" if r.get("size_before") else "-"
    print(f"[{n}] 日付:{r.get('date_source')} 圧縮:{r.get('optimized')} {size} {r.get('note', '')}")
for n, f in enumerate(res["failed"], 1):
    print(f"[失敗{n}] {str(f.get('reason'))[:200]}")
