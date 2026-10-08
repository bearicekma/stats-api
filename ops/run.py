# 目的：保存済みのハローワーク求人に職業分類コード（厚労省編、_M_shokugyo_hw）を書き足す
# 内容：app/collectors/hellowork.py の backfill_shokugyo を実行する
#       - 詳細ページを取り直し、「職種解説」リンクの code= から小分類コードを取り、職業分類の3列だけを書き足す（ほかの列は変えない）
#       - 掲載終了で取れない求人は空のまま。試した求人番号は hellowork/_backfill/shokugyo_tried.json に記録し、次の実行では飛ばす
#       - 1回あたり約25分で打ち切るので、残りがなくなるまで Actions 画面（ops → Run workflow）から繰り返し実行する
#       - 19:45〜22:00（夜間収集の時間帯）は途中で止まる
#       出力は件数だけ

from app.collectors import hellowork as hw

DRY_RUN = False                             # まず短時間の確認（保存なし）→ 問題なければ False にして push
TIME_BUDGET = 60 if DRY_RUN else 25 * 60    # 秒

hw.backfill_shokugyo(time_budget=TIME_BUDGET, dry_run=DRY_RUN, save_every=200)
