# 目的：10月1日の取りこぼし（障害者求人97件を含む約1,100件）を回収する
# 内容：収集処理 collect_hellowork() を、1回250件ずつ・合計23分以内で繰り返し実行する（1回ごとに保存されるので途中で止まっても失われない）
#       当日の一覧キャッシュは障害者求人を先頭に並べ替えて使う（件数の少ない障害者求人を先に取り切るため）
#       夜間収集（20:00〜20:50）が終わった後に実行すること

import time

from app.collectors import hellowork as hw

hw.TIME_BUDGET = 420          # 1回あたりの時間上限（秒）
TOTAL_BUDGET   = 23 * 60      # 全体の時間上限（ワークフローの30分制限より手前で止める）

# 一覧キャッシュを読むときだけ、障害者求人を先頭に並べ替える
_orig_read = hw._read_parquet
def _read_sorted(bucket, path):
    df = _orig_read(bucket, path)
    if df is not None and path.startswith(hw.LIST_PREFIX):
        df = df.sort_values("kind", key=lambda s: s.ne("障害者"), kind="stable")
    return df
hw._read_parquet = _read_sorted

started, total, n = time.monotonic(), 0, 0
while time.monotonic() - started < TOTAL_BUDGET - hw.TIME_BUDGET:
    n += 1
    added = hw.collect_hellowork(max_details=250)
    total += added
    print(f"--- {n}回目: {added}件追加（累計 {total}件 / {time.monotonic() - started:.0f}秒）")
    if added == 0:
        break
print(f"完了: 合計 {total}件を追加")
