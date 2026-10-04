# 在留外国人統計 テーブルデータ（市区町村別）収集スクリプト（GitHub Actions用）
# 使い方: python scripts/collect_zairyu.py [dryrun|full|update]

import os
import sys

# プロジェクトルートをパスに追加する
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.zairyu import run

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "dryrun"
    print(f"🚀 在留外国人統計 収集開始 mode={mode}", flush=True)
    n = run(mode=mode)
    print(f"✅ 終了: {n}時点", flush=True)
