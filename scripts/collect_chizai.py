# 地方財政状況調査 調査表CSV 収集スクリプト（GitHub Actions用）
# 使い方: python scripts/collect_chizai.py [dryrun|full|update] [pref]

import os
import sys

# プロジェクトルートをパスに追加する
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.chizai import run

if __name__ == "__main__":
    mode  = sys.argv[1] if len(sys.argv) > 1 else "dryrun"
    kubun = sys.argv[2] if len(sys.argv) > 2 else "pref"
    print(f"🚀 地方財政状況調査 収集開始 mode={mode} kubun={kubun}", flush=True)
    n = run(kubun=kubun, mode=mode)
    print(f"✅ 終了: {n}表", flush=True)
