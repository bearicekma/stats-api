# 国税庁 法人番号 全件データ（全国・閉鎖法人を含む）収集スクリプト（GitHub Actions用）
# 使い方: python scripts/collect_houjin.py [dryrun|full]

import os
import sys

# プロジェクトルートをパスに追加する
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.houjin import run

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "dryrun"
    print(f"🚀 法人番号 全件データ 収集開始 mode={mode}", flush=True)
    n = run(mode=mode)
    print(f"✅ 終了: {n}ファイル", flush=True)
