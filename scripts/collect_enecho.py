# 資源エネルギー庁 給油所小売価格調査 収集スクリプト（GitHub Actions用）
# FastAPIに依存しないスタンドアロン版
# app/collectors/enecho.py のロジックを直接呼び出す

import sys
import os

# プロジェクトルートをパスに追加する
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.enecho import collect_enecho_gasoline

if __name__ == "__main__":
    print("🚀 資源エネルギー庁 給油所小売価格調査 収集開始")
    count = collect_enecho_gasoline()
    print(f"✅ 完了: {count}件をGCSに保存しました")
