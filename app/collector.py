# 定期データ収集のメイン関数
# Cloud Schedulerから /collect エンドポイント経由で呼び出される

import os
import asyncio
from app.notifier          import send_gmail
from app.collectors.d_kanko import collect_d_kanko_monthly


async def run_all_collections():
    # 全データソースの定期収集を実行する
    total  = 0
    errors = []

    # デジタル観光統計オープンデータの定期収集（毎月第2木曜）
    try:
        count = await asyncio.to_thread(collect_d_kanko_monthly)
        total += count
        print(f"✅ d_kanko: {count}件")
    except Exception as e:
        print(f"❌ エラー: d_kanko: {e}")
        errors.append("d_kanko")

    # 収集完了をGmailで通知する
    if errors:
        send_gmail(
            subject="【Stats API】データ収集完了（一部エラー）",
            body=(
                f"データ収集が完了しました。\n\n"
                f"成功: {total}件\n"
                f"失敗: {', '.join(errors)}\n\n"
                f"Cloud Consoleでログを確認してください。"
            )
        )
    else:
        send_gmail(
            subject="【Stats API】データ収集完了",
            body=(
                f"データ収集が正常に完了しました。\n\n"
                f"取得件数: {total}件"
            )
        )

    print(f"収集完了: 成功{total}件 / 失敗{len(errors)}件")
