# 定期データ収集のメイン関数
# Cloud Schedulerから /collect エンドポイント経由で呼び出される

import asyncio
from app.notifier              import send_gmail
from app.collectors.d_kanko    import collect_d_kanko_monthly
from app.collectors.n_roudou   import collect_n_roudou_monthly
from app.collectors.enecho     import collect_enecho_gasoline
from app.collectors.jma        import collect_jma_nagano


def _notify(source_name: str, count: int, error: str | None = None):
    # 収集結果をGmailで通知する共通関数
    if error:
        send_gmail(
            subject=f"【Stats API】{source_name} 収集エラー",
            body=(
                f"{source_name} の定期収集でエラーが発生しました。\n\n"
                f"エラー内容: {error}\n\n"
                f"Cloud Consoleでログを確認してください。"
            )
        )
    else:
        send_gmail(
            subject=f"【Stats API】{source_name} 収集完了",
            body=f"{source_name} の定期収集が正常に完了しました。\n\n取得件数: {count}件"
        )


async def run_d_kanko_collection():
    # デジタル観光統計オープンデータの定期収集（毎月第2木曜 1:00 JST）
    # cron「8-14 * *」はOR仕様のため、コード側で木曜（weekday=3）を確認する
    from datetime import datetime, timezone, timedelta
    JST = timezone(timedelta(hours=9))
    if datetime.now(JST).weekday() != 3:  # 3=木曜
        print("⏭ d_kanko: 木曜以外のためスキップ")
        return 0
    try:
        count = await asyncio.to_thread(collect_d_kanko_monthly)
        print(f"✅ d_kanko: {count}件")
        _notify("d_kanko", count)
        return count
    except Exception as e:
        print(f"❌ エラー: d_kanko: {e}")
        _notify("d_kanko", 0, error=str(e))
        return 0


async def run_n_roudou_collection():
    # 長野労働局 月次PDFの定期収集（毎月末 23:00 JST）
    try:
        count = await asyncio.to_thread(collect_n_roudou_monthly)
        print(f"✅ n_roudou: {count}件")
        _notify("n_roudou", count)
        return count
    except Exception as e:
        print(f"❌ エラー: n_roudou: {e}")
        _notify("n_roudou", 0, error=str(e))
        return 0


async def run_enecho_collection():
    # 資源エネルギー庁 給油所小売価格調査の定期収集（毎週水曜 15:00 JST、GitHub Actions経由）
    try:
        count = await asyncio.to_thread(collect_enecho_gasoline)
        print(f"✅ enecho: {count}件")
        _notify("enecho_gasoline", count)
        return count
    except Exception as e:
        print(f"❌ エラー: enecho: {e}")
        _notify("enecho_gasoline", 0, error=str(e))
        return 0


async def run_jma_collection():
    # 気象庁 長野県天気予報の定期収集・LINE通知（毎朝6:00 JST）
    # LINE通知はcollect_jma_nagano()内部で実行される
    try:
        count = await asyncio.to_thread(collect_jma_nagano)
        print(f"✅ jma: {count}件")
        # 通知方針: 成功(正常件数)時はメールを送らない。
        # 例外は下のexceptで、0件は収集失敗とみなしてここでエラー通知する。
        if count == 0:
            _notify("jma_nagano", 0,
                    error="取得件数が0件でした（天気APIの仕様変更やレスポンス異常の可能性）")
        return count
    except Exception as e:
        print(f"❌ エラー: jma: {e}")
        _notify("jma_nagano", 0, error=str(e))
        return 0


async def run_all_collections():
    # 全データソースを一括実行する（手動・デバッグ用）
    await run_d_kanko_collection()
    await run_n_roudou_collection()
    await run_enecho_collection()
    await run_jma_collection()
