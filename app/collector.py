# 定期データ収集のメイン関数
# Cloud Schedulerから /collect エンドポイント経由で呼び出される
# 各データソースは独立した関数として実装し、個別のタイミングで実行可能

import asyncio
from app.notifier              import send_gmail
from app.collectors.d_kanko    import collect_d_kanko_monthly
from app.collectors.n_roudou   import collect_n_roudou_monthly


# ── 共通ヘルパー ─────────────────────────────────────────

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
            body=(
                f"{source_name} の定期収集が正常に完了しました。\n\n"
                f"取得件数: {count}件"
            )
        )


# ── データソース別の収集関数 ──────────────────────────────

async def run_d_kanko_collection():
    # デジタル観光統計オープンデータの定期収集
    # 実行タイミング: 毎月第2木曜 1:00 JST
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
    # 長野労働局「最近の雇用情勢」月次PDFの定期収集
    # 実行タイミング: 毎月末最終日 23:00 JST
    try:
        count = await asyncio.to_thread(collect_n_roudou_monthly)
        print(f"✅ n_roudou: {count}件")
        _notify("n_roudou", count)
        return count
    except Exception as e:
        print(f"❌ エラー: n_roudou: {e}")
        _notify("n_roudou", 0, error=str(e))
        return 0


# ── 全データソース一括収集（手動実行・デバッグ用） ─────────

async def run_all_collections():
    # 全データソースの定期収集を実行する（ラッパー関数）
    # 通常はデータソース別の個別関数をCloud Schedulerから呼ぶ
    # 全件バックフィルや開発時の一括実行に使用する
    await run_d_kanko_collection()
    await run_n_roudou_collection()
