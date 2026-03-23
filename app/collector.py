
import httpx           # 非同期対応のHTTPクライアント（requestsの非同期版）
import os
from app.processor import process_to_tidy   # 3-2で作った加工関数
from app.database  import save_stats        # 3-1で作った保存関数
from app.notifier  import send_line_message # ステップ2で作った通知関数

async def collect_estat(app_id: str, stats_data_id: str, description: str, collection_name: str):
    """
    e-Stat APIからデータを取得してFirestoreに保存する

    app_id          : e-StatのアプリケーションID
    stats_data_id   : 取得したい統計表のID
    description     : データの説明（Claude Haikuへの指示に使う）
    collection_name : Firestoreの保存先コレクション名
    """

    # e-Stat APIのエンドポイントURLとパラメータを定義
    url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
    params = {
        "appId":       app_id,
        "statsDataId": stats_data_id,
        "lang":        "J",           # 日本語で取得
        "limit":       100,           # 最大100件取得
    }

    # asyncwith: 非同期コンテキストマネージャ
    # 処理が終わったら自動的に接続を閉じてくれる
    async with httpx.AsyncClient() as client:
        # awaitで非同期HTTPリクエストを送信
        # 待っている間は他の処理を実行できる（非同期のメリット）
        response = await client.get(url, params=params, timeout=30)

    # ステータスコードが200以外（エラー）の場合は例外を発生させる
    response.raise_for_status()

    # レスポンスのJSONをPython辞書に変換
    raw_json = response.json()

    # APIレスポンスの深い階層からデータ部分だけを取り出す
    # e-StatのJSON構造: GET_STATS_DATA > STATISTICAL_DATA > DATA_INF > VALUE
    values = raw_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]

    # リストを文字列に変換してClaude Haikuに渡す
    # json.dumps的なstrでなくstr()を使うのは簡易的なデバッグのため
    raw_text = str(values[:20])  # 多すぎるとトークンが増えるので最初の20件

    # Claude Haikuで整然データに加工
    print(f"🤖 Claude Haiku でデータを加工中: {description}")
    tidy_data = process_to_tidy(raw_text, description)

    # Firestoreに1件ずつ保存
    # enumerate()でインデックス番号iと値dataを同時に取得
    for i, data in enumerate(tidy_data):
        save_stats(collection_name, str(i), data)

    print(f"✅ {collection_name}: {len(tidy_data)}件を保存しました")
    return len(tidy_data)  # 保存件数を返す（通知メッセージで使う）


async def run_all_collections():
    """
    全統計データを収集するメイン関数
    Cloud Schedulerから定期的に呼び出される
    """

    # e-StatのアプリケーションIDを環境変数から取得
    app_id = os.environ["ESTAT_APP_ID"]

    # 収集するデータの定義リスト
    # 追加したいデータはここにdictを追加するだけでよい設計
    targets = [
        {
            "stats_data_id":   "0003448237",       # e-Statの統計表ID
            "description":     "日本の年別人口推計",
            "collection_name": "population",        # Firestoreのコレクション名
        },
        {
            "stats_data_id":   "0003427113",
            "description":     "消費者物価指数",
            "collection_name": "cpi",
        },
    ]

    total  = 0   # 収集成功件数
    errors = []  # エラーになったデータ名のリスト

    # 各ターゲットを順番に収集
    for target in targets:
        try:
            # **target はdictをキーワード引数として展開する記法
            # collect_estat(stats_data_id="...", description="...", ...) と同じ
            count = await collect_estat(app_id=app_id, **target)
            total += count

        except Exception as e:
            # エラーが起きても次のデータの収集を続ける
            # 1つ失敗しても全体が止まらないようにするための設計
            print(f"❌ エラー: {target['collection_name']}: {e}")
            errors.append(target["collection_name"])

    # 全収集完了後にLINEで通知
    if errors:
        # 一部エラーがあった場合
        await send_line_message(
            f"データ収集完了（一部エラー）\n"
            f"成功: {total}件\n"
            f"失敗: {', '.join(errors)}"
        )
    else:
        # 全て成功した場合
        await send_line_message(
            f"データ収集完了\n"
            f"取得件数: {total}件\n"
            f"次回: 明日 00:00"
        )
