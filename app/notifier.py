
import httpx
import os

async def send_line_message(message: str) -> bool:
    """
    LINE Messaging API でメッセージを送信する

    message : 送信するテキスト
    戻り値  : 成功=True / 失敗=False
    """

    # 環境変数からトークンとユーザーIDを取得
    # コードに直書きせず環境変数から読むのはセキュリティの基本
    token   = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
    user_id = os.environ["LINE_USER_ID"]

    # asyncwith で非同期HTTPクライアントを作成
    # 処理が終わると自動的に接続を閉じる
    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.line.me/v2/bot/message/push",  # LINE APIのエンドポイント
            headers={
                "Authorization": f"Bearer {token}",     # 認証トークンをヘッダーに付ける
                "Content-Type":  "application/json",
            },
            json={
                "to":       user_id,                     # 送信先（自分のユーザーID）
                "messages": [{"type": "text", "text": message}],  # テキストメッセージ
            },
        )

    # ステータスコード200なら成功
    if res.status_code == 200:
        print("✅ LINE 通知を送信しました")
        return True
    else:
        # 失敗してもプログラム全体は止めない（Falseを返すだけ）
        print(f"❌ LINE 通知失敗: {res.status_code} {res.text}")
        return False
