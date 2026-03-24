
import httpx
import os

async def send_line_message(message: str) -> bool:
    token   = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
    user_id = os.environ["LINE_USER_ID"]

    async with httpx.AsyncClient() as client:
        res = await client.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            },
            json={
                "to":       user_id,
                "messages": [{"type": "text", "text": message}],
            },
        )

    if res.status_code == 200:
        print("✅ LINE 通知を送信しました")
        return True
    else:
        print(f"❌ LINE 通知失敗: {res.status_code} {res.text}")
        return False
