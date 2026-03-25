import smtplib                          # メール送信の標準ライブラリ
import os
from email.mime.text import MIMEText    # メール本文を作成するクラス
from email.mime.multipart import MIMEMultipart  # マルチパートメールを作成するクラス

def send_gmail(subject: str, body: str) -> bool:
    """
    Gmail でメール通知を送信する

    subject : メールの件名
    body    : メールの本文
    戻り値  : 成功=True / 失敗=False
    """

    # 環境変数から送信元アドレスとアプリパスワードを取得する
    gmail_address  = os.environ["GMAIL_ADDRESS"]
    app_password   = os.environ["GMAIL_APP_PASSWORD"]

    # メールの構造を作成する
    # MIMEMultipart: 件名・送信元・宛先などのヘッダーを持つメールオブジェクト
    msg = MIMEMultipart()
    msg["Subject"] = subject          # 件名
    msg["From"]    = gmail_address    # 送信元（自分のアドレス）
    msg["To"]      = gmail_address    # 宛先（自分のアドレスに送る）

    # 本文を追加する
    # MIMEText: テキスト形式の本文オブジェクト
    # "utf-8": 日本語を送るために必要
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        # GmailのSMTPサーバーに接続する
        # smtp.gmail.com: GmailのSMTPサーバーアドレス
        # 465: SSL接続のポート番号
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:

            # アプリパスワードでログインする
            smtp.login(gmail_address, app_password)

            # メールを送信する
            # send_message: msgオブジェクトをそのまま送信する
            smtp.send_message(msg)

        print("✅ Gmail 通知を送信しました")
        return True

    except Exception as e:
        # エラーが起きてもプログラム全体は止めない
        print(f"❌ Gmail 通知失敗: {e}")
        return False
