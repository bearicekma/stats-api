
import os
import json
import anthropic  # AnthropicのPython公式ライブラリ

def process_to_tidy(raw_data: str, description: str) -> list[dict]:
    """
    生データをClaude Haikuで整然データ（tidy data）に変換する

    raw_data    : APIやWebから取得した生のテキストデータ
    description : データの説明（例："日本の年別人口データ"）
    戻り値      : 整然データの辞書リスト
    """

    # 環境変数からAPIキーを取得（コードに直書きしないためのセキュリティ対策）
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    # Claudeへの指示文（プロンプト）を作成
    # 整然データとは「1行1観測、1列1変数」の形式
    prompt = f"""
以下のデータを整然データ（tidy data）形式のJSONに変換してください。

データの説明: {description}

生データ:
{raw_data}

ルール:
- 1つのリスト、各要素は1観測を表す辞書
- キー名は英語のスネークケース（例: population_count）
- 数値は文字列でなく数値型で返す
- 不明な値はnullにする
- JSON以外の文字（説明文など）は含めない

出力形式:
[{{"year": 2024, "value": 125000, ...}}, ...]
"""

    # Claude Haiku APIを呼び出す
    # max_tokens: 生成する最大トークン数（1000で十分）
    message = client.messages.create(
        model="claude-haiku-4-5",  # コスト最小のHaikuモデルを使用
        max_tokens=1000,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    # レスポンスのテキスト部分を取り出す
    response_text = message.content[0].text

    # JSON文字列をPythonのリストに変換
    # strip()で前後の空白や改行を除去してからパース
    return json.loads(response_text.strip())
