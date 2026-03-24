
import os
import json
import re
import anthropic

def process_to_tidy(raw_data: str, description: str) -> list[dict]:
    """
    生データをClaude Haikuで整然データに変換する
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

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
- JSON以外の文字（説明文・マークダウン記法など）は含めない

出力形式（このフォーマットのみで返すこと）:
[{{"year": 2024, "value": 125000}}]
"""

    message = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )

    response_text = message.content[0].text

    # デバッグ用：Claudeの生レスポンスを確認する
    print(f"Claudeレスポンス（先頭200文字）: {response_text[:200]}")

    # マークダウンのコードブロック記法を除去する
    # ```json ... ``` や ``` ... ``` の両方に対応
    response_text = re.sub(r"```(?:json)?\s*", "", response_text)
    response_text = re.sub(r"```", "", response_text)
    response_text = response_text.strip()

    # [ から ] までのJSON配列部分だけを抽出する
    # Claudeが前後に説明文を付けた場合でも対応できる
    match = re.search(r"\[.*\]", response_text, re.DOTALL)
    if match:
        response_text = match.group(0)
    else:
        # JSON配列が見つからない場合はエラーの詳細を出力
        print(f"❌ JSON配列が見つかりません。レスポンス全文: {response_text}")
        raise ValueError(f"Claude のレスポンスに JSON 配列が含まれていません")

    return json.loads(response_text)
