# Stats API のDockerイメージ定義
FROM python:3.11-slim

# ffmpeg を追加（動画→音声抽出・yt-dlpの音声変換用）
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

CMD ["hypercorn", "app.main:app", "--bind", "0.0.0.0:8080"]
