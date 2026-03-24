FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# デバッグ：コンテナ内のファイル一覧を表示
RUN echo "=== /app/app/ の中身 ===" && ls -la /app/app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
