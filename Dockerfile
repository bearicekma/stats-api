FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# キャッシュ破棄用（変更のたびに日付を更新する）
# 2026-03-23
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
