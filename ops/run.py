# 目的：Cloud Run（stats-api）のメモリ・CPU設定を確認する（_M_houjin を1ファイルで置く設計の判断用）
# 内容：読み取りのみ。Cloud Run Admin API でサービスのリソース設定を取得して出力する

import google.auth
import google.auth.transport.requests
import httpx

cred, project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
cred.refresh(google.auth.transport.requests.Request())
url = f"https://run.googleapis.com/v2/projects/{project}/locations/asia-northeast1/services/stats-api"
r = httpx.get(url, headers={"Authorization": f"Bearer {cred.token}"}, timeout=30)
print("status:", r.status_code)
if r.status_code == 200:
    t = r.json().get("template", {})
    for c in t.get("containers", []):
        print("resources:", c.get("resources"))
    print("scaling:", t.get("scaling"), "/ maxInstanceRequestConcurrency:", t.get("maxInstanceRequestConcurrency"), "/ timeout:", t.get("timeout"))
    print("executionEnvironment:", t.get("executionEnvironment"))
else:
    print(r.text[:300])
