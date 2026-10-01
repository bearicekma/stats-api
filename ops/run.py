# 目的：ハローワーク夜間収集の実行回数を 4回（20:00〜20:45）→ 8回（20:00〜21:45、15分おき）に増やす
# 内容：Cloud Scheduler のジョブ hellowork-collect のスケジュールを gcloud で更新し、更新後の設定を表示する
#       （サービスアカウントに Cloud Scheduler の権限がなければ失敗する → その場合は Colab から更新する）

import os
import subprocess

key = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
run = lambda *args: print(subprocess.run(["gcloud", *args], capture_output=True, text=True).__getattribute__("stdout") or "", end="")

subprocess.run(["gcloud", "auth", "activate-service-account", f"--key-file={key}", "--quiet"], check=True, capture_output=True)
r = subprocess.run(["gcloud", "scheduler", "jobs", "update", "http", "hellowork-collect",
                    "--project=stats-api-491107", "--location=asia-northeast1",
                    "--schedule=0,15,30,45 20-21 * * *", "--time-zone=Asia/Tokyo", "--quiet"],
                   capture_output=True, text=True)
print("update exit:", r.returncode)
print((r.stderr or "").strip()[-600:])
d = subprocess.run(["gcloud", "scheduler", "jobs", "describe", "hellowork-collect", "--project=stats-api-491107",
                    "--location=asia-northeast1", "--format=value(schedule,timeZone,state,attemptDeadline,retryConfig.retryCount)"],
                   capture_output=True, text=True)
print("describe:", d.stdout.strip(), (d.stderr or "").strip()[-300:])
