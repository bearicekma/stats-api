# ops：運用作業の実行場所

GCS のデータ修正・マスタ作成・付け直しなど、一回限りの運用作業を GitHub Actions 上で実行するための仕組みです。
Colab や手元の環境から GCS に接続しなくても、`ops/run.py` を書き換えて push するだけで実行できます。

## 使い方

1. `ops/run.py` を書き換える（先頭のコメントに「目的」と「何をするか」を書く）
2. main に push する → `.github/workflows/ops.yml` が自動で実行される
3. 実行ログは `ops-log` ブランチの `run.log` に上書きで保存される
   - 確認: `git fetch origin ops-log && git show origin/ops-log:run.log`
   - 1行目の `commit:` が push したコミットと一致しているかで、どの実行のログかを判断する

Actions の画面（ops ワークフロー → Run workflow）から、現在の `ops/run.py` を再実行することもできます。

## 約束事

- **書き込みは2段階**：GCS に書き込む処理は、まず `DRY_RUN = True` で実行して結果を確認し、問題なければ `False` にして再度 push する
- **出力は集計だけ**：リポジトリは公開のため、`ops-log` ブランチも Actions のログも誰でも見られる。件数・列名・コードなどの集計だけを print し、求人の中身や認証情報は出力しない
- **使える認証情報は GCS 用だけ**：`GCP_SERVICE_ACCOUNT_KEY` と `GCS_BUCKET_NAME` のみ設定される。e-Stat の appId など他のシークレットが必要な作業は、その都度 `ops.yml` に追加する
- **本番のコードを呼ぶ**：`requirements.txt` を入れているので、`app/` 以下の関数をそのまま import して使える
- **デプロイは走らない**：`ops/` だけの変更では `deploy.yml` は実行されない
- 同時に実行されるのは1本だけ、1回の上限は30分

## 実行してはいけない時間帯

- **20:00〜21:55**：ハローワーク夜間収集（Cloud Scheduler `hellowork-collect`、15分おき・8回）が `hellowork/kyujin/` に書き込む。
  この時間帯に同じファイルへ書き込む作業を行うと、どちらかの更新が失われる
