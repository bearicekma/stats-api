
# google-cloud-firestoreライブラリからクライアントクラスをインポート
from google.cloud import firestore

# Firestoreクライアントを初期化する関数
# 呼び出すたびに新しい接続を作るのではなく、
# モジュール読み込み時に1回だけ作成して使い回す
db = firestore.Client()

def save_stats(collection: str, doc_id: str, data: dict):
    """
    Firestoreにデータを保存する

    collection : テーブルに相当する概念（例："population"）
    doc_id     : レコードのID（例："2024"）
    data       : 保存する辞書データ
    """

    # collection(テーブル).document(行) という階層構造でデータを指定
    # set()で保存。同じdoc_idがあれば上書き、なければ新規作成
    db.collection(collection).document(doc_id).set(data)
    print(f"✅ Firestore に保存しました: {collection}/{doc_id}")

def get_stats(collection: str):
    """
    Firestoreからコレクション内の全データを取得する

    戻り値: 辞書のリスト（各ドキュメントのデータ）
    """

    # stream()でコレクション内の全ドキュメントを順に取得
    # doc.to_dict()でFirestoreのドキュメントをPython辞書に変換
    docs = db.collection(collection).stream()
    return [doc.to_dict() for doc in docs]
