# 目的：職業分類コードを取れなかった求人（掲載終了）の推定コード付与の準備（読み取りのみ・GCSへの書き込みなし）
# 内容：
#   1. 職業分類_小分類コードがない求人について、同じ事業所番号・同じ職種（表記を揃えて一致）の求人に付いている公式コードを写せるか調べる
#   2. 写せなかった求人と、精度確認用にコードが分かっている求人60件の判定材料（職種・仕事内容など）を暗号化して出力する
#      判定は Claude が会話の中で行う。リポジトリは公開のため、求人の中身は公開鍵で暗号化し、平文では出力しない
#      （秘密鍵は Claude の作業環境にのみあり、リポジトリには置かない）
#   出力は件数と暗号文だけ

import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "cryptography==46.0.3"], check=True)

import base64
import json
import os
import re
import unicodedata
import zlib

import pandas as pd
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from google.cloud import storage

from app.collectors import hellowork as hw

PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAjg1CcF78mA7xXvPtnc/D
/dg4omarRU7IAhkg6FtuA/jxtGwV3L1jKTkOxPoK4sH/laGiOujKEzneMszQnTWA
is+FGFw+FbXt9oWb2DmFvzsGppARQ8owIoweR+XAFBKzq5NpEB/MFR027ulR0RdF
qhNy7+TEQbf130c7IGRZEZfS3AXqB4v2+NX68ey9YlKg+ys2UiXs0hXGj9aRCRtp
e1GuoAv6mUNr2STOtu5u/wGzcmT8g0M4HHSytEQTQF37y1Nsk0qSTxkkSzCe0vJa
Br8zPH1DqkqB3sGHzEJMSPh6w5KVChRyklMMia5ByCLRA6R2X0QOjlXESllSqtLe
CfUA5fe3nfQU8lmUsWOgjH7nz16LjktWFE43E5CnhUUwIfb+G/q5a57uqn+1UJJ+
ab0UBkB65KLT3y3LRSumLZf3363xygOx4Hwh50Odtx1LwFxcv7shwppxBu6QZ/E3
Xc+t3/LXSeXr9i8MVRS4dHH1zTYQMEkiC0iHd6ufDQBVAgMBAAE=
-----END PUBLIC KEY-----"""


def encrypt(obj) -> str:
    # JSON → zlib圧縮 → AES-GCM（鍵はRSA-OAEPで包む）→ base64
    data = zlib.compress(json.dumps(obj, ensure_ascii=False).encode("utf-8"), 9)
    key, nonce = AESGCM.generate_key(bit_length=256), os.urandom(12)
    ct = AESGCM(key).encrypt(nonce, data, None)
    pub = serialization.load_pem_public_key(PUBLIC_KEY.encode())
    wk = pub.encrypt(key, padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
    return base64.b64encode(len(wk).to_bytes(2, "big") + wk + nonce + ct).decode()


def norm(s) -> str:
    if s is None or pd.isna(s):
        return ""
    return re.sub(r"\s", "", unicodedata.normalize("NFKC", str(s)))


bucket = storage.Client().bucket(hw.BUCKET_NAME)
months = sorted(b.name for b in bucket.list_blobs(prefix=hw.DATA_PREFIX + "/") if b.name.endswith(".parquet"))
df = pd.concat([hw.normalize(hw._read_parquet(bucket, p)) for p in months], ignore_index=True)
df["_key"] = df["事業所番号"].map(norm) + "|" + df["職種"].map(norm)
coded = df[df["職業分類_小分類コード"].notna()]
missing = df[df["職業分類_小分類コード"].isna()].drop_duplicates("求人番号")
print(f"全 {len(df)}件 / コードなし {len(missing)}件")

# 1. 同じ事業所・同じ職種の公式コードを写す（候補が1種類のときだけ）
cand = coded.groupby("_key")["職業分類_小分類コード"].agg(lambda s: sorted(set(s)))
copied, ambiguous = {}, 0
for _, r in missing.iterrows():
    c = cand.get(r["_key"])
    if c is not None and len(c) == 1:
        copied[r["求人番号"]] = c[0]
    elif c is not None:
        ambiguous += 1
print(f"同じ事業所・職種から写せる {len(copied)}件 / 候補が複数で写さない {ambiguous}件")

# 2. 判定材料（写せなかった求人 + 精度確認用60件）
cols = ["求人番号", "職種", "仕事内容", "産業分類", "雇用形態", "必要な経験等", "必要な免許資格", "免許資格_名称", "事業所名"]
def pack(rows):
    out = []
    for _, r in rows.iterrows():
        d = {c: (None if pd.isna(r[c]) else str(r[c])) for c in cols}
        d["仕事内容"] = (d["仕事内容"] or "")[:500]
        out.append(d)
    return out
todo = missing[~missing["求人番号"].isin(copied)]
val = coded.drop_duplicates("求人番号").sample(60, random_state=20261009)
print(f"判定対象 {len(todo)}件 / 精度確認用 {len(val)}件")
payload = {"copied": copied, "todo": pack(todo), "val": pack(val)}
answers = dict(zip(val["求人番号"], val["職業分類_小分類コード"]))
print("PAYLOAD_BEGIN"); print(encrypt(payload)); print("PAYLOAD_END")
print("ANSWERS_BEGIN"); print(encrypt(answers)); print("ANSWERS_END")
