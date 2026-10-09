# 目的：職業分類コードを取れなかった求人（掲載終了の101件）に、推定した職業分類コードを書き足す（一時的な対応）
# 内容：
#   - 15件は同じ事業所番号・同じ職種の求人の公式コードを写したもの、86件は Claude が職種・仕事内容等から判定したもの
#     （精度確認：コードが分かっている60件で 大分類97% / 中分類80% / 小分類67% 一致）
#   - 求人番号はそのまま置かず、数字部分の SHA-256 先頭16桁で照合する
#   - 職業分類_小分類コードが空の行だけに書き足し、中分類・大分類は _M_shokugyo_hw から付ける。ほかの列は変えない
#   - DRY_RUN = True なら件数の確認だけ（保存しない）
#   出力は件数だけ

import hashlib
import re

from google.cloud import storage

from app.collectors import hellowork as hw

DRY_RUN = True

CODES = {
    "000150f38651f08d": "096-01",
    "0313e6c6d66c6327": "050-02",
    "04ba14a8937bfba0": "045-07",
    "07006d61c1caa00f": "096-01",
    "0d28cdc1dbcae4c0": "094-04",
    "0da6c0f2faebfdea": "040-01",
    "18afe6d41570c6ac": "074-13",
    "1b1fc21798bced14": "033-03",
    "1c7cca171d90e65e": "023-99",
    "1ea56e04d7a2cf82": "034-01",
    "210b1547361aa23e": "008-05",
    "2120ffa5c9b60d1d": "096-03",
    "2183606407f3c521": "038-03",
    "2373a0002a04538b": "085-01",
    "28680643085ac497": "050-02",
    "2d17e82fdf2afee3": "071-11",
    "2ff1c7cfd0b0e5f0": "096-99",
    "3029e03980810486": "091-06",
    "30b763406036ef3e": "033-03",
    "349ff85f66407506": "094-05",
    "36676ab0cbc4b4d9": "092-01",
    "3a8084532d594881": "034-01",
    "3cb844ec5c76222b": "029-01",
    "3d4a06362852087f": "023-02",
    "3ee04642533cd801": "057-99",
    "459eaf2f837bebc6": "042-03",
    "492849be073e70cb": "064-01",
    "4959418745c6014e": "096-01",
    "4a9b8c621e5d6d27": "034-01",
    "4bc82f76c8bb6e1b": "078-02",
    "4d9f2e02faad739d": "050-99",
    "510dd4f6274b386a": "050-02",
    "5138e7f531f216a7": "083-01",
    "52e637462d9dcb92": "096-99",
    "53e3a6d64aa49daa": "034-03",
    "55b74805126923f3": "056-03",
    "59148794521b2c83": "072-05",
    "5b67eadc5c88a29e": "064-02",
    "5d9c0ead4d83c3e3": "055-08",
    "5e9d7b82bfabf71e": "020-99",
    "6d0316e9e7578a66": "064-01",
    "75fcdfe3dd1182fc": "071-06",
    "77a6fdc02b874f87": "036-02",
    "803662f88a29eb8d": "069-04",
    "8045207eed656a84": "082-01",
    "8218654ad72a93ab": "075-03",
    "82f362089bab0f0e": "050-01",
    "8452c50b190a30d3": "075-01",
    "8585269fcad871a9": "094-05",
    "86dcc6f93957fc31": "055-07",
    "88c964d0cb24f4a8": "089-01",
    "897090f2c3089b4f": "019-01",
    "8972aa5fd958f2d2": "095-02",
    "89957d872f26f703": "097-01",
    "8b479ca416d63f0f": "096-01",
    "933b3153ae621c2d": "034-01",
    "960c697b381d9bc3": "079-01",
    "962c616b164c4f54": "029-01",
    "96c1d3b371e34a72": "071-06",
    "97db7863d42cf271": "040-01",
    "9950de81e476a555": "055-08",
    "9ce7757350344bcb": "099-03",
    "a20827715249b41c": "082-02",
    "a4bfc71847b8f4bb": "095-04",
    "a74da73ad7593e94": "023-01",
    "a7a0110d675ba8a0": "050-01",
    "a91af60c829f7206": "055-08",
    "a999940822d671e0": "059-01",
    "b100dd881e2d7087": "023-02",
    "b25bc5ac6b4c1bfa": "038-03",
    "b360b70a66265527": "038-03",
    "b57bb9486052c2e4": "034-01",
    "b5e02b49328a2336": "059-02",
    "b696761eff2671b9": "050-01",
    "b9dde1cef70243d6": "099-04",
    "bbaa6fc4f74d7bd6": "045-14",
    "c113be47ddfddd9a": "096-01",
    "c190c9ae35f36635": "028-02",
    "c243973cf4492321": "034-01",
    "c5d6540908df8bfa": "096-01",
    "ca7ca849f0fea191": "050-99",
    "cab488502253a396": "083-01",
    "d1d139a11e93861d": "096-01",
    "d50155e1fea84354": "048-08",
    "d7ba8e900c845d86": "095-04",
    "d81eb3b4123428e2": "055-02",
    "da0624b3a8992b96": "085-02",
    "da7b334b9838890a": "049-10",
    "dfcc9a741cdc9abe": "048-08",
    "e0f8196635458ff3": "048-02",
    "e263ad0814e41d65": "074-13",
    "e6af3c3233554580": "096-99",
    "e960bb8f4744ce5a": "096-03",
    "eb1dc136d0774008": "040-01",
    "eb35ec074adbb6ea": "090-03",
    "eb972ead686d227c": "082-01",
    "edbd8081791ecbf5": "034-01",
    "f263c7e7722acc1e": "096-99",
    "f2d7c7c05c06c77f": "059-02",
    "f45c845f4bc44f60": "034-01",
    "fb0f62e8f23f12da": "096-01",
}


def key(no) -> str:
    return hashlib.sha256(re.sub(r"\D", "", str(no)).encode()).hexdigest()[:16]


bucket = storage.Client().bucket(hw.BUCKET_NAME)
dai_map = hw.load_shokugyo_dai(bucket)
months = sorted(b.name for b in bucket.list_blobs(prefix=hw.DATA_PREFIX + "/") if b.name.endswith(".parquet"))
total = 0
for p in months:
    df = hw.normalize(hw._read_parquet(bucket, p))
    keys = df["求人番号"].map(key)
    hit = df["職業分類_小分類コード"].isna() & keys.isin(CODES.keys())
    df.loc[hit, "職業分類_小分類コード"] = keys[hit].map(CODES)
    parents = [hw.shokugyo_parents(c, dai_map) for c in df.loc[hit, "職業分類_小分類コード"]]
    df.loc[hit, "職業分類_大分類コード"] = [x[0] for x in parents]
    df.loc[hit, "職業分類_中分類コード"] = [x[1] for x in parents]
    df = hw.normalize(df)
    total += int(hit.sum())
    print(f"{p}: {len(df)}件 / 今回書き足し {int(hit.sum())}件 / 小分類コードなし残り {int(df['職業分類_小分類コード'].isna().sum())}件"
          f" / 大分類コード欠け {int((df['職業分類_小分類コード'].notna() & df['職業分類_大分類コード'].isna()).sum())}件")
    if not DRY_RUN and hit.any():
        hw._write_parquet(bucket, p, df, schema=hw.SCHEMA)
        print(f"保存: {p}")
print(f"合計 書き足し {total}件（対応表 {len(CODES)}件）/ DRY_RUN={DRY_RUN}")
