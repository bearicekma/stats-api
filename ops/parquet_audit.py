# 目的：GCS上の全Parquetの圧縮方式・サイズを一覧にし、ZSTDにした場合のサイズを試算する
# 内容：読み取りのみ（GCSへの書き込みなし）。各ファイルをメモリに読み、ZSTDで再エンコードしたサイズをメモリ上で測るだけ
#       出力はパス・サイズ・行数などの集計のみ
import io, os, collections
import pyarrow.parquet as pq
from google.cloud import storage

bucket = storage.Client().bucket(os.environ["GCS_BUCKET_NAME"])
blobs = list(bucket.list_blobs())

# バケット全体の内訳（拡張子別）
ext = collections.Counter(); ext_n = collections.Counter()
for b in blobs:
    e = b.name.rsplit(".", 1)[-1] if "." in b.name.rsplit("/", 1)[-1] else "(なし)"
    ext[e] += b.size; ext_n[e] += 1
print("== バケット全体（拡張子別） ==")
for e, s in ext.most_common():
    print(f"{e:10s} {ext_n[e]:5d} files {s/1e6:10.2f} MB")
print(f"{'合計':10s} {len(blobs):5d} files {sum(ext.values())/1e6:10.2f} MB")

rows = []
for b in sorted([b for b in blobs if b.name.endswith(".parquet")], key=lambda x: -x.size):
    try:
        data = bucket.blob(b.name).download_as_bytes()   # 世代指定なし＝最新版を読む（バックフィルが並行して上書きしているため）
    except Exception as ex:
        print(f"skip {b.name}: {type(ex).__name__}"); continue
    pf = pq.ParquetFile(io.BytesIO(data)); md = pf.metadata
    cc = [md.row_group(i).column(j) for i in range(md.num_row_groups) for j in range(md.num_columns)]
    tbl = pf.read()
    buf = io.BytesIO(); pq.write_table(tbl, buf, compression="zstd"); z3 = buf.tell()
    buf = io.BytesIO(); pq.write_table(tbl, buf, compression="zstd", compression_level=9); z9 = buf.tell()
    rows.append(dict(path=b.name, MB=b.size/1e6, codec=",".join(sorted({c.compression for c in cc})),
                     rows=md.num_rows, rg=md.num_row_groups, cols=md.num_columns,
                     raw_MB=sum(c.total_uncompressed_size for c in cc)/1e6,
                     z3_MB=z3/1e6, z9_MB=z9/1e6, by=(md.created_by or "")[:30]))

print("\n== Parquet一覧（サイズ順） ==")
print(f"{'MB':>8} {'zstd3':>8} {'zstd9':>8} {'非圧縮':>8} {'rows':>9} {'rg':>4} {'cols':>4}  codec        path")
for r in rows:
    print(f"{r['MB']:8.2f} {r['z3_MB']:8.2f} {r['z9_MB']:8.2f} {r['raw_MB']:8.2f} {r['rows']:9d} {r['rg']:4d} {r['cols']:4d}  {r['codec']:12s} {r['path']}")

print("\n== コーデック別 ==")
agg = collections.defaultdict(lambda: [0, 0.0, 0.0, 0.0])
for r in rows:
    a = agg[r["codec"]]; a[0] += 1; a[1] += r["MB"]; a[2] += r["z3_MB"]; a[3] += r["z9_MB"]
for k, (n, s, z3, z9) in agg.items():
    print(f"{k:12s} {n:5d} files  現在 {s:8.2f} MB  → zstd3 {z3:8.2f} MB / zstd9 {z9:8.2f} MB")
t = [sum(r[k] for r in rows) for k in ("MB", "z3_MB", "z9_MB")]
print(f"{'合計':12s} {len(rows):5d} files  現在 {t[0]:8.2f} MB  → zstd3 {t[1]:8.2f} MB / zstd9 {t[2]:8.2f} MB")
print("\ncreated_by:", dict(collections.Counter(r["by"] for r in rows)))
