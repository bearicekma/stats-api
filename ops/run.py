# 目的：Gビズインフォ「データダウンロード」（一括CSV）のフォームの仕組みを確認する（法人マスタへの属性付与の設計用）
# 内容：読み取りのみ。ダウンロードページのHTMLから form・input・select・ボタン・スクリプト内のURLを集計して出力する
#       トークンは送らない・出力しない。GCS には書き込まない

import re

import httpx
from bs4 import BeautifulSoup

URL = "https://info.gbiz.go.jp/hojin/DownloadTop"
c = httpx.Client(timeout=60, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0 (stats-api ops)"})
r = c.get(URL)
print("status:", r.status_code, "final url:", re.sub(r";jsessionid=[^?]*", ";jsessionid=…", str(r.url)), "len:", len(r.text))
soup = BeautifulSoup(r.text, "html.parser")

for i, f in enumerate(soup.find_all("form")):
    print(f"\n[form {i}] action={re.sub(r';jsessionid=[^?]*', ';jsessionid=…', f.get('action') or '')} method={f.get('method')} id={f.get('id')} name={f.get('name')}")
    for el in f.find_all(["input", "select", "button", "textarea"]):
        attrs = {k: (v if k != "value" or el.get("type") != "hidden" or len(str(v)) < 40 else str(v)[:8] + "…") for k, v in el.attrs.items() if k in ("type", "name", "id", "value", "onclick", "class", "data-id", "data-type")}
        lab = ""
        if el.get("id"):
            l = soup.find("label", attrs={"for": el["id"]})
            lab = l.get_text(" ", strip=True) if l else ""
        if not lab:
            p = el.find_parent(["label", "td", "li", "div"])
            lab = p.get_text(" ", strip=True)[:40] if p else ""
        print(f"   <{el.name}> {attrs} 「{lab}」")
        if el.name == "select":
            for o in el.find_all("option"):
                print(f"       option value={o.get('value')!r} 「{o.get_text(strip=True)}」")

# フォーム外のリンク・ボタン（ダウンロード関連）
print("\n[ダウンロード関連の a / button（フォーム外も含む）]")
for el in soup.find_all(["a", "button"]):
    t = el.get_text(" ", strip=True)
    href = el.get("href") or ""
    if any(w in (t + href + str(el.get("onclick", ""))) for w in ("ダウンロード", "download", "Download", "csv", "zip")):
        print(f"   <{el.name}> text={t[:30]!r} href={re.sub(r';jsessionid=[^?]*', ';jsessionid=…', href)[:120]!r} onclick={str(el.get('onclick', ''))[:120]!r} id={el.get('id')}")

# スクリプト内のURL・関数（ダウンロード処理を探す）
print("\n[script]")
for s in soup.find_all("script"):
    src = s.get("src")
    if src:
        print("   src:", src)
    txt = s.string or ""
    for m in re.findall(r"""['"](/[^'"]*(?:[Dd]ownload|api|csv|zip)[^'"]*)['"]""", txt):
        print("   url in script:", m[:150])
    for m in re.findall(r"function\s+(\w*[Dd]ownload\w*)", txt):
        print("   function:", m)
    if "ajax" in txt or "fetch(" in txt:
        for line in txt.splitlines():
            if any(w in line for w in ("url", "ajax", "fetch(", "action", "token", "Token")):
                print("   >", line.strip()[:160])

# 外部JSの中も確認（同一サイトのもののみ）
for s in soup.find_all("script", src=True):
    src = s["src"]
    if "gbiz" in src or src.startswith("/"):
        u = src if src.startswith("http") else "https://info.gbiz.go.jp" + src
        try:
            js = c.get(u).text
        except Exception as e:
            print("   取得失敗", u, e)
            continue
        hits = set(re.findall(r"""['"`](/[^'"`\s]*(?:[Dd]ownload|api|csv|zip)[^'"`\s]*)['"`]""", js))
        if hits:
            print(f"   {u.split('?')[0][-60:]}: {sorted(hits)[:20]}")
        for line in js.splitlines():
            if "token" in line.lower() and ("header" in line.lower() or "data" in line.lower()):
                print("     >", line.strip()[:160])
print("\n完了")
