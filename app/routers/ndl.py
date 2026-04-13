# 国立国会図書館サーチAPIエンドポイント
# /ndl/pass : 書誌情報をパススルーで取得しJSONに変換して返す

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
import httpx
import xmltodict
import os

router = APIRouter(prefix="/ndl", tags=["ndl"])

# 国立国会図書館サーチ OpenSearch API のベースURL
NDL_BASE_URL = "https://ndlsearch.ndl.go.jp/api/opensearch"

# 1リクエストあたりの最大取得件数（API上限は500件）
NDL_MAX_CNT = 200


def parse_item(item: dict) -> dict:
    # RSS item要素を整形してシンプルなdictに変換する
    # 複数形式で返ってくるidentifier（ISBN等）を整理する
    identifiers = item.get("dc:identifier", [])
    if isinstance(identifiers, str):
        identifiers = [{"#text": identifiers, "@xsi:type": ""}]
    elif isinstance(identifiers, dict):
        identifiers = [identifiers]

    # ISBNだけを抽出する
    isbn = next(
        (i["#text"] for i in identifiers if "ISBN" in i.get("@xsi:type", "")),
        None
    )

    # 著者が複数の場合はリストで返ってくるため文字列に統一する
    creator = item.get("dc:creator", "")
    if isinstance(creator, list):
        creator = " / ".join(creator)

    # 出版年月日を取得する（dc:dateは辞書形式の場合がある）
    date = item.get("dc:date", "")
    if isinstance(date, dict):
        date = date.get("#text", "")

    # カテゴリが複数の場合はリストで返ってくるため文字列に統一する
    category = item.get("category", "")
    if isinstance(category, list):
        category = ", ".join(category)

    return {
        "title":     item.get("title", ""),
        "creator":   creator,
        "publisher": item.get("dc:publisher", ""),
        "date":      date,
        "isbn":      isbn,
        "category":  category,
        "link":      item.get("link", ""),
    }


@router.get("/pass")
async def ndl_pass(request: Request):
    # 国立国会図書館サーチAPIへのパススルー（XML→JSON変換）
    # クエリパラメータをそのまま転送する汎用設計
    #
    # 主なパラメータ：
    #   title     : タイトル検索
    #   creator   : 著者検索
    #   publisher : 出版社検索
    #   isbn      : ISBN検索
    #   from      : 開始出版日（YYYY-MM-DD 形式）
    #   until     : 終了出版日（YYYY-MM-DD 形式）
    #   dpid      : データプロバイダID（jpro=近刊含む）
    #   mediatype : 資料種別（1=図書のみ）
    #   cnt       : 取得件数（デフォルト200、最大500）
    #
    # 例: /ndl/pass?from=2026-04-01&until=2026-04-30&mediatype=1&dpid=jpro
    # 例: /ndl/pass?title=Python&from=2026-01-01&mediatype=1

    params = dict(request.query_params)

    # 最低1つの検索条件が必要
    search_keys = {"title", "creator", "publisher", "isbn", "from", "until", "any", "dpid"}
    if not search_keys.intersection(params.keys()):
        return JSONResponse(
            status_code=400,
            content={
                "error":   "検索条件が指定されていません",
                "hint":    "title / creator / publisher / isbn / from / until のいずれかを指定してください",
                "example": "/ndl/pass?from=2026-04-01&until=2026-04-30&mediatype=1&dpid=jpro",
            }
        )

    # 取得件数のデフォルト値を設定する
    params.setdefault("cnt", str(NDL_MAX_CNT))

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(NDL_BASE_URL, params=params)
    res.raise_for_status()

    # XMLをPython辞書に変換する
    raw = xmltodict.parse(res.text)
    channel = raw.get("rss", {}).get("channel", {})

    # 総件数・取得件数を取得する
    total_results = int(channel.get("openSearch:totalResults", 0))
    items_per_page = int(channel.get("openSearch:itemsPerPage", 0))

    # 検索結果のitemsを取得する（0件・1件・複数件の場合を統一する）
    items = channel.get("item", [])
    if isinstance(items, dict):
        # 1件のみの場合はdictで返ってくるのでリストに変換する
        items = [items]
    elif items is None:
        items = []

    # 各itemを整形してリスト化する
    data = [parse_item(item) for item in items]

    return {
        "total_results":  total_results,
        "items_per_page": items_per_page,
        "count":          len(data),
        "data":           data,
    }
