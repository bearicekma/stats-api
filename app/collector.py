import httpx
import os
from app.processor import process_to_tidy
from app.database  import save_stats

async def collect_estat(app_id: str, stats_data_id: str, description: str, collection_name: str):
    url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
    params = {
        "appId":       app_id,
        "statsDataId": stats_data_id,
        "lang":        "J",
        "limit":       100,
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=30)

    response.raise_for_status()
    raw_json = response.json()
    values = raw_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
    raw_text = str(values[:20])

    print(f"🤖 Claude Haiku でデータを加工中: {description}")
    tidy_data = process_to_tidy(raw_text, description)

    for i, data in enumerate(tidy_data):
        save_stats(collection_name, str(i), data)

    print(f"✅ {collection_name}: {len(tidy_data)}件を保存しました")
    return len(tidy_data)


async def run_all_collections():
    app_id = os.environ["ESTAT_APP_ID"]

    targets = [
        {
            "stats_data_id":   "0003448237",
            "description":     "日本の年別人口推計",
            "collection_name": "population",
        },
        {
            "stats_data_id":   "0003427113",
            "description":     "消費者物価指数",
            "collection_name": "cpi",
        },
    ]

    total  = 0
    errors = []

    for target in targets:
        try:
            count = await collect_estat(app_id=app_id, **target)
            total += count
        except Exception as e:
            print(f"❌ エラー: {target['collection_name']}: {e}")
            errors.append(target["collection_name"])

    print(f"収集完了: 成功{total}件 / 失敗{len(errors)}件")
