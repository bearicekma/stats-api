
from fastapi        import FastAPI, BackgroundTasks
from datetime       import datetime
from app.database   import get_stats
from app.collector  import run_all_collections
from app.routers    import estat, boj
from app.mcp_server import mcp

app = FastAPI(title="Stats API")

# ルーターを登録する
app.include_router(estat.router)
app.include_router(boj.router)

# MCPサーバーをStreamable HTTPでマウントする
# 接続URL: https://stats-api-491107.web.app/mcp
app.mount("/mcp", mcp.streamable_http_app())


@app.get("/")
def root():
    return {"status": "ok", "timestamp": str(datetime.now())}


@app.get("/master/{collection_name}")
def get_master(collection_name: str):
    data = get_stats(collection_name, category="master")
    return {"collection": collection_name, "updated_at": str(datetime.now()), "count": len(data), "data": data}


@app.get("/stats/{collection_name}")
def get_collection(collection_name: str):
    data = get_stats(collection_name)
    return {"collection": collection_name, "updated_at": str(datetime.now()), "count": len(data), "data": data}


@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
