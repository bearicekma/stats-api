# Stats API メインファイル

from fastapi           import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse, ORJSONResponse
from pathlib           import Path
from datetime          import datetime
from app.database      import get_stats
from app.collector     import (
    run_all_collections,
    run_d_kanko_collection,
    run_n_roudou_collection,
    run_enecho_collection,
    run_jma_collection,
)
from app.routers import estat, boj, eia, ndl, fred, d_kanko, n_roudou, enecho, jma, edinet

app = FastAPI(title="Stats API", default_response_class=ORJSONResponse)

# 個別ルーターを先に登録する（/stats/{collection_name}との競合を回避）
app.include_router(d_kanko.router)

# 各データソースのルーターを登録する
app.include_router(estat.router)
app.include_router(boj.router)
app.include_router(eia.router)
app.include_router(ndl.router)
app.include_router(fred.router)
app.include_router(n_roudou.router)
app.include_router(enecho.router)
app.include_router(jma.router)
app.include_router(edinet.router)

GUIDE_HTML = Path(__file__).parent / "templates" / "guide.html"

COLLECTION_TARGETS = {
    "d_kanko":         run_d_kanko_collection,
    "n_roudou":        run_n_roudou_collection,
    "enecho_gasoline": run_enecho_collection,
    "jma_nagano":      run_jma_collection,
}


@app.get("/")
def root():
    # 死活確認エンドポイント
    return {"status": "ok", "timestamp": str(datetime.now())}


@app.get("/guide", response_class=HTMLResponse)
def guide():
    # 利用者向けAPIガイドページを返す
    return HTMLResponse(GUIDE_HTML.read_text(encoding="utf-8"))


@app.get("/master/{collection_name}")
def get_master(collection_name: str):
    data = get_stats(collection_name, category="master")
    return {"collection": collection_name, "updated_at": str(datetime.now()), "count": len(data), "data": data}


@app.get("/stats/{collection_name}")
def get_collection(collection_name: str):
    data = get_stats(collection_name)
    return {"collection": collection_name, "updated_at": str(datetime.now()), "count": len(data), "data": data}


@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks, target: str = None):
    # データ収集をバックグラウンドでトリガーする
    if target is None:
        background_tasks.add_task(run_all_collections)
        return {"message": "全データソースの収集を開始しました", "timestamp": str(datetime.now())}

    func = COLLECTION_TARGETS.get(target)
    if func is None:
        return JSONResponse(status_code=400, content={
            "error":   f"不正なtarget値: {target}",
            "hint":    f"有効な値: {', '.join(COLLECTION_TARGETS.keys())}",
            "example": "/collect?target=jma_nagano",
        })

    background_tasks.add_task(func)
    return {"message": f"{target} の収集を開始しました", "timestamp": str(datetime.now())}
