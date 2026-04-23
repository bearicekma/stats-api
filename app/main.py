# Stats API メインファイル
# ルーターの登録と共通エンドポイントのみを記載する

from fastapi           import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from pathlib           import Path
from datetime          import datetime
from app.database      import get_stats
from app.collector     import (
    run_all_collections,
    run_d_kanko_collection,
    run_n_roudou_collection,
)
from app.routers       import estat, boj, eia, ndl, fred, d_kanko, n_roudou

app = FastAPI(title="Stats API")

app.include_router(d_kanko.router)
app.include_router(estat.router)
app.include_router(boj.router)
app.include_router(eia.router)
app.include_router(ndl.router)
app.include_router(fred.router)
app.include_router(n_roudou.router)

GUIDE_HTML = Path(__file__).parent / "templates" / "guide.html"

COLLECTION_TARGETS = {
    "d_kanko":  run_d_kanko_collection,
    "n_roudou": run_n_roudou_collection,
}


@app.get("/")
def root():
    return {"status": "ok", "timestamp": str(datetime.now())}


@app.get("/guide", response_class=HTMLResponse)
def guide():
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
    if target is None:
        background_tasks.add_task(run_all_collections)
        return {"message": "全データソースの収集を開始しました", "timestamp": str(datetime.now())}

    func = COLLECTION_TARGETS.get(target)
    if func is None:
        return JSONResponse(status_code=400, content={
            "error": f"不正なtarget値: {target}",
            "hint":  f"有効な値: {', '.join(COLLECTION_TARGETS.keys())}",
        })

    background_tasks.add_task(func)
    return {"message": f"{target} の収集を開始しました", "timestamp": str(datetime.now())}


@app.get("/enecho/test")
async def enecho_test():
    # 資源エネルギー庁サーバーへの疎通確認（テスト用・確認後削除）
    import httpx
    import traceback
    from datetime import datetime, timedelta, timezone

    JST = timezone(timedelta(hours=9))
    today = datetime.now(JST).date()
    days_since_wed = (today.weekday() - 2) % 7
    latest_wed = today - timedelta(days=days_since_wed)
    reiwa_year = latest_wed.year - 2018
    yy   = str(reiwa_year).zfill(2)
    mmdd = latest_wed.strftime("%m%d")
    url  = f"https://www.enecho.meti.go.jp/statistics/petroleum_and_lpgas/pl007/xlsx/{yy}{mmdd}s5.xlsx"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; stats-api/1.0)",
            "Referer": "https://www.enecho.meti.go.jp/statistics/petroleum_and_lpgas/pl007/results.html",
        }
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            res = await client.get(url, headers=headers)
        return {
            "url":            url,
            "status_code":    res.status_code,
            "content_type":   res.headers.get("content-type"),
            "content_length": len(res.content),
            "ok":             res.status_code == 200,
        }
    except Exception as e:
        return {
            "ok":         False,
            "error_type": type(e).__name__,
            "error":      str(e),
            "traceback":  traceback.format_exc(),
            "url":        url,
        }
