# 株価エンドポイント
# GET /kabuka : Yahoo Finance (yfinance) パススルー
# ORJSONResponseがデフォルトのためNaN→null変換は自動で行われる

from fastapi           import APIRouter, Query
from fastapi.responses import JSONResponse
from datetime          import date, datetime
from typing            import Optional
import yfinance as yf

router = APIRouter(prefix="/kabuka", tags=["kabuka"])

# yfinance列名 → 日本語列名のマッピング
COL_MAP = {
    "Open":   "始値",
    "High":   "高値",
    "Low":    "安値",
    "Close":  "終値",
    "Volume": "出来高",
}


@router.get("", summary="株価・指数データ取得")
async def get_kabuka(
    ticker:   str           = Query("^N225", description="yfinanceティッカー（例: ^N225, 7203.T）"),
    from_:    Optional[str] = Query(None,    alias="from", description="開始日 YYYY-MM-DD（省略時=全期間）"),
    to:       Optional[str] = Query(None,    description="終了日 YYYY-MM-DD（省略時=今日）"),
    interval: str           = Query("1d",    description="足種: 1d / 1wk / 1mo"),
):
    """
    株価・指数データを取得します（Yahoo Finance パススルー）。
    - `ticker` (str) ティッカーシンボル（yfinance形式）、省略時は ^N225
    - `from` (str) 開始日 YYYY-MM-DD、省略時は全期間
    - `to` (str) 終了日 YYYY-MM-DD、省略時は今日
    - `interval` (str) 足種: 1d（日次）/ 1wk（週次）/ 1mo（月次）
    """
    # intervalの許容値を確認する
    if interval not in ("1d", "1wk", "1mo"):
        return JSONResponse(
            status_code=400,
            content={"error": "intervalは 1d / 1wk / 1mo のいずれかを指定してください"},
        )

    try:
        t = yf.Ticker(ticker)

        # from指定あり→start/end方式、なし→period="max"で全期間取得
        if from_:
            hist = t.history(
                start       = from_,
                end         = to or str(date.today()),
                interval    = interval,
                auto_adjust = True,
            )
        else:
            hist = t.history(
                period      = "max",
                end         = to or str(date.today()),
                interval    = interval,
                auto_adjust = True,
            )

        if hist.empty:
            return {"ticker": ticker, "count": 0, "data": []}

        # Open/High/Low/Close/Volumeのみ取り出して日本語列名にリネームする
        hist = hist[["Open", "High", "Low", "Close", "Volume"]].rename(columns=COL_MAP)

        # インデックス（DatetimeIndex）をYYYY-MM-DD文字列に変換して列として追加する
        # タイムゾーン情報を除去してからstrftimeする
        hist.index = hist.index.tz_localize(None) if hist.index.tz else hist.index
        hist.index = hist.index.strftime("%Y-%m-%d")
        hist.index.name = "日付"
        hist = hist.reset_index()

        records = hist.to_dict(orient="records")

        return {
            "ticker":     ticker,
            "count":      len(records),
            "updated_at": str(datetime.now()),
            "data":       records,
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"データ取得失敗: {type(e).__name__}: {str(e)}"},
        )
