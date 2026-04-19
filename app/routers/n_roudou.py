# 長野労働局 新規求人数エンドポイント

from fastapi           import APIRouter
from fastapi.responses import JSONResponse
from app.database      import get_stats
from datetime          import datetime
import math

router = APIRouter(prefix="/n_roudou", tags=["長野労働局"])


@router.get(
    "/juri_sangyo",
    summary="受理地別・産業別 新規求人数",
)
def n_roudou_juri_sangyo():
    """
    長野労働局の月次PDFから抽出した、産業大分類別の新規求人数。

    **産業コード一覧:**
    - `all` 合計
    - `D` 建設業 / `E` 製造業 / `G` 情報通信業
    - `H` 運輸・郵便業 / `I` 卸売・小売業 / `J` 金融・保険業
    - `K` 不動産業 / `M` 宿泊・飲食サービス / `N` 生活関連サービス
    - `O` 教育・学習支援 / `P` 医療・福祉 / `R` サービス業
    - `other` その他

    **レスポンスフィールド:**

    | フィールド | 型 | 内容 |
    |---|---|---|
    | DATE | string | 年月（YYYY-MM-DD形式） |
    | 産業コード | string | 産業の識別コード（上記参照） |
    | 産業分類 | string | 産業名称 |
    | 新規求人数 | int | 当月の新規求人数 |
    | 前月比 | float | 前月比（%、▲はマイナス） |
    | 前年同月比 | float | 前年同月比（%） |
    | うちパート | int | パート求人数 |
    | うちパート前月比 | float | パート前月比（%） |
    | うちパート前年同月比 | float | パート前年同月比（%） |
    """
    # GCSからParquetを読み込んで返す
    raw = get_stats("juri_sangyo", category="n_roudou")

    def clean(v):
        # NaN・inf はJSONシリアライズできないためNoneに変換する
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    data = [{k: clean(v) for k, v in row.items()} for row in raw]
    return {
        "collection": "juri_sangyo",
        "updated_at": str(datetime.now()),
        "count":      len(data),
        "data":       data,
    }
