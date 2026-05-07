# 長野労働局 新規求人数エンドポイント

from fastapi           import APIRouter
from app.database      import get_stats
from datetime          import datetime

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

    <table>
    <tr><th>フィールド</th><th>型</th><th>内容</th></tr>
    <tr><td>DATE</td><td>string</td><td>年月（YYYY-MM-DD形式）</td></tr>
    <tr><td>産業コード</td><td>string</td><td>産業の識別コード（上記参照）</td></tr>
    <tr><td>産業分類</td><td>string</td><td>産業名称</td></tr>
    <tr><td>新規求人数</td><td>int</td><td>当月の新規求人数</td></tr>
    <tr><td>前月比</td><td>float</td><td>前月比（%、▲はマイナス）</td></tr>
    <tr><td>前年同月比</td><td>float</td><td>前年同月比（%）</td></tr>
    <tr><td>うちパート</td><td>int</td><td>パート求人数</td></tr>
    <tr><td>うちパート前月比</td><td>float</td><td>パート前月比（%）</td></tr>
    <tr><td>うちパート前年同月比</td><td>float</td><td>パート前年同月比（%）</td></tr>
    </table>
    """
    # GCSからParquetを読み込んで返す
    data = get_stats("juri_sangyo", category="n_roudou")
    return {
        "collection": "juri_sangyo",
        "updated_at": str(datetime.now()),
        "count":      len(data),
        "data":       data,
    }
