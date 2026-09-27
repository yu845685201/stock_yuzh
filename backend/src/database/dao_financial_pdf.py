"""
sync_financial_pdf_progress DAO（财报 PDF 采集进度表，自研小表不入 init.sql 主 DDL 链）

表内只记录采集进度（股票编码 + 最后财报期），财报 PDF 本体落文件系统，不落数据库。
"""

from datetime import date
from typing import Dict, List, Optional, Tuple


class FinancialPdfDaoMixin:
    """财报 PDF 采集进度 DAO"""

    def ensure_financial_pdf_progress_table(self) -> None:
        """建表兜底（IF NOT EXISTS，幂等；DDL 同步维护在 doc/init-V2.sql）"""
        sql = """
        CREATE TABLE IF NOT EXISTS sync_financial_pdf_progress (
            stock_code     VARCHAR(16) PRIMARY KEY,
            last_stat_date DATE NOT NULL,
            updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                conn.commit()

    def fetch_financial_pdf_progress(self, stock_codes: Optional[List[str]] = None) -> Dict[str, date]:
        """读取采集进度：stock_code -> last_stat_date（最后财报期）"""
        query = "SELECT stock_code, last_stat_date FROM sync_financial_pdf_progress"
        params = None
        if stock_codes:
            query += " WHERE stock_code = ANY(%s)"
            params = (stock_codes,)
        rows = self.execute_query(query, params)
        return {r['stock_code']: r['last_stat_date'] for r in rows}

    def upsert_financial_pdf_progress(self, rows: List[Tuple[str, date]]) -> int:
        """批量 upsert 进度（只前进不回退由调用方保证：last_stat_date 取成功落盘的最大报告期）"""
        if not rows:
            return 0
        sql = """
        INSERT INTO sync_financial_pdf_progress (stock_code, last_stat_date, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (stock_code)
        DO UPDATE SET last_stat_date = EXCLUDED.last_stat_date, updated_at = NOW()
        """
        return self.execute_batch(sql, rows)
