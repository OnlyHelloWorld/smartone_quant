import unittest
from datetime import datetime, timedelta
from sqlmodel import Session, SQLModel, create_engine
from sqlalchemy import text

from app.models.qmt_stock_weekly import QmtStockWeeklyOri
from app.cruds.qmt_stock_weekly_crud import (
    create_weekly_klines,
    delete_weekly_klines_by_stock_code,
    get_weekly_klines_by_stock_code_and_date_range
)
from app.core.config import settings
from utils.quant_logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

"""
本测试文件用于测试股票周K线数据的CRUD操作。
测试流程如下：
1. 连接真实MySQL数据库
2. 自动建表（如未用Alembic迁移）
3. 测试批量创建周K数据
4. 测试按股票代码和时间范围查询
5. 测试按股票代码删除数据
每一步均有详细中文注释
"""

class TestQmtStockWeeklyCrud(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 连接真实MySQL数据库
        mysql_url = settings.SQLALCHEMY_MYSQL_DATABASE_URI
        assert mysql_url, "请在配置文件中正确设置MySQL连接串"
        cls.engine = create_engine(mysql_url, echo=True)
        # 自动建表（如未用Alembic迁移）
        SQLModel.metadata.create_all(cls.engine)

    def setUp(self):
        self.session = Session(self.engine)
        # 每次测试前清理测试数据
        self.session.exec(text(f"DELETE FROM {QmtStockWeeklyOri.__tablename__} WHERE stock_code = '000001.SZ'"))
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_create_and_query_and_delete_weekly_klines(self):
        """测试周K线数据的创建、查询和删除操作"""
        stock_code = "000001.SZ"
        now = datetime.now()

        # 准备测试数据：创建4周的K线数据
        test_data = []
        for i in range(4):
            test_data.append({
                "stock_code": stock_code,
                "time": now - timedelta(weeks=i),
                "open": 10.0 + i,
                "high": 11.0 + i,
                "low": 9.0 + i,
                "close": 10.5 + i,
                "volume": 1000000 + i * 100000,
                "amount": 10500000.0 + i * 1000000
            })

        # 1. 测试批量创建
        created_klines = create_weekly_klines(session=self.session, kline_list=test_data)
        self.assertEqual(len(created_klines), 4, "应该成功创建4条周K线数据")
        logger.info(f"成功创建{len(created_klines)}条周K数据")

        # 2. 测试按时间范围查询
        start_time = now - timedelta(weeks=2)
        end_time = now
        queried_klines = get_weekly_klines_by_stock_code_and_date_range(
            session=self.session,
            stock_code=stock_code,
            start_time=start_time,
            end_time=end_time
        )
        self.assertEqual(len(queried_klines), 3, "应该查询到3条周K线数据")
        logger.info(f"查询到{len(queried_klines)}条符合时间范围的数据")

        # 3. 测试删除操作
        delete_weekly_klines_by_stock_code(session=self.session, stock_code=stock_code)

        # 4. 验证删除结果
        remaining_klines = get_weekly_klines_by_stock_code_and_date_range(
            session=self.session,
            stock_code=stock_code,
            start_time=start_time,
            end_time=end_time
        )
        self.assertEqual(len(remaining_klines), 0, "删除后应该没有数据")
        logger.info("成功删除所有测试数据")

if __name__ == '__main__':
    unittest.main()
