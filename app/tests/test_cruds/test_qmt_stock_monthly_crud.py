import unittest
from datetime import datetime, timedelta
from sqlmodel import Session, SQLModel, create_engine
from sqlalchemy import text

from app.models.qmt_stock_monthly import QmtStockMonthlyOri
from app.cruds.qmt_stock_monthly_crud import (
    create_monthly_klines,
    delete_monthly_klines_by_stock_code,
    get_monthly_klines_by_stock_code_and_date_range
)
from app.core.config import settings

"""
本测试文件用于测试股票月K线数据的CRUD操作。
测试流程如下：
1. 连接真实MySQL数据库
2. 自动建表（如未用Alembic迁移）
3. 测试批量创建月K数据
4. 测试按股票代码和时间范围查询
5. 测试按股票代码删除数据
每一步均有详细中文注释
"""

class TestQmtStockMonthlyCrud(unittest.TestCase):
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
        self.session.exec(text(f"DELETE FROM {QmtStockMonthlyOri.__tablename__} WHERE stock_code = '000001.SZ'"))
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_create_and_query_and_delete_monthly_klines(self):
        """测试月K线数据的创建、查询和删除操作"""
        stock_code = "000001.SZ"
        now = datetime.now()

        # 准备测试数据：创建3个月的K线数据
        test_data = []
        for i in range(3):
            test_data.append({
                "stock_code": stock_code,
                "time": now - timedelta(days=30*i),
                "open": 10.0 + i,
                "high": 11.0 + i,
                "low": 9.0 + i,
                "close": 10.5 + i,
                "volume": 1000000 + i * 100000,
                "amount": 10500000.0 + i * 1000000
            })

        # 1. 测试批量创建
        created_klines = create_monthly_klines(session=self.session, kline_list=test_data)
        self.assertEqual(len(created_klines), 3, "应该成功创建3条月K线数据")
        print(f"成功创建{len(created_klines)}条月K数据")

        # 2. 测试按时间范围查询
        start_time = now - timedelta(days=60)  # 两个月前
        end_time = now
        queried_klines = get_monthly_klines_by_stock_code_and_date_range(
            session=self.session,
            stock_code=stock_code,
            start_time=start_time,
            end_time=end_time
        )
        self.assertEqual(len(queried_klines), 2, "应该查询到2条月K线数据")
        print(f"查询到{len(queried_klines)}条符合时间范围的数据")

        # 3. 测试删除操作
        delete_monthly_klines_by_stock_code(session=self.session, stock_code=stock_code)

        # 4. 验证删除结果
        remaining_klines = get_monthly_klines_by_stock_code_and_date_range(
            session=self.session,
            stock_code=stock_code,
            start_time=start_time,
            end_time=end_time
        )
        self.assertEqual(len(remaining_klines), 0, "删除后应该没有数据")
        print("成功删除所有测试数据")

if __name__ == '__main__':
    unittest.main()
