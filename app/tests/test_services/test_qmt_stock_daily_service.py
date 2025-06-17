import unittest
from unittest.mock import patch
from sqlmodel import Session, SQLModel, create_engine

from app.services.qmt_stock_daily_service import sync_stock_daily_klines_to_db
from app.core.config import settings
from utils.quant_logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

class TestQmtStockDailyService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mysql_url = settings.SQLALCHEMY_MYSQL_DATABASE_URI
        assert mysql_url, "请在配置文件中正确设置MySQL连接串"
        cls.engine = create_engine(mysql_url, echo=True)
        SQLModel.metadata.create_all(cls.engine)

    def setUp(self):
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    @patch('app.services.qmt_stock_daily_service.xtdata')
    def test_sync_stock_daily_klines_to_db(self, mock_xtdata):
        """测试同步日K数据到数据库的服务"""
        # 模拟 xtdata.get_market_data 的返回值
        mock_data = {
            'time': [1622476800, 1622563200],  # 2021-06-01, 2021-06-02
            'open': [10.0, 10.5],
            'high': [11.0, 11.5],
            'low': [9.0, 9.5],
            'close': [10.5, 11.0],
            'volume': [1000000, 1100000],
            'amount': [10500000.0, 11500000.0]
        }
        mock_xtdata.get_market_data.return_value = mock_data

        # 执行同步操作
        result = sync_stock_daily_klines_to_db(
            db=self.session,
            stock_code="000001.SZ",
            start_sync_time="2021-06-01",
            end_sync_time="2021-06-02"
        )

        # 验证数据同步结果
        self.assertEqual(result, 2, "应该同步了2条数据")

        # 验证 xtdata.get_market_data 被正确调用
        mock_xtdata.get_market_data.assert_called_once_with(
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=["000001.SZ"],
            start_time="2021-06-01",
            end_time="2021-06-02",
            period='1d'
        )

        logger.info("日K数据同步服务测试完成")

if __name__ == '__main__':
    unittest.main()
