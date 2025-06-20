import unittest

from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine

from app.core.config import settings
from app.cruds.qmt_sector_stock_crud import create_qmt_sector_stock, update_qmt_sector_stock, \
    get_qmt_sector_stock_by_sector_and_code
from app.models.qmt_sector_stock import QmtSectorStock
from utils.quant_logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

"""
本测试文件用于测试QmtSectorStock成分股相关的CRUD操作。
测试流程如下：
1. 连接真实MySQL数据库。
2. 自动建表（如未用Alembic迁移）。
3. 测试创建成分股(create_qmt_sector_stock)。
4. 测试通过sector_id+stock_code查询(get_qmt_sector_stock_by_sector_and_code)。
5. 测试更新成分股(update_qmt_sector_stock)。
每一步均有详细中文注释。
"""

class TestQmtSectorStockCrud(unittest.TestCase):
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
        # 每次测试前清理测试用成分股，避免主键冲突
        self.session.exec(text(f"DELETE FROM {QmtSectorStock.__tablename__} WHERE stock_code in ('600000', '600001') AND sector_id=1"))
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_create_and_get_and_update_qmt_sector_stock(self):
        """
        测试流程：
        1. 创建成分股
        2. 通过sector_id+stock_code查询
        3. 更新股票代码
        4. 再次通过新代码查询
        5. 旧代码应查不到
        """
        # 1. 创建成分股
        stock = QmtSectorStock(sector_id=1, stock_code="600000")
        created_stock = create_qmt_sector_stock(session=self.session, qmt_sector_stock_create=stock)
        logger.info(f"创建的成分股ID: {created_stock.id}, 板块ID: {created_stock.sector_id}, 股票代码: {created_stock.stock_code}")
        self.assertIsNotNone(created_stock.id, "创建后应有ID")
        self.assertEqual(created_stock.stock_code, "600000")

        # 2. 通过sector_id+stock_code查询
        found_stock = get_qmt_sector_stock_by_sector_and_code(session=self.session, sector_id=1, stock_code="600000")
        logger.info(f"查询到的成分股ID: {found_stock.id}, 股票代码: {found_stock.stock_code}")
        self.assertIsNotNone(found_stock, "应能查到成分股")
        self.assertEqual(found_stock.stock_code, "600000")

        # 3. 更新股票代码
        updated_stock = QmtSectorStock(sector_id=1, stock_code="600001")
        logger.info(f"更新前的成分股ID: {found_stock.id}, 旧代码: {found_stock.stock_code}")
        update_qmt_sector_stock(session=self.session, db_qmt_sector_stock=found_stock, qmt_sector_stock_in=updated_stock)

        # 4. 通过新代码查询
        found_new = get_qmt_sector_stock_by_sector_and_code(session=self.session, sector_id=1, stock_code="600001")
        logger.info(f"更新后的成分股ID: {found_new.id}, 新代码: {found_new.stock_code}")
        self.assertIsNotNone(found_new, "更新后应能查到新代码成分股")
        self.assertEqual(found_new.stock_code, "600001")

        # 5. 旧代码应查不到
        found_old = get_qmt_sector_stock_by_sector_and_code(session=self.session, sector_id=1, stock_code="600000")
        logger.info(f"查询旧代码成分股: {found_old}")
        self.assertIsNone(found_old, "旧代码应查不到")

if __name__ == '__main__':
    unittest.main()
