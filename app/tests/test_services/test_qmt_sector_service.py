import unittest
from unittest.mock import patch

from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine

from app.core.config import settings
from app.models.qmt_sector import QmtSector
from app.services.qmt_sector_service import sync_sector_list_to_db

"""
本测试文件用于测试QmtSectorService服务层的板块同步功能。
测试流程如下：
1. 连接真实MySQL数据库。
2. 自动建表（如未用Alembic迁移）。
3. 测试sync_sector_list_to_db方法。
每一步均有详细中文注释。
"""

class TestQmtSectorService(unittest.TestCase):
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
        # 每次测试前清理测试用板块，避免主键冲突
        self.session.exec(text(f"DELETE FROM {QmtSector.__tablename__} WHERE sector_name in ('测试板块1', '测试板块2')"))
        self.session.commit()

    def tearDown(self):
        self.session.close()

    @patch('app.services.qmt_sector_service.xtdata')
    def test_sync_sector_list_to_db(self, mock_xtdata):
        """
        测试sync_sector_list_to_db方法：
        1. 模拟xtdata.get_sector_list返回测试板块列表
        2. 调用服务方法同步到数据库
        3. 校验数据库中是否插入了对应板块
        """
        # 模拟返回测试数据
        mock_xtdata.get_sector_list.return_value = ['测试板块1', '测试板块2']

        # 执行同步方法
        inserted = sync_sector_list_to_db(self.session)

        # 验证结果
        self.assertEqual(len(inserted), 2)
        names = [s.sector_name for s in inserted]
        self.assertIn('测试板块1', names)
        self.assertIn('测试板块2', names)

    @patch('app.services.qmt_sector_service.xtdata')
    def test_sync_sector_list_to_db_empty(self, mock_xtdata):
        """
        测试sync_sector_list_to_db方法处理空列表的情况：
        1. 模拟xtdata.get_sector_list返回空列表
        2. 调用服务方法同步到数据库
        3. 验证返回空列表
        """
        # 模拟返回空列表
        mock_xtdata.get_sector_list.return_value = []

        # 执行同步方法
        inserted = sync_sector_list_to_db(self.session)

        # 验证结果
        self.assertEqual(len(inserted), 0)

if __name__ == '__main__':
    unittest.main()
