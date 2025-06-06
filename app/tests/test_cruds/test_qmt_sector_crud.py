import unittest
from sqlmodel import Session, SQLModel, create_engine
from sqlalchemy import text
from app.models.qmt_sector import QmtSector
from app.cruds.qmt_sector_crud import create_qmt_sector, update_qmt_sector, get_qmt_sector_by_name
from app.core.config import settings

"""
本测试文件用于测试QmtSector板块相关的CRUD操作。
测试流程如下：
1. 连接真实MySQL数据库。
2. 自动建表（如未用Alembic迁移）。
3. 测试创建板块(create_qmt_sector)。
4. 测试通过名称查询板块(get_qmt_sector_by_name)。
5. 测试更新板块(update_qmt_sector)。
每一步均有详细中文注释。
"""

class TestQmtSectorCrud(unittest.TestCase):
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
        self.session.exec(text(f"DELETE FROM {QmtSector.__tablename__} WHERE sector_name in ('科技板块', '新能源板块')"))
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_create_and_get_and_update_qmt_sector(self):
        """
        测试流程：
        1. 创建板块
        2. 通过名称查询板块
        3. 更新板块名称
        4. 再次通过新名称查询
        """
        # 1. 创建板块
        sector = QmtSector(sector_name="科技板块")
        created_sector = create_qmt_sector(session=self.session, qmt_sector_create=sector)
        print(f"创建的板块ID: {created_sector.id}, 名称: {created_sector.sector_name}")
        self.assertIsNotNone(created_sector.id, "创建后应有ID")
        self.assertEqual(created_sector.sector_name, "科技板块")

        # 2. 通过名称查询板块
        found_sector = get_qmt_sector_by_name(session=self.session, name="科技板块")
        print(f"查询到的板块ID: {found_sector.id}, 名称: {found_sector.sector_name}")
        self.assertIsNotNone(found_sector, "应能通过名称查到板块")
        self.assertEqual(found_sector.sector_name, "科技板块")

        # 3. 更新板块名称
        updated_sector = QmtSector(sector_name="新能源板块")
        print(f"更新前的板块ID: {found_sector.id}, 旧名称: {found_sector.sector_name}")
        update_qmt_sector(session=self.session, db_qmt_sector=found_sector, qmt_sector_in=updated_sector)

        # 4. 通过新名称查询
        found_new = get_qmt_sector_by_name(session=self.session, name="新能源板块")
        print(f"更新后的板块ID: {found_new.id}, 新名称: {found_new.sector_name}")
        self.assertIsNotNone(found_new, "更新后应能查到新名称板块")
        self.assertEqual(found_new.sector_name, "新能源板块")

        # 5. 旧名称应查不到
        found_old = get_qmt_sector_by_name(session=self.session, name="科技板块")
        print(f"查询旧名称板块: {found_old}")
        self.assertIsNone(found_old, "旧名称应查不到")

if __name__ == '__main__':
    unittest.main()
