import logging
from typing import TypeVar, List, Type
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlmodel import SQLModel
from utils.quant_logger import init_logger

logger = init_logger()


def insert_ignore(
    db: Session,
    model_cls: Type[SQLModel],
    objs: List[SQLModel]
) -> int:
    """
    批量插入数据到 MySQL，遇到主键或唯一索引冲突时自动跳过（使用 INSERT IGNORE）

    Args:
        db: SQLAlchemy Session 对象
        model_cls: SQLModel 表模型类
        objs: 要插入的对象列表

    Returns:
        实际尝试插入的记录数量（不保证都成功）
    """
    if not objs:
        return 0

    try:
        table = model_cls.__table__

        # 兼容 SQLModel 0.0.14+，使用 model_dump()
        values = [obj.model_dump(exclude_unset=True) for obj in objs]

        insert_stmt = mysql_insert(table).values(values)
        ignore_stmt = insert_stmt.prefix_with("IGNORE")
        db.execute(ignore_stmt)
        db.commit()
        return len(values)
    except Exception as e:
        db.rollback()
        raise RuntimeError(f"执行 insert_ignore 失败: {e}")


T = TypeVar('T', bound=SQLModel)


def insert_on_duplicate_update_for_kline(
        db: Session,
        model_cls: Type[T],
        objs: List[T],
        auto_commit: bool = False,
        update_fields: List[str] = None
) -> int:
    """
    通用的股票K线数据插入/更新函数

    Args:
        db: 数据库会话
        model_cls: 股票数据模型类
        objs: 股票数据对象列表
        auto_commit: 是否自动提交
        update_fields: 要更新的字段列表，默认为标准K线字段

    Returns:
        int: 受影响的行数
    """
    if not objs:
        return 0

    # 默认的K线数据更新字段
    if update_fields is None:
        update_fields = ['open', 'high', 'low', 'close', 'volume', 'amount']

    try:
        table = model_cls.__table__
        values = [obj.model_dump(exclude_unset=True) for obj in objs]

        if not values:
            return 0

        insert_stmt = mysql_insert(table).values(values)

        # 验证字段是否存在于模型中
        available_fields = []
        for field in update_fields:
            if hasattr(model_cls, field) and field in values[0]:
                available_fields.append(field)

        if not available_fields:
            # 如果没有可更新字段，使用 INSERT IGNORE
            insert_stmt = mysql_insert(table).prefix_with("IGNORE").values(values)
            result = db.execute(insert_stmt)
        else:
            update_dict = {
                field: insert_stmt.inserted[field] for field in available_fields
            }
            upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)
            result = db.execute(upsert_stmt)

        if auto_commit:
            db.commit()

        return result.rowcount

    except Exception as e:
        if auto_commit:
            db.rollback()
        model_name = getattr(model_cls, '__name__', str(model_cls))
        raise RuntimeError(f"{model_name} 数据插入/更新失败: {e}")

# 数据下载回调函数
def download_kline_callback(data):
    logger.info(f'下载K线数据回调: {data}')
