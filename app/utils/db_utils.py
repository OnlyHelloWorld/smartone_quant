from typing import List, Type, Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlmodel import SQLModel


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


def insert_on_duplicate_update(
    db: Session,
    model_cls: Type[SQLModel],
    objs: List[SQLModel],
    update_fields: Optional[List[str]] = None
) -> int:
    """
    批量插入数据，若唯一键冲突则自动更新指定字段（MySQL专用）

    Args:
        db: SQLAlchemy Session 对象
        model_cls: SQLModel 表模型类
        objs: 要插入的对象列表
        update_fields: 冲突时要更新的字段列表。如果为 None，则默认更新所有字段（除了主键和唯一键）

    Returns:
        int: 尝试插入的记录数（不代表实际写入行数）
    """
    if not objs:
        return 0

    try:
        table = model_cls.__table__
        values = [obj.model_dump(exclude_unset=True) for obj in objs]

        insert_stmt = mysql_insert(table).values(values)

        # 构建 ON DUPLICATE KEY UPDATE 子句
        if update_fields is None:
            # 默认更新除主键和唯一约束之外的字段（此处简化，用户需按需指定）
            update_fields = list(values[0].keys())

        update_dict = {
            field: insert_stmt.inserted[field] for field in update_fields
        }

        upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)
        db.execute(upsert_stmt)
        db.commit()
        return len(values)
    except Exception as e:
        db.rollback()
        raise RuntimeError(f"执行 insert_on_duplicate_update 失败: {e}")