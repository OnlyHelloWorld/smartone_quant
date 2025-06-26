from datetime import date
from typing import Any, List

from sqlalchemy import text, and_
from sqlmodel import Session, select

from app.models.qmt_stock_divid_factors import QmtStockDividFactors

"""
对QmtStockDividFactors模型的增删改查操作
"""


def create_qmt_stock_divid_factors(*, session: Session,
                                   divid_factors_create: QmtStockDividFactors) -> QmtStockDividFactors:
    """创建除权数据记录"""
    db_obj = QmtStockDividFactors(**divid_factors_create.model_dump(exclude={'id'}))
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def update_qmt_stock_divid_factors(*, session: Session, db_divid_factors: QmtStockDividFactors,
                                   divid_factors_in: QmtStockDividFactors) -> Any:
    """更新除权数据记录"""
    divid_factors_data = divid_factors_in.model_dump(exclude_unset=True)
    db_divid_factors.sqlmodel_update(divid_factors_data)
    session.add(db_divid_factors)
    session.commit()
    session.refresh(db_divid_factors)
    return db_divid_factors


def get_qmt_stock_divid_factors_by_stock_and_date(*, session: Session, stock_code: str,
                                                  divid_date: date) -> QmtStockDividFactors | None:
    """根据股票代码和除权日期获取除权记录"""
    statement = select(QmtStockDividFactors).where(
        and_(QmtStockDividFactors.stock_code == stock_code, QmtStockDividFactors.divid_date == divid_date)
    )
    return session.exec(statement).first()


def get_qmt_stock_divid_factors_by_date(*, session: Session, target_date: date) -> List[QmtStockDividFactors]:
    """获取指定日期的所有除权记录"""
    statement = select(QmtStockDividFactors).where(QmtStockDividFactors.divid_date == target_date)
    return list(session.exec(statement).all())


def get_qmt_stock_divid_factors_by_stock_and_date_range(
        *, session: Session, stock_code: str, start_date: date, end_date: date
) -> List[QmtStockDividFactors]:
    """获取某股票在指定时间范围的除权记录"""
    statement = select(QmtStockDividFactors).where(
        and_(
            QmtStockDividFactors.stock_code == stock_code,
            QmtStockDividFactors.divid_date >= start_date,
            QmtStockDividFactors.divid_date <= end_date
        )
    ).order_by(QmtStockDividFactors.divid_date)
    return list(session.exec(statement).all())


def get_qmt_stock_divid_factors_by_stocks_and_date_range(
        *, session: Session, stock_codes: List[str], start_date: date, end_date: date
) -> List[QmtStockDividFactors]:
    """获取多个股票在指定时间范围的除权记录"""
    statement = select(QmtStockDividFactors).where(
        and_(
            QmtStockDividFactors.stock_code.in_(stock_codes),
            QmtStockDividFactors.divid_date >= start_date,
            QmtStockDividFactors.divid_date <= end_date
        )
    ).order_by(QmtStockDividFactors.stock_code, QmtStockDividFactors.divid_date)
    return list(session.exec(statement).all())


def get_qmt_stock_divid_factors_by_date_range(
        *, session: Session, start_date: date, end_date: date
) -> List[QmtStockDividFactors]:
    """获取指定日期范围内的所有除权记录"""
    statement = select(QmtStockDividFactors).where(
        and_(
            QmtStockDividFactors.divid_date >= start_date,
            QmtStockDividFactors.divid_date <= end_date
        )
    ).order_by(QmtStockDividFactors.divid_date, QmtStockDividFactors.stock_code)
    return list(session.exec(statement).all())


def get_stocks_with_divid_on_date(*, session: Session, target_date: date) -> List[str]:
    """获取指定日期发生除权的股票代码列表"""
    statement = select(QmtStockDividFactors.stock_code).where(QmtStockDividFactors.divid_date == target_date).distinct()
    return list(session.exec(statement).all())


def batch_upsert_qmt_stock_divid_factors(*, session: Session, divid_factors_list: List[QmtStockDividFactors]) -> int:
    """
    批量插入或更新除权数据记录
    使用 ON DUPLICATE KEY UPDATE 处理唯一索引冲突
    """
    if not divid_factors_list:
        return 0

    try:
        # 构建批量插入SQL
        insert_sql = """
                     INSERT INTO qmt_stock_divid_factors
                     (stock_code, time, divid_date, interest, stock_bonus, stock_gift, allot_num, allot_price, gugai, \
                      dr)
                     VALUES \
                     """

        # 准备数据
        values_list = []
        for item in divid_factors_list:
            values = f"('{item.stock_code}', {item.time}, '{item.divid_date}', {item.interest}, " \
                     f"{item.stock_bonus}, {item.stock_gift}, {item.allot_num}, {item.allot_price}, " \
                     f"{item.gugai}, {item.dr})"
            values_list.append(values)

        insert_sql += ",".join(values_list)

        # 添加 ON DUPLICATE KEY UPDATE 子句
        update_sql = """
        ON DUPLICATE KEY UPDATE
        time = VALUES(time),
        interest = VALUES(interest),
        stock_bonus = VALUES(stock_bonus),
        stock_gift = VALUES(stock_gift),
        allot_num = VALUES(allot_num),
        allot_price = VALUES(allot_price),
        gugai = VALUES(gugai),
        dr = VALUES(dr),
        update_time = CURRENT_TIMESTAMP
        """

        final_sql = insert_sql + update_sql

        result = session.exec(text(final_sql))
        session.commit()
        return result.rowcount

    except Exception as e:
        session.rollback()
        raise e


def delete_qmt_stock_divid_factors_by_stock_and_date_range(
        *, session: Session, stock_code: str, start_date: date, end_date: date
) -> int:
    """删除某股票指定时间范围的除权记录"""
    statement = text(
        f"DELETE FROM {QmtStockDividFactors.__tablename__} "
        f"WHERE stock_code = :stock_code AND divid_date >= :start_date AND divid_date <= :end_date"
    )
    result = session.exec(statement, {
        "stock_code": stock_code,
        "start_date": start_date,
        "end_date": end_date
    })
    session.commit()
    return result.rowcount


def delete_all_qmt_stock_divid_factors(session: Session) -> int:
    """删除所有除权数据记录并返回删除的数量"""
    statement = text(f"DELETE FROM {QmtStockDividFactors.__tablename__}")
    result = session.exec(statement)
    session.commit()
    return result.rowcount


# main函数用于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings
    from datetime import date

    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    with Session(engine) as session:
        # 测试查询指定日期的除权记录
        target_date = date(2024, 7, 12)
        records = get_qmt_stock_divid_factors_by_date(session=session, target_date=target_date)
        print(f"日期 {target_date} 的除权记录数量: {len(records)}")

        # 测试查询指定日期发生除权的股票
        stocks = get_stocks_with_divid_on_date(session=session, target_date=target_date)
        print(f"日期 {target_date} 发生除权的股票: {stocks}")
