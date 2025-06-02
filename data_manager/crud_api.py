from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List

# 数据库配置
DB_USER = 'root'
DB_PASSWORD = ''  # 请根据实际情况填写
DB_HOST = '8.130.141.23'
DB_PORT = '3306'
DB_NAME = 'test'
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=True, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ORM模型
class Test(Base):
    __tablename__ = 'test'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(64), index=True)
    value = Column(String(128))

# Pydantic模型
class TestCreate(BaseModel):
    name: str
    value: str

class TestUpdate(BaseModel):
    name: str = None
    value: str = None

class TestOut(BaseModel):
    id: int
    name: str
    value: str
    class Config:
        orm_mode = True

# FastAPI实例
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 创建表（如未创建）
Base.metadata.create_all(bind=engine)

# 增
@app.post("/test/", response_model=TestOut)
def create_test(item: TestCreate, db: Session = Depends(get_db)):
    db_item = Test(**item.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

# 查（全部）
@app.get("/test/", response_model=List[TestOut])
def read_tests(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(Test).offset(skip).limit(limit).all()

# 查（单个）
@app.get("/test/{test_id}", response_model=TestOut)
def read_test(test_id: int, db: Session = Depends(get_db)):
    db_item = db.query(Test).filter(Test.id == test_id).first()
    if not db_item:
        raise HTTPException(status_code=404, detail="Item not found")
    return db_item

# 改
@app.put("/test/{test_id}", response_model=TestOut)
def update_test(test_id: int, item: TestUpdate, db: Session = Depends(get_db)):
    db_item = db.query(Test).filter(Test.id == test_id).first()
    if not db_item:
        raise HTTPException(status_code=404, detail="Item not found")
    update_data = item.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_item, key, value)
    db.commit()
    db.refresh(db_item)
    return db_item

# 删
@app.delete("/test/{test_id}", response_model=dict)
def delete_test(test_id: int, db: Session = Depends(get_db)):
    db_item = db.query(Test).filter(Test.id == test_id).first()
    if not db_item:
        raise HTTPException(status_code=404, detail="Item not found")
    db.delete(db_item)
    db.commit()
    return {"ok": True}

# 启动命令：
# uvicorn data_manager.crud_api:app --reload

