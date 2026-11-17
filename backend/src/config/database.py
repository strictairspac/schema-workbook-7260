from sqlalchemy import create_engine,text,inspect
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL
class Base(DeclarativeBase): pass
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(engine, expire_on_commit=False)
def db_session():
    with Session() as db: yield db

def ensure_schema():
    """create_all 负责新建表；对已存在的库做幂等补列，保证旧环境可平滑恢复新语义。"""
    Base.metadata.create_all(engine)
    inspector=inspect(engine)
    if 'reports' not in inspector.get_table_names():return
    existing={col['name'] for col in inspector.get_columns('reports')}
    with engine.begin() as conn:
        if 'run_token' not in existing:
            conn.execute(text('ALTER TABLE reports ADD COLUMN run_token VARCHAR(64) NULL'))
        if 'updated_at' not in existing:
            conn.execute(text('ALTER TABLE reports ADD COLUMN updated_at DATETIME NULL'))
