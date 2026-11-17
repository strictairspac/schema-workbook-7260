import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from src.config.database import Base,engine
from src.models import entities
from src.controllers.auth import router as auth
from src.routes import relations,dependencies,reports,audit
from src.middlewares.errors import ErrorMiddleware
from src.middlewares.request_id import RequestIdMiddleware
@asynccontextmanager
async def lifespan(app):
    from src.config.database import ensure_schema
    ensure_schema()
    if os.getenv('SEED_DEMO','true')=='true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception: logging.exception('seed failed; manual retry is available')
    yield
app=FastAPI(title='实验委托模型拆解册',lifespan=lifespan)
app.add_middleware(ErrorMiddleware);app.add_middleware(RequestIdMiddleware)
for r in (auth,relations.router,dependencies.router,reports.router,audit.router): app.include_router(r)
@app.get('/health')
def health():
    with engine.connect() as c: c.execute(text('SELECT 1'))
    queue_state='up'
    try:
        from src.services.queueing import queue
        queue.ping()
    except Exception:
        # Redis 中断期间已受理任务保留 queued，由对账自动接回，API 仍可用。
        queue_state='degraded'
    return {'status':'healthy','queue':queue_state}
