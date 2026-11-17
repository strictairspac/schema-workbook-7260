from sqlalchemy import select
from fastapi import HTTPException
from redis.exceptions import RedisError
from src.models.entities import Relation,Dependency,Report
from src.constants.enums import RUN_STATES,ACTIVE_STATES
from src.utils.serialization import record
from src.utils.artifacts import artifact_path,is_artifact_consistent
from .common import save
from . import queueing

def listing(db):return [record(x) for x in db.scalars(select(Report).order_by(Report.id.desc()))]
def detail(db,ident):
    obj=db.get(Report,ident)
    if not obj:raise HTTPException(404,'报告不存在')
    return record(obj)

def create(db,data,user):
    relation=db.get(Relation,data.relation_id)
    if not relation:raise HTTPException(404,'关系不存在')
    deps=list(db.scalars(select(Dependency).where(Dependency.relation_id==relation.id)))
    # 受理瞬间冻结关系与依赖快照，后续草案/依赖变更不影响本任务，旧报告始终可查。
    snapshot={'relation':record(relation),'dependencies':[{'lhs':x.lhs,'rhs':x.rhs} for x in deps]}
    obj=Report(**data.model_dump(),input_snapshot=snapshot,status=RUN_STATES[0])
    result=save(db,obj,user,'范式分解受理 '+data.name)
    # 队列暂不可用时任务仍已受理：保留 queued，对账线程在 Redis 恢复后自动接回，绝不标记假失败。
    try:queueing.enqueue(obj.id)
    except RedisError:pass
    return result

def cancel(db,ident,user):
    obj=db.get(Report,ident)
    if not obj:raise HTTPException(404,'报告不存在')
    if obj.status not in ACTIVE_STATES:
        raise HTTPException(409,f'任务已处于「{obj.status}」终态，无法取消')
    obj.status='cancelled';obj.run_token=None
    obj.error=obj.error or '任务已取消，执行结果不予发布'
    save(db,obj,user,f'取消分解任务 {obj.name}')
    # 清掉队列消息与执行租约；迟到执行者发布前会因令牌/状态校验失败而放弃。
    queueing.detach(ident)
    return record(obj)

def retry(db,ident,user):
    obj=db.get(Report,ident)
    if not obj:raise HTTPException(404,'报告不存在')
    if obj.status!='failed':
        raise HTTPException(409,'仅失败任务可以重新执行；排队中断的任务会自动接回')
    # 复用受理时冻结的快照，不重新读取当前草案与依赖。
    obj.status='queued';obj.error=None;obj.run_token=None;obj.result=None
    save(db,obj,user,f'重新执行分解任务 {obj.name}')
    queueing.detach(ident)
    try:queueing.enqueue(ident)
    except RedisError:pass
    return record(obj)

def file_status(db,ident):
    """供导出/详情核对：completed 必须有与结果一致的报告文件，否则暴露可恢复异常而不是假完成。"""
    obj=db.get(Report,ident)
    if not obj:raise HTTPException(404,'报告不存在')
    if obj.status!='completed':return {'available':False,'path':artifact_path(ident),'consistent':False}
    consistent=is_artifact_consistent(ident,obj.result)
    return {'available':consistent,'path':artifact_path(ident),'consistent':consistent}
