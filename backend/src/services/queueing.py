"""Redis 任务队列与对账恢复。

设计要点：
- LIST 承载投递，SET 为去重索引，Lua 保证「入索引并入队」原子，重复投递不会产生重复执行。
- 每个任务一把 NX 租约锁，执行者持锁计算；停机后锁自然过期，对账把残留 running 接回排队。
- 数据库状态是唯一权威：Redis 整体不可用时任务仍以 queued 落库，恢复后由对账自动补投。
"""
from redis import Redis
from redis.exceptions import RedisError
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from sqlalchemy import select
from src.config.settings import REDIS_URL,QUEUE_KEY,QUEUE_INDEX_KEY,LOCK_KEY_PREFIX,LEASE_SECONDS
from src.models.entities import Report
from src.utils.artifacts import is_artifact_consistent,write_artifact_atomic

# 有限次退避重试，覆盖抖动；长时间中断时异常上抛由调用方保留任务，绝不假失败。
queue=Redis.from_url(REDIS_URL,decode_responses=True,socket_connect_timeout=3,socket_timeout=5,
                     retry=Retry(ExponentialBackoff(),3),retry_on_error=[RedisError],retry_on_timeout=True)

# 幂等入队：SET 中已存在则不重复 RPUSH，保证同一任务在队列里至多一条待取消息。
_ENQUEUE_LUA=queue.register_script('''
if redis.call('sismember', KEYS[2], ARGV[1]) == 0 then
  redis.call('sadd', KEYS[2], ARGV[1])
  redis.call('rpush', KEYS[1], ARGV[1])
end
return 1
''')
# 仅持令牌者可释放/续租锁，避免后来者的锁被旧执行者误删。
_RELEASE_LUA=queue.register_script('if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end')
_RENEW_LUA=queue.register_script('if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("expire", KEYS[1], ARGV[2]) else return 0 end')

def lock_key(ident):return f'{LOCK_KEY_PREFIX}{ident}'

def enqueue(ident):
    """幂等投递；Redis 故障时抛 RedisError，调用方保留 queued 状态等待对账。"""
    _ENQUEUE_LUA(keys=[QUEUE_KEY,QUEUE_INDEX_KEY],args=[str(ident)])

def detach(ident):
    """取消/重试时尽力清理队列残留；失败不影响数据库权威状态，对账稍后兜底。"""
    try:
        queue.srem(QUEUE_INDEX_KEY,str(ident))
        queue.lrem(QUEUE_KEY,0,str(ident))
        queue.delete(lock_key(ident))
    except RedisError:pass

def acquire_lock(ident,token):
    try:return bool(queue.set(lock_key(ident),token,nx=True,ex=LEASE_SECONDS))
    except RedisError:return False

def renew_lock(ident,token):
    try:return bool(_RENEW_LUA(keys=[lock_key(ident)],args=[token,LEASE_SECONDS]))
    except RedisError:return False

def release_lock(ident,token):
    try:_RELEASE_LUA(keys=[lock_key(ident)],args=[token])
    except RedisError:pass

def lock_alive(ident):
    try:return bool(queue.exists(lock_key(ident)))
    except RedisError:
        # 无法确认租约时按存活处理，宁可延后接回也不误判中断。
        return True

def reconcile(db):
    """周期性对账：中断接回、丢失补投、索引纠偏、报告文件自愈。返回处置条数。"""
    actions=0
    queued=[r for r in db.scalars(select(Report).where(Report.status=='queued'))]
    # 1. running 且租约已失：执行者停机/中断，重置排队并清空旧执行令牌。
    for obj in db.scalars(select(Report).where(Report.status=='running')):
        if not lock_alive(obj.id):
            obj.status='queued';obj.run_token=None
            obj.error='检测到执行中断，任务已自动重新排队'
            queued.append(obj);actions+=1
    if actions:db.commit()
    # 2. 队列去重索引纠偏：清理数据库侧已不存在的排队登记。
    try:indexed={int(x) for x in queue.smembers(QUEUE_INDEX_KEY)}
    except RedisError:indexed=set()
    queued_ids={r.id for r in queued}
    for stale in indexed-queued_ids:
        try:queue.srem(QUEUE_INDEX_KEY,str(stale))
        except RedisError:break
    # 3. 已受理但队列里没有的任务补投（含 Redis 恢复后的自动接回）；
    #    已有执行者持锁时跳过，避免把正在执行的任务重复排进队列。
    for ident in sorted(queued_ids-indexed):
        if lock_alive(ident):continue
        try:enqueue(ident);actions+=1
        except RedisError:break
    # 4. 已完成任务的报告文件缺失/损坏时，以数据库结果为准重写，杜绝「假完成」与文件丢失。
    for obj in db.scalars(select(Report).where(Report.status=='completed')):
        if obj.result is not None and not is_artifact_consistent(obj.id,obj.result):
            try:write_artifact_atomic(obj.id,obj.result);actions+=1
            except OSError:pass
    return actions
