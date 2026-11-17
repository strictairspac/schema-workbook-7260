"""单 worker 串行执行器，具备中断接回、取消语义与幂等发布。

正确性边界：
- Redis NX 锁保证同一时刻只有一个执行者处理同一任务；重复投递的消息在抢锁阶段被丢弃。
- 数据库 CAS（按 run_token）是最终权威：取消后迟到返回的执行者无法发布结果。
- 报告文件先原子落盘，再提交 completed；落盘失败只进入可恢复 failed，不显示假完成。
- 周期性对账把停机残留的 running 接回排队，把 Redis 恢复后丢失的 queued 补投。
"""
import secrets,threading,time,traceback
from pathlib import Path
from redis.exceptions import RedisError
from sqlalchemy import update
from src.config.database import Session,ensure_schema
from src.config.settings import QUEUE_KEY,QUEUE_INDEX_KEY,RECONCILE_EVERY_SECONDS,LEASE_SECONDS
from src.models.entities import Report
from src.services import queueing
from src.services.normalization import analyze
from src.utils.artifacts import artifact_path,write_artifact_atomic

def _start_renewer(ident,token):
    stop=threading.Event()
    def loop():
        while not stop.wait(max(1,LEASE_SECONDS//3)):
            if not queueing.renew_lock(ident,token):
                stop.set();return
    threading.Thread(target=loop,name=f'lease-{ident}',daemon=True).start()
    return stop

def claim(db,ident,token):
    """乐观认领：仅 queued/running 可转为带令牌的 running，返回当前行（含终态）或 None。"""
    rowcount=db.execute(
        update(Report).where(Report.id==ident,Report.status.in_(('queued','running')))
        .values(status='running',run_token=token,error=None)
    ).rowcount
    db.commit()
    if not rowcount:return db.get(Report,ident)
    return db.get(Report,ident)

def publish_completed(db,ident,token,result):
    """令牌校验发布：行计数为 0 说明任务已被取消/接走，当前结果作废。"""
    rowcount=db.execute(
        update(Report).where(Report.id==ident,Report.status=='running',Report.run_token==token)
        .values(status='completed',result=result,error=None)
    ).rowcount
    db.commit()
    return rowcount

def publish_failed(db,ident,token,message):
    rowcount=db.execute(
        update(Report).where(Report.id==ident,Report.status=='running',Report.run_token==token)
        .values(status='failed',error=message)
    ).rowcount
    db.commit()
    return rowcount

def process_one(ident):
    token=secrets.token_hex(16)
    if not queueing.acquire_lock(ident,token):
        return  # 已有执行者持锁，重复投递直接丢弃
    try:
        with Session() as db:
            obj=claim(db,ident,token)
            if obj is None or obj.status in ('completed','cancelled','failed'):
                return  # 终态消息（例如取消后才被取出），不再执行
            snapshot=obj.input_snapshot
        renewer_stop=_start_renewer(ident,token)
        try:
            result=analyze(snapshot['relation']['attributes'],snapshot['dependencies'])
            # 先写文件：原子替换成功后才允许提交完成态，保证结果与报告文件一致。
            write_artifact_atomic(ident,result)
        except Exception as exc:
            traceback.print_exc()
            with Session() as db:
                if publish_failed(db,ident,token,f'执行失败，可重新执行：{exc}'):
                    print(f'[worker] report {ident} marked failed: {exc}')
            return
        finally:
            renewer_stop.set()
        with Session() as db:
            if publish_completed(db,ident,token,result):
                print(f'[worker] report {ident} completed and artifact stored')
            else:
                # 取消确认后迟到返回：绝不发布。仅当文件内容仍属于本次作废结果时才清理，
                # 避免误删后续执行者已生成的同名新报告。
                current=db.get(Report,ident)
                if current is not None and (current.run_token==token or current.status=='cancelled') \
                        and current.result!=result:
                    try:Path(artifact_path(ident)).unlink()
                    except OSError:pass
                print(f'[worker] report {ident} result discarded (cancelled or superseded)')
    except Exception:
        traceback.print_exc()  # 租约仍受 TTL 保护，对账会把任务接回
    finally:
        queueing.release_lock(ident,token)

def main():
    ensure_schema()
    last_reconcile=0.0
    while True:
        Path('/tmp/worker-heartbeat').touch()
        now=time.monotonic()
        if now-last_reconcile>=RECONCILE_EVERY_SECONDS:
            try:
                with Session() as db:queueing.reconcile(db)
            except Exception:
                traceback.print_exc()
            last_reconcile=now
        try:
            item=queueing.queue.blpop(QUEUE_KEY,timeout=2)
        except RedisError:
            time.sleep(1);continue
        if not item:continue
        ident=int(item[1])
        try:queueing.queue.srem(QUEUE_INDEX_KEY,ident)
        except RedisError:pass
        process_one(ident)

if __name__=='__main__':
    main()
