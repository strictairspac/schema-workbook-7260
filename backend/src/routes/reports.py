from fastapi import APIRouter,Depends
from fastapi.responses import Response
from src.config.database import db_session
from src.utils.security import current,writer
from src.schemas.report import ReportInput
from src.services import reports as service
from src.utils.artifacts import serialize_result,write_artifact_atomic,is_artifact_consistent
router=APIRouter()
@router.get('/reports')
def listing(db=Depends(db_session),user=Depends(current)):return service.listing(db)
@router.post('/reports',status_code=201)
def create(data:ReportInput,db=Depends(db_session),user=Depends(writer)):return service.create(db,data,user)
@router.get('/reports/{ident}')
def detail(ident:int,db=Depends(db_session),user=Depends(current)):return service.detail(db,ident)
@router.post('/reports/{ident}/cancel')
def cancel(ident:int,db=Depends(db_session),user=Depends(writer)):return service.cancel(db,ident,user)
@router.post('/reports/{ident}/retry')
def retry(ident:int,db=Depends(db_session),user=Depends(writer)):return service.retry(db,ident,user)
@router.get('/reports/{ident}/file')
def file_status(ident:int,db=Depends(db_session),user=Depends(current)):return service.file_status(db,ident)
@router.get('/reports/{ident}/export')
def export(ident:int,db=Depends(db_session),user=Depends(current)):
    obj=service.detail(db,ident)
    if obj['status']!='completed' or not obj.get('result'):
        from fastapi import HTTPException
        raise HTTPException(409,'任务尚未完成，没有可导出的报告文件')
    # 导出与磁盘报告文件同源：文件缺失/损坏时以数据库结果原子补写，补写失败明确报错而非返回假文件。
    if not is_artifact_consistent(ident,obj['result']):
        try:write_artifact_atomic(ident,obj['result'])
        except OSError:
            from fastapi import HTTPException
            raise HTTPException(503,'报告文件暂不可用，已保留任务结果，稍后可重试导出')
    return Response(serialize_result(obj['result']),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="report-{ident}.json"'})
