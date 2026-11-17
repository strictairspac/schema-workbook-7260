"""报告文件与数据库结果的同源写入/校验。

文件先写临时文件再原子替换，只有替换成功后才允许把任务置为 completed；
任何写入失败都停留在可恢复状态，页面不会出现没有文件支撑的假完成。
"""
import json,os,tempfile
from src.config.settings import ARTIFACT_DIR

def serialize_result(result):
    return json.dumps(result,ensure_ascii=False,indent=2)

def artifact_path(ident):
    return os.path.join(ARTIFACT_DIR,f'report-{ident}.json')

def write_artifact_atomic(ident,result):
    """原子写入报告文件，返回最终路径；目录或磁盘故障时抛 OSError 由调用方转可恢复失败。"""
    os.makedirs(ARTIFACT_DIR,exist_ok=True)
    payload=serialize_result(result)
    fd,tmp=tempfile.mkstemp(prefix=f'.report-{ident}-',suffix='.tmp',dir=ARTIFACT_DIR)
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as fh:
            fh.write(payload);fh.flush();os.fsync(fh.fileno())
        os.replace(tmp,artifact_path(ident))
    except BaseException:
        try:os.unlink(tmp)
        except OSError:pass
        raise
    return artifact_path(ident)

def read_artifact(ident):
    with open(artifact_path(ident),encoding='utf-8') as fh:
        return json.loads(fh.read())

def is_artifact_consistent(ident,result):
    """文件存在、可解析且与数据库结果逐字节一致（统一序列化口径）。"""
    try:
        with open(artifact_path(ident),encoding='utf-8') as fh:
            on_disk=fh.read()
        return on_disk==serialize_result(result)
    except (OSError,ValueError):
        return False
