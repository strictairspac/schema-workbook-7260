import os
DATABASE_URL=os.getenv('DATABASE_URL','mysql+pymysql://schemaworkbook:schemaworkbook_demo_pw@localhost:17743/schemaworkbook')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.getenv('JWT_SECRET','local-schema-workbook-change-before-public-use')
# 报告文件与数据库结果必须同源；容器内挂载独立命名卷，本地可通过环境变量覆盖。
ARTIFACT_DIR=os.getenv('ARTIFACT_DIR','/artifacts')
QUEUE_KEY='schemaworkbook:queue'
QUEUE_INDEX_KEY='schemaworkbook:queue:index'
LOCK_KEY_PREFIX='schemaworkbook:lock:'
# 执行者租约时长；执行期间由续约线程保活，停机且未续约超过该时长即判定为中断。
LEASE_SECONDS=int(os.getenv('LEASE_SECONDS','30'))
RECONCILE_EVERY_SECONDS=int(os.getenv('RECONCILE_EVERY_SECONDS','5'))
