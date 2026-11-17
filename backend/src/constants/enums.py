ROLES = ('admin', 'operator', 'viewer')
# queued：已受理等待调度；running：已被执行者认领；completed/failed/cancelled 均为终态。
RUN_STATES = ('queued', 'running', 'completed', 'failed', 'cancelled')
# 可重新接回调度的状态（失败可人工重试；排队中由对账自动补投）。
RECOVERABLE_STATES = ('queued', 'failed')
ACTIVE_STATES = ('queued', 'running')
TERMINAL_STATES = ('completed', 'failed', 'cancelled')
