/** Relation:{id,code,title,attributes,note}; Dependency:{id,relation_id,lhs,rhs,note};
 * Report:{id,name,relation_id,input_snapshot,status,result,error,run_token,created_at,updated_at}
 * status: queued 待执行 / running 执行中 / completed 已完成 / failed 失败 / cancelled 已取消 */
export const entityNames=['relations','dependencies','reports'];
