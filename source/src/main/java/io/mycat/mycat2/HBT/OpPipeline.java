
package io.mycat.mycat2.HBT;

import io.mycat.mycat2.MycatSession;

/**
*@desc
*@author zhangwy   @date 2017年10月19日 上午7:39:00
**/
public interface OpPipeline {
    public OpPipeline group() ;
    
    public OpPipeline limit() ;
    public OpPipeline filter() ;
    
    public OpPipeline join(MycatSession session, SqlMeta sqlMeta,
            RowMeta rowMeta, JoinMeta joinMeta, ResultSetMeta resultSetMeta, MatchCallback callback);
    
    public void out(MycatSession session);
    
}

