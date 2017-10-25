
package io.mycat.mycat2.hbt.pipeline;

import java.util.List;
import java.util.function.Function;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.hbt.JoinMeta;
import io.mycat.mycat2.hbt.MatchCallback;
import io.mycat.mycat2.hbt.OrderMeta;
import io.mycat.mycat2.hbt.GroupPairKey;
import io.mycat.mycat2.hbt.GroupPairKeyMeta;
import io.mycat.mycat2.hbt.ResultSetMeta;
import io.mycat.mycat2.hbt.RowMeta;
import io.mycat.mycat2.hbt.SqlMeta;

/**
*@desc
*@author zhangwy   @date 2017年10月19日 上午7:39:00
**/
public interface OpPipeline {
    public OpPipeline group(GroupPairKeyMeta keyFunction,  ResultSetMeta resultSetMeta,
    		List<Function<List<List<byte[]>>,List<byte[]>>> groupOpFunction
    		) ;
    
    public OpPipeline limit(int limit) ;
    public OpPipeline skip(int n) ;
    public OpPipeline filter() ;
    public OpPipeline join(MycatSession session, SqlMeta sqlMeta,
            RowMeta rowMeta, JoinMeta joinMeta, ResultSetMeta resultSetMeta, MatchCallback callback);
    
    public void out(MycatSession session);

	public OpPipeline order(OrderMeta orderMeta);
    
}

