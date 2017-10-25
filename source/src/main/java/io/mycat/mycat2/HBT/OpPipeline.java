
package io.mycat.mycat2.HBT;

import java.util.List;
import java.util.function.Function;

import io.mycat.mycat2.MycatSession;

/**
*@desc
*@author zhangwy   @date 2017年10月19日 上午7:39:00
**/
public interface OpPipeline {
    public OpPipeline group(Function<List<byte[]>,PairKey> keyFunction,  ResultSetMeta resultSetMeta,
    		List<Function<List<List<byte[]>>,List<byte[]>>> opFunction
    		) ;
    
    public OpPipeline limit(int limit) ;
    public OpPipeline skip(int n) ;
    public OpPipeline filter() ;
    public OpPipeline join(MycatSession session, SqlMeta sqlMeta,
            RowMeta rowMeta, JoinMeta joinMeta, ResultSetMeta resultSetMeta, MatchCallback callback);
    
    public void out(MycatSession session);

	public OpPipeline order(OrderMeta orderMeta);
    
}

