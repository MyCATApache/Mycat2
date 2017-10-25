package io.mycat.mycat2.hbt.pipeline;


import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.hbt.RowMeta;
import io.mycat.mycat2.hbt.SqlMeta;

public class HBTEngine {
    public static final String MEM = null;
    
    public OpPipeline streamOf(MycatSession mycatSession, SqlMeta sqlMeta, RowMeta rowMeta) {
        return new HeaderHBTPipeline(mycatSession, sqlMeta, rowMeta);
    }
    
    

}
