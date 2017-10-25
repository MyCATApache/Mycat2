package io.mycat.mycat2.HBT;


import io.mycat.mycat2.MycatSession;

public class HBTEngine {
    public static final String MEM = null;
    
    public OpPipeline streamOf(MycatSession mycatSession, SqlMeta sqlMeta, RowMeta rowMeta) {
        return new HeaderHBTPipeline(mycatSession, sqlMeta, rowMeta);
    }
    
    

}
