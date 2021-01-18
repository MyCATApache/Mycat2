package io.mycat.calcite;

import java.util.Map;

public class StreamMycatEnumerableRelImplementor extends MycatEnumerableRelImplementor{

    public StreamMycatEnumerableRelImplementor(Map<String, Object> internalParameters) {
        super(internalParameters);
    }

    public void probe(MycatRel mycatRel){
    }
}
