package io.mycat.calcite;

import io.mycat.calcite.physical.MycatCalc;

import java.util.Map;

public class StreamMycatEnumerableRelImplementor extends MycatEnumerableRelImplementor{
    boolean stream = true;
    public StreamMycatEnumerableRelImplementor(Map<String, Object> internalParameters) {
        super(internalParameters);
    }

    public boolean isStream() {
        return stream;
    }

    public void setStream(boolean stream) {
        this.stream = stream;
    }
    public void probe(MycatRel mycatRel){
    }
}
