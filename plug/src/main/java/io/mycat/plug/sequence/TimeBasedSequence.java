package io.mycat.plug.sequence;

import com.imadcn.framework.idworker.algorithm.Snowflake;
import io.mycat.config.SequenceConfig;

public class TimeBasedSequence implements SequenceHandler {

    private long workerId;
    private Snowflake snowflake;

    @Override
    public void init(SequenceConfig config, long workerId) {
        this.workerId = workerId;
        this.snowflake = Snowflake.create(workerId);
    }

    @Override
    public void setStart(Number value) {

    }


    public Number nextId() {
        return snowflake.nextId();
    }

    @Override
    public Number get() {
        return nextId();
    }
}
