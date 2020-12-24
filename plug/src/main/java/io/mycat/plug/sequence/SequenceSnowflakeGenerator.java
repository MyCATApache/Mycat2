package io.mycat.plug.sequence;

import com.imadcn.framework.idworker.algorithm.Snowflake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class SequenceSnowflakeGenerator implements Supplier<Number> {
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(SequenceSnowflakeGenerator.class);
    private final Snowflake snowflake;

    public SequenceSnowflakeGenerator(Map<String, Object> config) {
        this.snowflake = Snowflake.create(Long.parseLong(Objects.toString(config.get("workerId"))));
    }

    @Override
    public Number get() {
        return snowflake.nextId();
    }
}