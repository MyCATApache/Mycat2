package io.mycat.plug.sequence;

import com.imadcn.framework.idworker.algorithm.Snowflake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class SequenceSnowflakeGenerator implements Supplier<String> {
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(SequenceSnowflakeGenerator.class);
    private final Snowflake snowflake;
    public SequenceSnowflakeGenerator(String config) {
        this. snowflake = Snowflake.create(Long.parseLong(config.split(":")[1]));
    }
    @Override
    public String get() {
        return String.valueOf(snowflake.nextId());
    }
}