package io.mycat.util;

import io.mycat.MycatException;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeUnitUtil {
    public static TimeUnit valueOf(String name) {
        Objects.requireNonNull(name, "TimeUnit must not be null");
        try {
            return TimeUnit.valueOf(name.trim().toUpperCase());
        } catch (Throwable throwable) {
            String message = Stream.of(TimeUnit.values()).map(i -> i.toString())
                    .collect(Collectors.joining(","));
            throw new MycatException(" TimeUnit must be " + message);
        }
    }
}
