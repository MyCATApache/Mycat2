package io.mycat.mpp;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AggCalls {
    final static Map<String, AggCall> map = new HashMap<>();

    static {
        map.put("avg", new Avg());
        map.put("count", new Count());
        map.put("sum", new Sum());
    }

    interface AggCall {

        String name();

        public void accept(Object value);

        public Object getValue();

        public void reset();

        default Class type() {
            return Long.TYPE;
        }

        void merge(AggCall call);
    }

    public static Class getReturnType(String name) {
        return Objects.requireNonNull(map.get(name)).type();
    }

    public static AggCall getAggCall(String name) {
        switch (name) {
            case "avg":
                return new Avg();
            case "count":
                return new Count();
            case "sum":
                return new Sum();
            default:
                throw new IllegalArgumentException();
        }
    }
}