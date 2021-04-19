package io.mycat.calcite.spm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class UpdatePlanCache {
    private final ConcurrentHashMap<String, Plan> map = new ConcurrentHashMap<>();
    private final static Logger log = LoggerFactory.getLogger(UpdatePlanCache.class);

    public Plan computeIfAbsent(String sql, Function<String, Plan> mappingFunction) {
        return map.computeIfAbsent(sql, mappingFunction);
    }

    public void clear() {
        map.clear();
    }
}
