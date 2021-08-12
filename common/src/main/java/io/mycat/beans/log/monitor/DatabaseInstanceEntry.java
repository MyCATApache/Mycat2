package io.mycat.beans.log.monitor;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class DatabaseInstanceEntry implements LogEntry {

    String datasourceName;

    public AtomicLong qpsSum = new AtomicLong();

    public AtomicLong con = new AtomicLong();

    public AtomicLong thread = new AtomicLong();

    public static final ConcurrentHashMap<String, DatabaseInstanceEntry> map = (new ConcurrentHashMap<String, DatabaseInstanceEntry>());

    public void plusQps() {
        qpsSum.getAndIncrement();
    }

    public void plusCon() {
        con.getAndIncrement();
    }

    public void decCon() {
        con.decrementAndGet();
    }

    public void plusThread() {
        thread.getAndIncrement();
    }

    public void decThread() {
        thread.decrementAndGet();
    }


    public static void reset() {
        map.clear();
    }

    public static DatabaseInstanceEntry stat(String ds) {
        return map.computeIfAbsent(ds, s -> new DatabaseInstanceEntry());
    }

    public static Map<String, DatabaseInstanceEntry> snapshot() {
        return new HashMap<>(map);
    }

}
