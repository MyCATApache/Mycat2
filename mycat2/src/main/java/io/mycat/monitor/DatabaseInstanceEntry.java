package io.mycat.monitor;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Data
@ToString
@EqualsAndHashCode
public class DatabaseInstanceEntry implements LogEntry {

    public AtomicLong qpsSum = new AtomicLong();

    public AtomicLong con = new AtomicLong();

    public AtomicLong thread = new AtomicLong();

    public static final ConcurrentHashMap<String, DatabaseInstanceEntry> map = (new ConcurrentHashMap<String, DatabaseInstanceEntry>());
    public static long startTime = System.currentTimeMillis();

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
        startTime = System.currentTimeMillis();
    }

    public static DatabaseInstanceEntry stat(String ds) {
        return map.computeIfAbsent(Objects.requireNonNull(ds), s -> new DatabaseInstanceEntry());
    }

    public static DatabaseInstanceMap snapshot() {
        Map<String, DatabaseInstanceEntryViewEntry> res = new HashMap<>();
        double seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)*1.0;
        for (Map.Entry<String, DatabaseInstanceEntry> entryEntry : map.entrySet()) {
            long con = entryEntry.getValue().getCon().get();
            long sum = entryEntry.getValue().getQpsSum().get();
            long thread = entryEntry.getValue().getThread().get();

            DatabaseInstanceEntryViewEntry databaseInstanceEntry2 = new DatabaseInstanceEntryViewEntry();
            databaseInstanceEntry2.con = con;
            databaseInstanceEntry2.qps = (long)(sum/seconds);
            databaseInstanceEntry2.thread = thread;
            res.put(entryEntry.getKey(), databaseInstanceEntry2);
        }
        DatabaseInstanceMap databaseInstanceMap = new DatabaseInstanceMap();
        databaseInstanceMap.databaseInstanceMap = res;
        return databaseInstanceMap;
    }
    @EqualsAndHashCode
    public static class DatabaseInstanceEntryViewEntry implements LogEntry {

        public long qps = 0;

        public long con = 0;

        public long thread = 0;
    }

    @Data
    @EqualsAndHashCode
    public static class DatabaseInstanceMap {

        Map<String, DatabaseInstanceEntryViewEntry> databaseInstanceMap = new HashMap<>();
    }


}
