package io.mycat.beans.log.monitor;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class RWEntry implements LogEntry {
    String replica;
    long master;
    long slave;

    public RWEntry(String replica, long master, long slave) {
        this.replica = replica;
        this.master = master;
        this.slave = slave;
    }

    public static class Entry {
        public final AtomicLong SLAVE = new AtomicLong();
        public final AtomicLong MASTER = new AtomicLong();
    }

    public static final ConcurrentHashMap<String, Entry> map = (new ConcurrentHashMap<String, Entry>());


    public static void stat(String replica, boolean master) {
        Entry entry = map.computeIfAbsent(replica, s -> new Entry());
        if (master){
            entry.MASTER.getAndIncrement();
        }else{
            entry.SLAVE.getAndIncrement();
        }
    }

    public static Map<String,RWEntry> snapshot() {
        Map<String,RWEntry> rwEntryMap = new HashMap<>();
        for (Map.Entry<String, Entry> entry : map.entrySet()) {
            Entry value = entry.getValue();
            rwEntryMap.put(entry.getKey(),new RWEntry(entry.getKey(), value.MASTER.get(), value.SLAVE.get()));
        }
        return rwEntryMap;
    }

    public void reset() {
        map.clear();
    }


}
