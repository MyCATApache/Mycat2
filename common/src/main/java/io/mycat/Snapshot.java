package io.mycat;

public class Snapshot {
    final Long timestamp;

    public Snapshot(Long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
}