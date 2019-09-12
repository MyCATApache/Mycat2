package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;

public class PositionRecorder {
    final HashMap<String, Position> map = new HashMap<>();
    Position currentPosition;
    String name;

    public void startRecordName(String name, int startOffset) {
        if (this.name == null || !this.name.equals(name)) {
            this.currentPosition = map.get(name);
            this.name = name;
        }
        if (currentPosition == null) {
            map.put(name, currentPosition = new Position());
            currentPosition.start = Integer.MAX_VALUE;
        }
        currentPosition.start = Math.min(currentPosition.start, startOffset);
    }

    public void record(int endOffset) {
        currentPosition.end = Math.max(currentPosition.end, endOffset);
    }

}
