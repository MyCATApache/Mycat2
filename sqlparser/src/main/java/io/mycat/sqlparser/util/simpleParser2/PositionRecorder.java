package io.mycat.sqlparser.util.simpleParser2;

import java.util.HashMap;
import java.util.Map;

public  class PositionRecorder {
            final HashMap<String, Position> map = new HashMap<>();
            Position currentPosition;

            public void startRecordName(String name, int startOffset) {
                if (currentPosition == null) {
                    Position position = map.remove(name);
                    currentPosition = position==null?new Position():position;
                    currentPosition.start = Integer.MAX_VALUE;
                }
                currentPosition.start = Math.min(currentPosition.start, startOffset);
            }

            public void record(int endOffset) {
                if (currentPosition != null) {
                    currentPosition.end = Math.max(currentPosition.end, endOffset);
                }
            }

            public void endRecordName(String name) {
                if (currentPosition != null) {
                    map.put(name, currentPosition);
                    currentPosition = null;
                }
            }
        }
