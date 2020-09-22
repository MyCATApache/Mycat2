package io.mycat.calcite.sqlfunction;

import org.apache.calcite.util.TimestampString;

public class MycatTimestampString extends TimestampString {
    public MycatTimestampString(String v) {
        super(v);
    }

    public MycatTimestampString(int year, int month, int day, int h, int m, int s) {
        super(year, month, day, h, m, s);
    }
}