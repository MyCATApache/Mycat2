package io.mycat.hbt4;

import java.util.List;

public class MycatContext {
    public Object[] values;
    public Object[] slots;
    public List<Object> params;
    public boolean forUpdate;

    public Object get(String name) {
        if (name.length() > 1 && name.charAt(0) == '?') {
            return params.get(Integer.parseInt(name.substring(1)));
        } else {
            return null;
        }
    }

    public Object[] getSlots() {
        return slots;
    }
}