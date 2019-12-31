package io.mycat.lib;

import io.mycat.pattern.InstructionSet;
import io.mycat.lib.impl.MapLib;

public class SessionMapExport implements InstructionSet {

    public void put(int ctx, Object key, Object value) {
        MapLib.INSTANCE.put(ctx, key, value);
    }

    public <T> T get(int ctx, Object key) {
        return MapLib.INSTANCE.get(ctx, key);
    }

    public boolean getAsBool(int ctx, Object key) {
        return MapLib.INSTANCE.getAsBool(ctx, key);
    }

    public int getAsInt(int ctx, Object key) {
        return MapLib.INSTANCE.getAsInt(ctx, key);
    }

    public long getAsLong(int ctx, Object key) {
        return MapLib.INSTANCE.getAsLong(ctx, key);
    }

    public double getAsDouble(int ctx, Object key) {
        return MapLib.INSTANCE.getAsDouble(ctx, key);
    }

    public String getAsString(int ctx, Object key) {
        return MapLib.INSTANCE.getAsString(ctx, key);
    }
}