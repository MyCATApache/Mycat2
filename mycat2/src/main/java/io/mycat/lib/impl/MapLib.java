package io.mycat.lib.impl;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chen junwen
 */
public enum  MapLib {
    INSTANCE;
    final  ConcurrentHashMap<Integer, HashMap<Object, Object>> map = new ConcurrentHashMap<>();

    private static HashMap<Object, Object> apply(Integer integer) {
        return new HashMap<>();
    }

    public void put(int ctx, Object key, Object value) {
        HashMap<Object, Object> hashMap = map.computeIfAbsent(ctx, MapLib::apply);
        hashMap.put(key, value);
    }

    public <T> T get(int ctx, Object key) {
        HashMap<Object, Object> hashMap = this.map.get(ctx);
        if (hashMap == null) {
            put(ctx, key, null);
            return null;
        }
        return (T) hashMap.get(key);
    }

    public boolean getAsBool(int ctx, Object key) {
        Object o = get(ctx, key);
        if (o == null) {
            return false;
        }
        return (Boolean) o;
    }

    public int getAsInt(int ctx, Object key) {
        Object o = get(ctx, key);
        if (o == null) {
            return 0;
        }
        return (Integer) o;
    }

    public long getAsLong(int ctx, Object key) {
        Object o = get(ctx, key);
        if (o == null) {
            return 0;
        }
        return (Long) o;
    }

    public double getAsDouble(int ctx, Object key) {
        Object o = get(ctx, key);
        if (o == null) {
            return 0;
        }
        return (Double) o;
    }

    public String getAsString(int ctx, Object key) {
        Object o = get(ctx, key);
        if (o == null) {
            return null;
        }
        return o.toString();
    }
}