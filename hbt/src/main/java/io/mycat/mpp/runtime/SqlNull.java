package io.mycat.mpp.runtime;

public final class SqlNull {
    public static final SqlNull SINGLETON = new SqlNull();

    @Override
    public String toString() {
        return "NULL";
    }

    public int getType() {
        return 0;
    }
}