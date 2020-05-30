package io.mycat.mpp;

public interface NullPointer {
    void setNullValue(boolean value);

    public final NullPointer DEFAULT = value -> {
    };
}