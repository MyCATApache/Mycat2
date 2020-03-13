package io.mycat;

public interface Wrapper {
    <T> T unwrap(java.lang.Class<T> iface) throws Exception;
    boolean isWrapperFor(java.lang.Class<?> iface) throws Exception;
}