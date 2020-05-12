package io.mycat;

public interface Wrapper {
    <T> T unwrap(java.lang.Class<T> iface) ;
    boolean isWrapperFor(java.lang.Class<?> iface) ;
}