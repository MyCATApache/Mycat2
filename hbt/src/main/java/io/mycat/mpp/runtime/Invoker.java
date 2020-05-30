package io.mycat.mpp.runtime;

@FunctionalInterface
public interface Invoker {
    public Object invokeWithArguments(Object... arguments)throws Throwable;
}