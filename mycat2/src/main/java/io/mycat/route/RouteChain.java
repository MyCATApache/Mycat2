package io.mycat.route;

@FunctionalInterface
public interface RouteChain<T> {
    public  boolean handle(T t);
}