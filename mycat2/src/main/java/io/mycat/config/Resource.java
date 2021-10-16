package io.mycat.config;

import com.alibaba.druid.util.JdbcUtils;
import lombok.SneakyThrows;

import java.io.Closeable;
import java.lang.reflect.Method;

public class Resource<T> {
    private T object;
    private boolean borrow;

    public Resource(T object, boolean borrow) {
        this.object = object;
        this.borrow = borrow;
    }

    public static <T> Resource<T> of(T object, boolean borrow) {
        return new Resource<>(object, borrow);
    }

    @SneakyThrows
    public void giveup() {
        if (!borrow && object != null) {
            if (object instanceof Closeable) {
                JdbcUtils.close((Closeable) object);
            } else if (object instanceof AutoCloseable) {
                ((AutoCloseable) object).close();
            } else {
                try{
                    Method method =  object.getClass().getMethod("close");
                    if (method != null) {
                        method.invoke(object);
                    }
                }catch (Throwable ignored){

                }
            }
        }
    }

    public T get() {
        return object;
    }

    public boolean isBorrow(){
        return borrow;
    }
}
