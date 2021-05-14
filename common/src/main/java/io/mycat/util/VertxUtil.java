package io.mycat.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.future.FutureInternal;
import io.vertx.core.impl.future.PromiseImpl;
import io.vertx.core.impl.future.PromiseInternal;

/**
 * vertx异步工具类
 * wangzihaogithub 2021年1月23日
 */
public class VertxUtil {
    /**
     * 返回一个已完成的结果
     * @param throwable throwable
     * @return 已完成的结果
     */
    public static AsyncResult<Void> newFailResult(Throwable throwable){
        return newFailPromise(throwable);
    }

    /**
     * 返回一个未完成的延迟结果
     * @param <T> 泛型
     * @return 未完成的延迟结果
     */
    public static <T>PromiseInternal<T> newPromise(){
        PromiseImpl<T> promise = new PromiseImpl<>(context());
        return promise;
    }

    public static ContextInternal context(){
        return ContextInternal.current();
    }

    /**
     * 返回一个已完成的结果
     * @param throwable throwable
     * @return 已完成的结果
     */
    public static PromiseInternal<Void> newFailPromise(Throwable throwable){
        PromiseImpl<Void> promise = new PromiseImpl<>(context());
        promise.tryFail(throwable);
        return promise;
    }

    /**
     * 返回一个已完成的结果
     * @return 已完成的结果
     */
    public static PromiseInternal<Void> newSuccessPromise(){
        PromiseImpl<Void> promise = new PromiseImpl<>(context());
        promise.tryComplete();
        return promise;
    }

    /**
     * 基本上参数future 一定是PromiseInternal的实现类, 不会走到第二种,第三种情况
     * @param future
     * @param <T>
     * @return
     */
    public static <T>PromiseInternal<T> castPromise(Future<T> future){
        if(future instanceof PromiseInternal){
            // 第一种情况
            return (PromiseInternal<T>) future;
        }else if(future instanceof FutureInternal){
            // 第二种情况
            PromiseImpl<T> promise = new PromiseImpl<>(context());
            ((FutureInternal<T>) future).addListener(promise);
            return promise;
        }else {
            // 第三种情况
            PromiseImpl<T> promise = new PromiseImpl<>(context());
            future.onFailure(promise::onFailure);
            future.onSuccess(promise::onSuccess);
            return promise;
        }
    }
}
