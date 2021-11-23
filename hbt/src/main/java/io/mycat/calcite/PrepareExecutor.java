package io.mycat.calcite;

import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlObjectArrayRow;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.Getter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.runtime.ArrayBindable;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

@Getter
public class PrepareExecutor {
   final PrepareExecutorType type;
   final  ArrayBindable bindable1;
   final MycatRowMetaData mycatRowMetaData;

    public PrepareExecutor(PrepareExecutorType type, ArrayBindable executor, MycatRowMetaData mycatRowMetaData) {
        this.type = type;
        this.bindable1 = executor;
        this.mycatRowMetaData = mycatRowMetaData;
    }

    public static  PrepareExecutor of(PrepareExecutorType type, ArrayBindable executor, MycatRowMetaData mycatRowMetaData){
        return new PrepareExecutor(type,executor,mycatRowMetaData);
    }

   public Observable<Object[]> asObservableObjectArray( AsyncMycatDataContextImpl newMycatDataContext){
       Object bindObservable = bindable1.bindObservable(newMycatDataContext);
       if (bindObservable instanceof Observable) {
           return (Observable) bindObservable;
       } else {
           Enumerable<Object[]> enumerable = (Enumerable) bindObservable;
         return toObservable(newMycatDataContext, enumerable);
       }
   }
    public Observable<VectorSchemaRoot> asObservableVector( AsyncMycatDataContextImpl newMycatDataContext){
        throw new UnsupportedOperationException();
    }
    public RowBaseIterator asRowBaseIterator( AsyncMycatDataContextImpl newMycatDataContext){
        Observable<Object[]> bind = asObservableObjectArray(newMycatDataContext);
        Iterable<Object[]> objects = bind.blockingIterable();
        Iterator<Object[]> iterator = objects.iterator();
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        while (iterator.hasNext()){
            Object[] row = iterator.next();
            resultSetBuilder.addObjectRowPayload(row);
        }
        return resultSetBuilder.build(mycatRowMetaData);
    }

    public  Observable<MysqlPayloadObject> asMysqlPayloadObjectObservable(AsyncMycatDataContextImpl newMycatDataContext){
        return getMysqlPayloadObjectObservable(bindable1, newMycatDataContext, mycatRowMetaData);
    }


    @NotNull
    public static Observable<MysqlPayloadObject> getMysqlPayloadObjectObservable(
            ArrayBindable bindable,
            AsyncMycatDataContextImpl newMycatDataContext,
            MycatRowMetaData rowMetaData) {
        Observable<MysqlPayloadObject> rowObservable = Observable.<MysqlPayloadObject>create(emitter -> {
            emitter.onNext(new MySQLColumnDef(rowMetaData));
            try {

                Object bindObservable;
                bindObservable = bindable.bindObservable(newMycatDataContext);
                Observable<Object[]> observable;
                if (bindObservable instanceof Observable) {
                    observable = (Observable) bindObservable;
                } else {
                    Enumerable<Object[]> enumerable = (Enumerable) bindObservable;
                    observable = toObservable(newMycatDataContext, enumerable);
                }
                observable.subscribe(objects -> emitter.onNext(new MysqlObjectArrayRow(objects)),
                        throwable -> {
                            newMycatDataContext.endFuture()
                                    .onComplete(event -> emitter.onError(throwable));
                        }, () -> {
                            CompositeFuture compositeFuture = newMycatDataContext.endFuture();
                            compositeFuture.onSuccess(event -> emitter.onComplete());
                            compositeFuture.onFailure(event -> emitter.onError(event));
                        });
            } catch (Throwable throwable) {
                CompositeFuture compositeFuture = newMycatDataContext.endFuture();
                compositeFuture.onComplete(event -> emitter.onError(throwable));
            }
        });
        return rowObservable;
    }

    @NotNull
    private static Observable<Object[]> toObservable(AsyncMycatDataContextImpl context, Enumerable<Object[]> enumerable) {
        Observable<Object[]> observable;
        observable = Observable.create(emitter1 -> {
            Future future;
            try (Enumerator<Object[]> enumerator = enumerable.enumerator()) {
                while (enumerator.moveNext()) {
                    emitter1.onNext(enumerator.current());
                }
                future = Future.succeededFuture();
            } catch (Throwable throwable) {
                future = Future.failedFuture(throwable);
            }
            CompositeFuture.join(future, context.endFuture())
                    .onSuccess(event -> emitter1.onComplete())
                    .onFailure(event -> emitter1.onError(event));
        });
        return observable;
    }
}
