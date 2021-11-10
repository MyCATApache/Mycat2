package io.mycat.swapbuffer;

import io.reactivex.rxjava3.core.Emitter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;

public class SwapBufferUtil {

    public  static Future<Void> consume(PacketMessageConsumer messageConsumer, Observable<PacketRequest> sender,
                                Emitter<PacketResponse> recycler) {
        return Future.future(promise -> {
            sender.subscribe(request -> {
                messageConsumer.onNext(request,recycler);
            }, throwable -> {
                recycler.onError(throwable);
                promise.tryFail(throwable);
            }, () -> {
                recycler.onComplete();
                promise.tryComplete();
            });
        });

    }

}
