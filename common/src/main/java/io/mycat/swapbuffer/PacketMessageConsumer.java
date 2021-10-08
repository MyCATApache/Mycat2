package io.mycat.swapbuffer;

import io.reactivex.rxjava3.core.Emitter;
import io.vertx.core.Future;

public interface PacketMessageConsumer {
    public void onNext(PacketRequest packetMessage, Emitter<PacketResponse> emitter);

    public Future<Void> onComplete();
}