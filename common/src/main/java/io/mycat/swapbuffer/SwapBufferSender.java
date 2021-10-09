package io.mycat.swapbuffer;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.*;

import java.nio.ByteBuffer;

public abstract class SwapBufferSender {
    private ByteBuffer buffer;
    private int upstreamIndex = 0;
    private int downstreamIndex = 0;

    public SwapBufferSender(ByteBuffer buffer) {
        this.buffer = buffer;
        this.upstreamIndex = 0;
        this.downstreamIndex = 0;
    }

    Emitter<PacketRequest> downstreamEmitter;
    Emitter<PacketResponse> recycler = new Emitter<PacketResponse>() {
        @Override
        public void onNext(PacketResponse packetResponse) {
            downstreamIndex += packetResponse.getCopyCount();
            if (downstreamIndex < upstreamIndex) {
                downstreamEmitter.onNext(createRequest(buffer, downstreamIndex, upstreamIndex));
            } else if (packetResponse.getRequest() == PacketRequest.END) {
                onComplete();
            }
        }

        @Override
        public void onError(Throwable error) {

        }

        @Override
        public void onComplete() {

        }
    };
    Observable<PacketRequest> sender = Observable.create(new ObservableOnSubscribe<PacketRequest>() {
        @Override
        public void subscribe(@NonNull ObservableEmitter<PacketRequest> emitter) throws Throwable {
            downstreamEmitter = emitter;
            SwapBufferSender.this.subscribe(SwapBufferSender.this);
        }
    });

    public abstract void subscribe(SwapBufferSender observer);


    protected PacketRequest createRequest(final ByteBuffer body,
                                          final int offset,
                                          final int length) {
        return new PacketRequestImpl(body, offset, length);
    }

    protected int capacity() {
        return buffer.capacity();
    }

    public boolean onNext(byte b) {
        if (this.upstreamIndex == this.downstreamIndex&&this.upstreamIndex== capacity()) {
            this.upstreamIndex = 0;
            this.downstreamIndex = 0;
        }
        int offset = this.upstreamIndex;
       final int length= 1;
       final int nextUpstreamIndex = this.upstreamIndex+length;



        if (nextUpstreamIndex >capacity()){
            return false;
        }

        buffer.put(this.upstreamIndex, b);
        this.upstreamIndex  =nextUpstreamIndex;

        onNext(new PacketRequestImpl(buffer,offset,        this.upstreamIndex-offset));
        return true;
    }

    public void onNext(PacketRequest packetRequest) {
        downstreamEmitter.onNext(packetRequest);
    }

    public void onError(Throwable e) {
        downstreamEmitter.onError(e);
    }

    public void onComplete() {
        downstreamEmitter.onNext(PacketRequest.END);
        downstreamEmitter.onComplete();
    }
}
