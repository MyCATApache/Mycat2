package io.mycat.swapbuffer;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Emitter;

//已经写入的数据区域
public interface PacketResponse {
    PacketRequest getRequest();

    int getCopyCount();

    void setCopyCount(int n);

    default void close() {
        getRequest().close();
    }
    public static final Emitter<PacketResponse> EMPTY_RECYCLER =  new Emitter<PacketResponse>() {
         @Override
         public void onNext(@NonNull PacketResponse value) {
              value.close();
         }

         @Override
         public void onError(@NonNull Throwable error) {

         }

         @Override
         public void onComplete() {

         }
    };
}
