package io.mycat.swapbuffer;

import io.reactivex.rxjava3.core.Emitter;
import io.vertx.core.Future;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class SwapBufferSenderTest {

    @Test
    public void baseTest() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8192);
        FixSwapBufferSenderImpl fixSwapBufferSender = new FixSwapBufferSenderImpl(byteBuffer) {
            @Override
            public void subscribe(SwapBufferSender observer) {
                observer.onComplete();
            }
        };
        Future<Void> future = SwapBufferUtil.consume(new PacketMessageConsumer() {

            @Override
            public void onNext(PacketRequest packetMessage, Emitter<PacketResponse> emitter) {
                if (packetMessage == PacketRequest.END){
                    emitter.onComplete();
                    return;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<Void> onComplete() {
                return Future.succeededFuture();
            }
        }, fixSwapBufferSender.sender, fixSwapBufferSender.recycler);

        Assert.assertTrue(future.succeeded());
        System.out.println();
    }
    @Test
    public void baseTest2() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8192);
        ArrayList<Byte> byteArrayList = new ArrayList<>();
        FixSwapBufferSenderImpl fixSwapBufferSender = new FixSwapBufferSenderImpl(byteBuffer) {
            @Override
            public void subscribe(SwapBufferSender observer) {
                observer.onNext((byte) (1));
                observer.onComplete();
                System.out.println();
            }
        };
        Future<Void> future = SwapBufferUtil.consume(new PacketMessageConsumer() {

            @Override
            public void onNext(PacketRequest packetMessage, Emitter<PacketResponse> emitter) {
                if (packetMessage == PacketRequest.END){
                    emitter.onComplete();
                    return;
                }
                ByteBuffer body = packetMessage.asJavaByteBuffer();
                int offset = packetMessage.offset();
                int length = packetMessage.length();
                for (int i = offset; i < offset + length; i++) {
                    byteArrayList.add(body.get(i));
                }
                emitter.onNext(packetMessage.response(length));
            }

            @Override
            public Future<Void> onComplete() {
                return Future.succeededFuture();
            }
        }, fixSwapBufferSender.sender, fixSwapBufferSender.recycler);

        Assert.assertTrue(future.succeeded());
        Assert.assertEquals("[1]",byteArrayList.toString());
        System.out.println();
    }
    public static void main(String[] args) {


    }
}