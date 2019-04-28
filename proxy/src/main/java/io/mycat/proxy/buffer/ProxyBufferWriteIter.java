package io.mycat.proxy.buffer;

//        while (iter.hasNext()){
//            if(iter.write(currentProxyBuffer())){
//                iter.writeFinished(currentProxyBuffer());
//                }else {
//                break;
//              }
//            }
public interface ProxyBufferWriteIter {
    boolean hasNext();

    boolean write(ProxyBuffer buffer);

    void writeFinished(ProxyBuffer buffer);
}
