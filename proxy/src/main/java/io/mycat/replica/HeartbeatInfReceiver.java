package io.mycat.replica;

@FunctionalInterface
public interface HeartbeatInfReceiver<T extends HeartbeatDetector> {
    boolean apply(T inf);

   static HeartbeatInfReceiver identity() {
        return (t) -> false;
    }
}
