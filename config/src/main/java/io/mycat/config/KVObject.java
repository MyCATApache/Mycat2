package io.mycat.config;

import org.jetbrains.annotations.NotNull;

public interface KVObject extends Comparable<KVObject> {
    String keyName();

    String path();

    String fileName();

    @Override
    default int compareTo(@NotNull KVObject o) {
        return keyName().compareTo(o.keyName());
    }
}
