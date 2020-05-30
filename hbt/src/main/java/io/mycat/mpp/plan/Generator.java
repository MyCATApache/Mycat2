package io.mycat.mpp.plan;

import java.util.Optional;

@FunctionalInterface
public interface Generator<T> {
    Optional<T> next();
}