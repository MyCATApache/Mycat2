package io.mycat.hint;

import io.mycat.client.Context;

import java.util.function.Consumer;


public interface Hint extends Consumer<Context> {
    String getName();
}