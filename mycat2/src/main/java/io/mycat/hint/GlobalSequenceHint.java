package io.mycat.hint;

import io.mycat.client.Context;

public enum GlobalSequenceHint implements Hint {
    INSTANCE;
    @Override
    public void accept(Context context) {
        String nextSequence = context.getVariable("sequenceKey");
        context.putVaribale("sequenceValue","-1");
    }

    @Override
    public final String getName() {
        return "globalSequence";
    }
}