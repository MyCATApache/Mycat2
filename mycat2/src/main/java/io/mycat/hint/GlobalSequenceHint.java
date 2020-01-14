package io.mycat.hint;

import io.mycat.client.Context;
import io.mycat.plug.sequence.SequenceGenerator;

public enum GlobalSequenceHint implements Hint {
    INSTANCE;
    @Override
    public void accept(Context context) {
        String nextSequence = context.getVariable("sequenceKey");
        String next = SequenceGenerator.INSTANCE.next(nextSequence);
        context.putVaribale("sequenceValue",next);
    }

    @Override
    public final String getName() {
        return "globalSequence";
    }
}