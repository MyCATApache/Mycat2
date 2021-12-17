package io.ordinate.engine.function.bind;

import io.mycat.MycatDataContext;

public interface SessionVariable {
    void setSession(MycatDataContext context);
}
