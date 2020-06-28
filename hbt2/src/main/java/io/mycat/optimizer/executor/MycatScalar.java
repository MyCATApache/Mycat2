package io.mycat.optimizer.executor;

import io.mycat.optimizer.Row ;

public interface MycatScalar {
    void execute(Row input, Row  output);
}