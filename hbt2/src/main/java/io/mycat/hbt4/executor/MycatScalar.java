package io.mycat.hbt4.executor;


import io.mycat.mpp.Row;

public interface MycatScalar {
    void execute(Row input, Row output);
}