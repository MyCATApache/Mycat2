package io.mycat.commands;

import io.mycat.beans.resultset.MycatResponse;

public interface SQLExecuterWriterHandler {
    public void writeToMycatSession(MycatResponse response)throws Exception;
}