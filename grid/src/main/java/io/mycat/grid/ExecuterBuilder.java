package io.mycat.grid;

import io.mycat.beans.resultset.MycatResponse;

public interface ExecuterBuilder {

  MycatResponse[] generate(byte[] sqlBytes);
}