package io.mycat.grid;

import io.mycat.beans.resultset.SQLExecuter;

public interface ExecuterBuilder {

  SQLExecuter[] generate(byte[] sqlBytes);
}