package io.mycat.util;

import io.mycat.MycatDataContext;
import io.mycat.upondb.MycatDBContext;

import java.util.Set;

public interface MySQLFunction {
    public Set<String> getFunctionNames();

    int getArgumentSize();

    Object eval(MycatDataContext  context, Object[] args);
}