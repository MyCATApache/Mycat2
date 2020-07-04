package io.mycat.util;

import java.util.Set;

public interface MySQLFunction {
    public Set<String> getFunctionNames();

    int getArgumentSize();

    Object eval(SQLContext context,Object[] args);
}