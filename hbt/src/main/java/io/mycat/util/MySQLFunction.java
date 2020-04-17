package io.mycat.util;

public interface MySQLFunction {
    public String getFunctionName();

    int getArgumentSize();

    Object eval(SQLContext context,Object[] args);
}