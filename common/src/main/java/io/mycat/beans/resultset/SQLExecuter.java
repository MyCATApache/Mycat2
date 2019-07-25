package io.mycat.beans.resultset;

@FunctionalInterface
public interface SQLExecuter {
  public MycatResponse execute() throws Exception;
}