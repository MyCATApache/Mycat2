package cn.lightfish.sql.ast.function;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum FunctionManager {
  INSTANCE;
  ConcurrentMap<String, Function> map = new ConcurrentHashMap<>();
}