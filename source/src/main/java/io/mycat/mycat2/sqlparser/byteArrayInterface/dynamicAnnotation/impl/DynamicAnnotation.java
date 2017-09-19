package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;

import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.function.Function;

/**
 * Created by jamie on 2017/9/5.
 */

public class DynamicAnnotation {
 public final DynamicAnnotationKey key;
 public final DynamicAnnotationMatch match;
 public final Function<BufferSQLContext, BufferSQLContext> actions;
 public final DynamicAnnotationRuntime runtime;

  public DynamicAnnotation(DynamicAnnotationKey key, DynamicAnnotationMatch match, Function<BufferSQLContext, BufferSQLContext> actions,  DynamicAnnotationRuntime runtime) {
    this.key = key;
    this.match = match;
    this.actions = actions;
    this.runtime = runtime;
  }


}
