package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;

import java.util.function.Function;

/**
 * Created by jamie on 2017/9/5.
 */

public class DynamicAnnotation {
  final DynamicAnnotationKey key;
  final DynamicAnnotationMatch match;
  final Function<BufferSQLContext, BufferSQLContext> actions;
  final DynamicAnnotationRuntime runtime;

  public DynamicAnnotation(DynamicAnnotationKey key, DynamicAnnotationMatch match, Function<BufferSQLContext, BufferSQLContext> actions,  DynamicAnnotationRuntime runtime) {
    this.key = key;
    this.match = match;
    this.actions = actions;
    this.runtime = runtime;
  }


}
