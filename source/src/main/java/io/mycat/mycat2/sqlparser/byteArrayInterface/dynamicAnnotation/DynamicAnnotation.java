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
  final DynamicAnnotationManager manager;
  final DynamicAnnotationRuntime runtime;

  public DynamicAnnotation(DynamicAnnotationKey key, DynamicAnnotationMatch match, Function<BufferSQLContext, BufferSQLContext> actions, DynamicAnnotationManager manager, DynamicAnnotationRuntime runtime) {
    this.key = key;
    this.match = match;
    this.actions = actions;
    this.manager = manager;
    this.runtime = runtime;
  }

  public void match(BufferSQLContext context) {
    HashArray array = context.getHashArray();
    match.pick(0, array.getCount(), context,array);
//    if (match.isComplete()) {
//      actions.apply(context);
//    }
  }

  public DynamicAnnotationMatch getMatch() {
    return match;
  }
}
