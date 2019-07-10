package io.mycat.compute;

import java.util.function.Predicate;

public class SelectQueryExecuterImpl implements SelectQueryExecuter {

  @Override
  public void executeWhere(StreamBuilder streamBuilder, Session session, Expression expression) {
    Predicate<Object> predicate = predicate(expression);
    streamBuilder.addFilter(predicate);
  }

  private Predicate<Object> predicate(Expression expression) {
    return (i) -> true;
  }

  @Override
  public void executeAggregation(StreamBuilder streamBuilder, Session session,
      Expression expression) {

  }

  @Override
  public void executeMegreAggregated(StreamBuilder streamBuilder, Session session,
      Expression expression) {

  }

  @Override
  public void executeHaving(StreamBuilder streamBuilder, Session session, Expression expression) {

  }

  @Override
  public void executeOrder(StreamBuilder streamBuilder, Session session, Expression expression) {

  }

  @Override
  public void executeExpression(StreamBuilder streamBuilder, Session session,
      Expression expression) {

  }

  @Override
  public void executeSubResultset(StreamBuilder streamBuilder, Session session,
      Expression expression) {

  }

  @Override
  public void executeLimit(StreamBuilder streamBuilder, Session session, Expression expression) {

  }

  @Override
  public void executeMergeSorted(StreamBuilder streamBuilder, Session session,
      Expression expression) {

  }

  @Override
  public void executeProjection(StreamBuilder streamBuilder, Session session,
      Expression expression) {

  }
}