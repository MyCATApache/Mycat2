package io.mycat.compute;

public interface SelectQueryExecuter {

  void executeWhere(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeAggregation(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeMegreAggregated(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeHaving(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeOrder(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeExpression(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeSubResultset(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeLimit(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeMergeSorted(StreamBuilder streamBuilder, Session session, Expression expression);

  void executeProjection(StreamBuilder streamBuilder, Session session, Expression expression);
}