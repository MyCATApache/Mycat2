package io.mycat.config.schema;

public enum SchemaType {
    // 所有表在一个MySQL Server上（但不分片），
    DB_IN_ONE_SERVER,
    // 所有表在不同的MySQL Server上（但不分片），
    DB_IN_MULTI_SERVER,
    // 只使用基于SQL注解的路由模式（高性能但手工指定,但是分片结果不合拼）
    ANNOTATION_ROUTE,
    // 使用SQL解析的方式去判断路由
    SQL_PARSE_ROUTE;
  }
