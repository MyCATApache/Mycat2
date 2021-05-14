package io.mycat.vertx;

import io.mycat.calcite.logical.MycatViewSqlString;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

public interface DataNodeMapping extends Function<List<Object>, MycatViewSqlString>, Serializable {
}
