package org.apache.calcite.runtime;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.newquery.NewMycatConnection;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;

import java.util.List;
import java.util.Map;

public interface NewMycatDataContext extends DataContext {

    Observable<Object[]> getTableObservable(String schmea, String table);

    public Future<NewMycatConnection> getConnection(String key);

    public void recycleConnection(String key, Future<NewMycatConnection> connectionFuture);

    public List<Observable<Object[]>> getObservableList(String node);

    Observable<Object[]> getObservable(String node,
                                       org.apache.calcite.linq4j.function.Function1 function1,
                                       java.util.Comparator comparator, int offset, int fetch);


    Observable<Object[]> getObservable(String node);

    public Object getSessionVariable(String name);

    public Object getGlobalVariable(String name);

    public String getDatabase();

    public Long getLastInsertId();

    public Long getRowCount();

    public Long getConnectionId();

    public Object getUserVariable(String name);

    public String getCurrentUser();

    public String getUser();

    public DrdsSqlWithParams getDrdsSql();

    public MycatDataContext getContext();
}
