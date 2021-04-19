package org.apache.calcite.runtime;

import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public interface NewMycatDataContext extends DataContext {

    Enumerable<Object[]> getEnumerable(String node);

    Enumerable<Object[]> getEnumerable(String node,
                                       org.apache.calcite.linq4j.function.Function1 function1,
                                       java.util.Comparator comparator, int offset, int fetch);

     Observable<Object[]> getObservable(String node);


     Observable<Object[]> getObservable(String relNode,
                                               org.apache.calcite.linq4j.function.Function1 function1,
                                               java.util.Comparator comparator, int offset, int fetch);

    public Object getSessionVariable(String name);

    public Object getGlobalVariable(String name);

    public String getDatabase();

    public Long getLastInsertId();

    public Long getRowCount();

    public Long getConnectionId();

    public Object getUserVariable(String name);

    public String getCurrentUser();

    public String getUser();

    public List<Object> getParams();

    public boolean isForUpdate();
}
