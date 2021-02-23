package org.apache.calcite.runtime;

import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public interface NewMycatDataContext extends DataContext {

    Enumerable<Object[]> getEnumerable(org.apache.calcite.rel.RelNode node);

    List<Enumerable<Object[]>> getEnumerables(org.apache.calcite.rel.RelNode node);

    default Observable<Object[]> getObservable(org.apache.calcite.rel.RelNode node){
        throw new UnsupportedOperationException();
    }

    default List<Observable<Object[]>> getObservables(RelNode node){
        throw new UnsupportedOperationException();
    }


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
