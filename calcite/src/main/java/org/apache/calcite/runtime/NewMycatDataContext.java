package org.apache.calcite.runtime;

import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Queue;

public interface NewMycatDataContext extends DataContext {

    public void allocateResource();

    Enumerable<Object[]> getEnumerable(org.apache.calcite.rel.RelNode node);

    List<Enumerable<Object[]>> getEnumerables(org.apache.calcite.rel.RelNode node);

    io.reactivex.rxjava3.core.Observable <Object[]> getObservable(org.apache.calcite.rel.RelNode node);
    Queue<List<Observable<Object[]>>> getObservables(RelNode node);

    public Object getSessionVariable(String name);

    public Object getGlobalVariable(String name);

    public String getDatabase();

    public Long getLastInsertId();

    public Long getConnectionId();

    public Object getUserVariable(String name);

    public String getCurrentUser();

    public String getUser();

    public List<Object> getParams();

    public boolean isForUpdate();
}
