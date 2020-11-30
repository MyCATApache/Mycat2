package io.mycat.drdsrunner;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.google.common.collect.ImmutableList;
import io.mycat.MycatConnection;
import io.mycat.MycatDataContext;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.*;
import io.mycat.hbt4.executor.MycatInsertExecutor;
import io.mycat.hbt4.executor.MycatUpdateExecutor;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.metadata.SchemaHandler;
import io.mycat.mpp.Row;
import io.mycat.proxy.session.SimpleTransactionSessionRunner;
import io.mycat.runtime.MycatDataContextImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DrdsTest {
    HashMap<String,SchemaHandler> schemas = new HashMap<>();
    DrdsRunner drdsRunner = new DrdsRunner(() -> schemas, PlanCache.INSTANCE);

    @Test
    public void select1(){
        String sql = "select 1";
        Assert.assertEquals("[{1=1}]", runAsString(sql));
    }

    @Test
    public void lpadTest(){
        String sql = "select 1";
        Assert.assertEquals("[{1=1}]", runAsString(sql));
    }

    private String runAsString(String sql) {
        return Objects.toString(run(sql));
    }

    private List<Map<String,Object>> run(String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MycatDataContext context = new MycatDataContextImpl(new SimpleTransactionSessionRunner());
        Iterable<DrdsSql> drdsSqls = drdsRunner.preParse(Collections.singletonList(sqlStatement), Collections.emptyList());
        Iterable<DrdsSql> iterable = drdsRunner.convertToMycatRel(drdsSqls, context);
        DrdsSql drdsSql = iterable.iterator().next();
        ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(new DataSourceFactory() {
            @Override
            public void open() {

            }


            @Override
            public Map<String, MycatConnection> getConnections(List<String> targets) {
                return null;
            }

            @Override
            public void registered(ImmutableList<String> asList) {

            }


            @Override
            public MycatConnection getConnection(String key) {
                return null;
            }

            @Override
            public List<MycatConnection> getTmpConnections(List<String> targets) {
                return null;
            }

            @Override
            public void recycleTmpConnections(List<MycatConnection> connections) {

            }

            @Override
            public void close() throws Exception {

            }
        }, new TempResultSetFactoryImpl()) {

            @Override
            public void implementRoot(MycatRel rel, List<String> aliasList) {

            }
        };
        new ExecutorImplementorImpl(new DataSourceFactory() {
            @Override
            public void open() {

            }


            @Override
            public Map<String, MycatConnection> getConnections(List<String> targets) {
                return null;
            }

            @Override
            public void registered(ImmutableList<String> asList) {

            }


            @Override
            public MycatConnection getConnection(String key) {
                return null;
            }

            @Override
            public List<MycatConnection> getTmpConnections(List<String> targets) {
                return null;
            }

            @Override
            public void recycleTmpConnections(List<MycatConnection> connections) {

            }

            @Override
            public void close() throws Exception {

            }
        }, new TempResultSetFactoryImpl()) {

            @Override
            public void implementRoot(MycatRel rel, List<String> aliasList) {

                return;
            }
        }.setParams(drdsSql.getParams());
        MycatRel mycatRel = (MycatRel) drdsSql.getRelNode();
        Executor executor = mycatRel.implement(executorImplementor);
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        List<String> aliasList = drdsSql.getAliasList();
        executor.open();
        Row row;
        while ((row=executor.next())!=null){
            Object[] values = row.getValues();
            Map<String,Object> map = new HashMap<>();
            for (int i = 0; i < aliasList.size(); i++) {
                map.put(aliasList.get(i),values[i]);
            }
            list.add(map);
        }
        return list;
    }

    public static void main(String[] args) {


    }
}
