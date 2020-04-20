package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.hbt.TextUpdateInfo;
import io.mycat.upondb.PlanRunner;
import lombok.Builder;
import lombok.ToString;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@ToString
@Builder
public class SubReceiver implements Receiver {
    boolean setHasMore;
    Throwable e;
    boolean sendOk;
    SQLSelectStatement evalSimpleSql;
    private String update;
    private List<String> plan;
    SQLStatement proxyDDL;
    SQLStatement proxyShow;

    @Override
    public void setHasMore(boolean more) {
        this.setHasMore = more;
    }

    @Override
    public void sendError(Throwable e) {
        this.e = e;
    }

    @Override
    public void sendOk() {
        this.sendOk = true;
    }


    @Override
    public void evalSimpleSql(SQLSelectStatement evalSimpleSql) {
        this.evalSimpleSql = evalSimpleSql;
    }

    String proxySelect;

    @Override
    public void proxySelect(String defaultTargetName, SQLSelectStatement statement) {
        this.proxySelect = MessageFormat.format("defaultTargetName:{0},statement:{1}",
                defaultTargetName, statement);
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        this.proxySelect = MessageFormat.format("defaultTargetName:{0},statement:{1}",
                defaultTargetName, statement);
    }

    @Override
    public void eval(PlanRunner plan) {
        this.plan = plan.explain();
    }

    @Override
    public void proxyUpdate(String defaultTargetName, String statement) {
        this.update = MessageFormat.format("defaultTargetName:{0},statement:{1}",
                defaultTargetName, statement);
    }


    @Override
    public void multiUpdate(String string, Iterator<TextUpdateInfo> iterator) {
        ArrayList<TextUpdateInfo> proxyUpdate = new ArrayList<>();
        while (iterator.hasNext()) {
            proxyUpdate.add(iterator.next());
        }
        this.update = proxyUpdate.toString();
    }

    @Override
    public void multiInsert(String string, Iterator<TextUpdateInfo> apply) {
        multiUpdate(string,apply);
    }



    @Override
    public void proxyDDL(SQLStatement proxyDDL) {
        this.proxyDDL = proxyDDL;
    }



    @Override
    public void proxyShow(SQLStatement proxyShow) {
        this.proxyShow = proxyShow;
    }
}