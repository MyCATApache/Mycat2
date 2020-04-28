package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.ExplainDetail;
import io.mycat.TextUpdateInfo;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResponse;
import lombok.Builder;
import lombok.ToString;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@ToString
@Builder
public class SubReceiver implements Response {
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
    public void sendError(String errorMessage, int errorCode) {

    }

    @Override
    public void sendExplain(Class defErrorCommandClass, Object map) {

    }

    @Override
    public void sendResultSet(RowBaseIterator rowBaseIterator) {

    }

    @Override
    public void sendResponse(MycatResponse[] mycatResponses) {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void begin() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void execute(ExplainDetail detail) {

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