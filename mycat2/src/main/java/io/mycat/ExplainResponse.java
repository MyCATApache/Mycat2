package io.mycat;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.util.Response;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class ExplainResponse implements Response {
    final Class defClass;
    final Response response;

    public ExplainResponse(Class defClass, Response response) {
        this.defClass = defClass;
        this.response = response;
    }

    @Override
    public void setHasMore(boolean more) {
        throw new UnsupportedOperationException("unsupported explain multi statement");
    }

    @Override
    public void sendError(Throwable e) {
        response.sendExplain(defClass, e);
    }

    @Override
    public void sendOk() {
        response.sendExplain(defClass, "send ok");
    }

    @Override
    public void evalSimpleSql(SQLStatement evalSimpleSql) {
        response.sendExplain(defClass, "evalSimpleSql=" + evalSimpleSql);
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        response.sendExplain(defClass, MessageFormat.format("proxySelect:{0} {1} ", defaultTargetName, statement));
    }

    @Override
    public void proxyUpdate(String defaultTargetName, String proxyUpdate) {
        response.sendExplain(defClass, MessageFormat.format("proxyUpdate:{0} {1} ", defaultTargetName, proxyUpdate));
    }

    @Override
    public void proxyDDL(SQLStatement statement) {
        response.sendExplain(defClass, MessageFormat.format("proxyDDL:{0} ", statement));
    }

    @Override
    public void proxyShow(SQLStatement statement) {
        response.sendExplain(defClass, MessageFormat.format("proxyShow:{0} ", statement));
    }

    @Override
    public void multiUpdate(String string, Iterator<TextUpdateInfo> apply) {
        response.sendExplain(defClass, MessageFormat.format("multiUpdate: targetName={0} {1}", string, ReceiverImpl.toMap(apply)));
    }

    @Override
    public void multiInsert(String string, Iterator<TextUpdateInfo> apply) {
        response.sendExplain(defClass, MessageFormat.format("multiInsert:targetName={0} {1} ", string, ReceiverImpl.toMap(apply)));
    }

    @Override
    public void sendError(String errorMessage, int errorCode) {
        response.sendExplain(defClass, MessageFormat.format("sendError:errorMessage={0} {1} ", errorMessage, errorCode));
    }

    @Override
    public void sendExplain(Class defErrorCommandClass, Object map) {
       response.sendExplain(defErrorCommandClass,map);
    }

    @Override
    public void sendResultSet(RowBaseIterator rowBaseIterator, Supplier<List<String>> explainSupplier) {
        response.sendExplain(defClass,explainSupplier.get());
    }

    @Override
    public void sendResponse(MycatResponse[] mycatResponses, Supplier<List<String>> explainSupplier) {
        response.sendExplain(defClass,explainSupplier.get());
    }

    @Override
    public void rollback() {
        response.sendExplain(defClass, Arrays.asList("rollback"));
    }

    @Override
    public void begin() {
        response.sendExplain(defClass, Arrays.asList("begin"));
    }

    @Override
    public void commit() {
        response.sendExplain(defClass, "commit");
    }

    @Override
    public void execute(ExplainDetail detail) {
        response.sendExplain(defClass, detail);
    }
}