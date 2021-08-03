package io.mycat.connection;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;

import java.util.List;

public class MycatMeta extends MetaImpl {
    public MycatMeta(AvaticaConnection connection) {
        super(connection);
    }

    @Override
    public StatementHandle prepare(ConnectionHandle connectionHandle, String s, long l) {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String s, long l, PrepareCallback prepareCallback) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String s, long l, int i, PrepareCallback prepareCallback) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle statementHandle, List<String> list) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle statementHandle, List<List<TypedValue>> list) throws NoSuchStatementException {
        return null;
    }

    @Override
    public Frame fetch(StatementHandle statementHandle, long l, int i) throws NoSuchStatementException, MissingResultsException {
        return null;
    }

    @Override
    public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> list, long l) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> list, int i) throws NoSuchStatementException {
        return null;
    }

    @Override
    public void closeStatement(StatementHandle statementHandle) {

    }

    @Override
    public boolean syncResults(StatementHandle statementHandle, QueryState queryState, long l) throws NoSuchStatementException {
        return false;
    }

    @Override
    public void commit(ConnectionHandle connectionHandle) {

    }

    @Override
    public void rollback(ConnectionHandle connectionHandle) {

    }
}
