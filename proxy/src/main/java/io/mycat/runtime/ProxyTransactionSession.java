package io.mycat.runtime;

import io.mycat.MycatDataContext;
import io.mycat.ThreadUsageEnum;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

public class ProxyTransactionSession extends LocalTransactionSession {
    public ProxyTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String name() {
        return "proxy";
    }

    @Override
    public ThreadUsageEnum getThreadUsageEnum() {
        return ThreadUsageEnum.THIS_THREADING;
    }

    @Override
    public TransactionType transactionType() {
        return TransactionType.PROXY_TRANSACTION_TYPE;
    }

    @Override
    protected void callBackBegin() {

    }

    @Override
    protected void callBackCommit() {

    }

    @Override
    protected void callBackRollback() {

    }

    @Override
    @SneakyThrows
    public void check() {
        Set<Map.Entry<String, DefaultConnection>> entries = updateConnectionMap.entrySet();
        for (Map.Entry<String, DefaultConnection> entry : entries) {
            DefaultConnection value = entry.getValue();
            Connection rawConnection = value.getRawConnection();
            if(!rawConnection.getAutoCommit()){
                rawConnection.rollback();
            }
            value.close();
        }
        updateConnectionMap.clear();
        dataSourceNearness.clear();
    }
}