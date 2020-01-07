package io.mycat.beans.mycat;

public enum TransactionType {
    PROXY_TRANSACTION_TYPE("proxy"),
    JDBC_TRANSACTION_TYPE("xa");

    private String name;

    public final static TransactionType DEFAULT = JDBC_TRANSACTION_TYPE;

    TransactionType(String name) {
        this.name = name;
    }

    public static TransactionType parse(String name) {
        return TransactionType.JDBC_TRANSACTION_TYPE.name.equalsIgnoreCase(name) ? TransactionType.JDBC_TRANSACTION_TYPE : TransactionType.PROXY_TRANSACTION_TYPE;
    }
}