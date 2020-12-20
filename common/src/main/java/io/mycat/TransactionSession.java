/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.TransactionType;

import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * @author Junwen Chen
 **/
public interface TransactionSession extends Dumpable {

    public final static String LOCAL = "local";
    public final static String XA = "xa";
    public final static String PROXY = "proxy";

    String name();

    void setTransactionIsolation(int transactionIsolation);

    void begin();

    void commit();

    void rollback();

    boolean isInTransaction();

    void setAutocommit(boolean autocommit);

    boolean isAutocommit();

    MycatConnection getConnection(String targetName);

    public int getServerStatus();

    boolean isReadOnly();

    public void setReadOnly(boolean readOnly);

    int getTransactionIsolation();

    ThreadUsageEnum getThreadUsageEnum();

    void clearJdbcConnection();

    void close();

    String resolveFinalTargetName(String targetName);

    TransactionType transactionType();

    /**
     * 模拟autocommit = 0 时候自动开启事务
     */
    public void ensureTranscation();

    public void addCloseResource(AutoCloseable closeable);

    String getTxId();
}