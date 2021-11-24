/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.sqlhandler;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.statement.*;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.calcite.ExecutorProvider;
import io.mycat.calcite.table.GlobalTableHandler;
import io.mycat.calcite.table.NormalTableHandler;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.prototypeserver.mysql.MySQLResultSet;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.ClassUtil;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.vertx.core.Future;
import lombok.EqualsAndHashCode;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

@EqualsAndHashCode
public abstract class AbstractSQLHandler<Statement extends SQLStatement> implements SQLHandler<Statement> {
    private final Class statementClass;
    public final static String DDL_LOCK = "DDL_LOCK";

    public AbstractSQLHandler() {
        Class<?> statement = ClassUtil.findGenericType(this, AbstractSQLHandler.class, "Statement");
        Objects.requireNonNull(statement);
        statementClass = statement;
    }

    public AbstractSQLHandler(Class statementClass) {
        this.statementClass = statementClass;
    }

    @Override
    public Future<Void> execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) {
        try {
            onExecuteBefore(request, dataContext, response);
            return onExecute(request, dataContext, response);
        } finally {
            onExecuteAfter(request, dataContext, response);
        }
    }

    protected void onExecuteBefore(SQLRequest<Statement> request, MycatDataContext dataContext, Response respons) {
    }

    protected abstract Future<Void> onExecute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response);

    protected void onExecuteAfter(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) {


    }

    public Class getStatementClass() {
        return statementClass;
    }

    public void resolveSQLExprTableSource(SQLExprTableSource tableSource, MycatDataContext dataContext) {
        if (tableSource.getSchema() == null) {
            String defaultSchema = checkDefaultSchemaNotNull(dataContext);
            MycatSQLExprTableSourceUtil.setSchema(defaultSchema, tableSource);
        }
    }

    @NotNull
    public String checkDefaultSchemaNotNull(MycatDataContext dataContext) {
        String defaultSchema = dataContext.getDefaultSchema();
        if (defaultSchema == null) {
            throw new MycatException("please use schema");
        }
        return defaultSchema;
    }

    public void executeOnDataNodes(SQLStatement sqlStatement, JdbcConnectionManager connectionManager, Collection<Partition> partitions, SQLExprTableSource tableSource) {
        HashSet<String> set = new HashSet<>();
        for (Partition partition : partitions) {
            MycatSQLExprTableSourceUtil.setSqlExprTableSource(partition.getSchema(), partition.getTable(), tableSource);
            String sql = sqlStatement.toString();
            try (DefaultConnection connection = connectionManager.getConnection(partition.getTargetName())) {
                DatasourceConfig config = connection.getDataSource().getConfig();
                ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(config.getUrl());
                HostInfo hostInfo = connectionUrlParser.getHosts().get(0);
                String ip = hostInfo.getHost();
                String port = hostInfo.getPort() + "";
                if (set.add(ip + ":" + port + ":" + sql)) {
                    connection.executeUpdate(sql, false);
                }
            }
        }
    }

    public Set<Partition> getDataNodes(TableHandler tableHandler) {
        List<Partition> partitions;
        switch (tableHandler.getType()) {
            case SHARDING: {
                ShardingTableHandler handler = (ShardingTableHandler) tableHandler;
                partitions = handler.dataNodes();
                break;
            }
            case GLOBAL: {
                GlobalTableHandler handler = (GlobalTableHandler) tableHandler;
                partitions = handler.getGlobalDataNode();
                break;
            }
            case NORMAL: {
                NormalTableHandler handler = (NormalTableHandler) tableHandler;
                partitions = Collections.singletonList(handler.getDataNode());
                break;
            }
            case CUSTOM:
            default:
                throw MycatErrorCode.createMycatException(MycatErrorCode.ERR_NOT_SUPPORT, "alter custom table supported");
        }
        return new HashSet<>(partitions);
    }

    public PrototypeService getPrototypeService() {
        if (MetaClusterCurrent.exist(PrototypeService.class)) {
            return MetaClusterCurrent.wrapper(PrototypeService.class);
        }
        return new PrototypeService();
    }

    public static String generateSimpleSQL(List<String[]> projects, String schema, String table, SQLExpr where, SQLExpr like) {
        return generateSimpleSQL(projects, schema, table, null, Optional.ofNullable(where).map(i -> i.toString()).orElse(null), Optional.ofNullable(like).map(i -> i.toString()).orElse(null));
    }


    public static String generateSimpleSQL(List<String[]> projects, String schema, String table, SQLExpr where0, SQLExpr where, SQLExpr like) {
        return generateSimpleSQL(projects, schema, table, Optional.ofNullable(where0).map(i -> i.toString()).orElse(null), Optional.ofNullable(where).map(i -> i.toString()).orElse(null), Optional.ofNullable(like).map(i -> i.toString()).orElse(null));
    }

    public static String generateSimpleSQL(List<String[]> projects, String schema, String table, String where, String like) {
        return generateSimpleSQL(projects, schema, table, null, where, like);
    }

    public static String generateSimpleSQL(List<String[]> projects, String schema, String table, String where0, String where, String like) {
        String s = Optional.ofNullable(where0).orElse(" true ");
        if (like != null) {
            s +=" and "+ like;
        }
        String sql = "select *" + " from (select " +
                projects.stream().map(i -> "" + i[0] + "" + " as " + i[1] + "").collect(Collectors.joining(",")) + "  from `" + schema + "`.`" + table + "` where " +s
                +") " +table+
                " where " + Optional.ofNullable(where).orElse(" true ");
        return sql;
    }

    public static RowBaseIterator runAsRowIterator(MycatDataContext dataContext, String sql) {
        ExecutorProvider executorProvider = MetaClusterCurrent.wrapper(ExecutorProvider.class);
        return executorProvider.runAsObjectArray(dataContext, sql);
    }

}
