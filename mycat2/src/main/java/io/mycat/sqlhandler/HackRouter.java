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
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class HackRouter {
    SQLStatement selectStatement;
    private MycatDataContext dataContext;
    private  NameMap<MetadataManager.SimpleRoute> singleTables;

    public HackRouter(SQLStatement selectStatement, MycatDataContext context) {
        this.selectStatement = selectStatement;
        this.dataContext = context;
    }
    public boolean analyse(){
        Set<Pair<String, String>> tableNames = new HashSet<>();
        selectStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource x) {
                String tableName = x.getTableName();
                if (tableName != null) {
                    String schema = Optional.ofNullable(x.getSchema()).orElse(dataContext.getDefaultSchema());
                    if (schema == null) {
                        throw new MycatException("please use schema;");
                    }
                    tableNames.add(Pair.of(schema, SQLUtils.normalize(tableName)));
                }
                return super.visit(x);
            }
        });
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        this.singleTables = new NameMap<>();
        boolean singleTarget = metadataManager.checkVaildNormalRoute(tableNames, singleTables);
        return singleTarget;
    }

    public Pair<String,String> getPlan(){
        String[] targetName = new String[1];
        selectStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource x) {
                String tableName = x.getTableName();
                if (tableName != null) {
                    tableName = SQLUtils.normalize(tableName);
                    MetadataManager.SimpleRoute normalTable = singleTables.get(tableName);
                    if (normalTable != null) {
                        String schema = Optional.ofNullable(x.getSchema()).orElse(dataContext.getDefaultSchema());
                        if (normalTable.getSchemaName().equalsIgnoreCase(schema)) {
                            MycatSQLExprTableSourceUtil.setSqlExprTableSource(normalTable.getSchemaName(),normalTable.getTableName(),x);
                            targetName[0] = normalTable.getTargetName();
                        }
                    }
                }
                return super.visit(x);
            }
        });
        return Pair.of(targetName[0], selectStatement.toString());
    }
}
