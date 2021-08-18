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
import io.mycat.*;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class HackRouter {
    SQLStatement selectStatement;
    private MycatDataContext dataContext;
    Optional<Distribution> res;
    private MetadataManager metadataManager;
    private String targetName;
    private NameMap<Partition> targetMap;

    public HackRouter(SQLStatement selectStatement, MycatDataContext context) {
        this.selectStatement = selectStatement;
        this.dataContext = context;
    }

    public boolean analyse() {
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
        this.metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        res = metadataManager.checkVaildNormalRoute(tableNames);
        if (res.isPresent()) {
            Distribution distribution = res.get();
            targetMap = NameMap.immutableCopyOf(Collections.emptyMap());

            targetMap.putAll(
                    distribution.getGlobalTables().stream().collect(Collectors.toMap(k -> k.getUniqueName(), v -> v.getDataNode())));

            Map<String, Partition> normalMap = distribution.getNormalTables().stream().collect(Collectors.toMap(k -> k.getUniqueName(), v -> v.getDataNode()));
            targetMap.putAll(normalMap);

            switch (distribution.type()) {
                case BROADCAST:
                    List<Partition> globalDataNode = distribution.getGlobalTables().get(0).getGlobalDataNode();
                    int i = ThreadLocalRandom.current().nextInt(0, globalDataNode.size());
                    targetName = globalDataNode.get(i).getTargetName();
                    return true;
                case SHARDING:
                    return false;
                case PHY: {
                    targetName = normalMap.values().iterator().next().getTargetName();
                    return true;
                }
            }
        } else {
            return false;
        }
        return false;
    }

    public Pair<String, String> getPlan() {
        if (targetMap != null) {
            selectStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    String tableName = SQLUtils.normalize(x.getTableName());
                    String schema = SQLUtils.normalize(Optional.ofNullable(x.getSchema()).orElse(dataContext.getDefaultSchema()));
                    Partition partition = targetMap.get(schema + "_" + tableName, false);
                    if (partition != null) {
                        MycatSQLExprTableSourceUtil.setSqlExprTableSource(partition.getSchema(), partition.getTable(), x);
                    }
                    return super.visit(x);
                }
            });
        }
        return Pair.of(targetName, selectStatement.toString());
    }
}
