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
package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


public class ShowCollationSQLHandler extends AbstractSQLHandler<MySqlShowCollationStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowCollationSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowCollationStatement> request, MycatDataContext dataContext, Response response) {
        MySqlShowCollationStatement mySqlShowCollationStatement = request.getAst();
        try {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            boolean okOnPrototype = false;
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(MetadataManager.getPrototype())) {
                JdbcUtils.executeQuery(connection.getRawConnection(), mySqlShowCollationStatement.toString(), Collections.emptyList());
                okOnPrototype = true;
            } catch (Throwable throwable) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("try query {} from prototype fail", mySqlShowCollationStatement);
                }
            }
            if (okOnPrototype) {
                return response.proxySelect(Collections.singletonList(MetadataManager.getPrototype()), mySqlShowCollationStatement.toString(), Collections.emptyList());
            }
        }catch (Throwable throwable){
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try query {} from prototype fail", mySqlShowCollationStatement);
            }
        }
        String sql = toNormalSQL(request.getAst());
        return DrdsRunnerHelper.runOnDrds(dataContext, DrdsRunnerHelper.preParse(sql, dataContext.getDefaultSchema()), response);
    }

    private String toNormalSQL(MySqlShowCollationStatement ast) {

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable("information_schema", "Collations");
        SimpleColumnInfo pad_attribute = tableHandler.getColumnByName("Pad_attribute");
        SQLExpr where = ast.getWhere();
        SQLExpr pattern = ast.getPattern();

        List<String[]> project = pad_attribute != null ? Arrays.asList(
                new String[]{"COLLATION_NAME", "`Collation`"},//utf8_general_ci
                new String[]{"CHARACTER_SET_NAME", "Charset"},//utf8
                new String[]{"ID", "Id"},//209
                new String[]{"IS_DEFAULT", "`Default`"},//YES
                new String[]{"IS_COMPILED", "Complied"},//YES
                new String[]{"SORTLEN", "Sortlen"},//8
                new String[]{"PAD_ATTRIBUTE", "Pad_attribute"}//PAD SPACE
        ) : Arrays.asList(
                new String[]{"COLLATION_NAME", "`Collation`"},//utf8_general_ci
                new String[]{"CHARACTER_SET_NAME", "Charset"},//utf8
                new String[]{"ID", "Id"},//209
                new String[]{"IS_DEFAULT", "`Default`"},//YES
                new String[]{"IS_COMPILED", "Complied"},//YES
                new String[]{"SORTLEN", "Sortlen"}//8
        );

        return generateSimpleSQL(project,
                "information_schema", "Collations",
                Optional.ofNullable(where).map(i -> i.toString()).orElse(null),
                Optional.ofNullable(pattern).map(i -> "COLLATION_NAME like " + i.toString()).orElse(null)).toString();
    }
}
