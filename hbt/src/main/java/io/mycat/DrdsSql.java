/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.calcite.MycatHint;
import io.mycat.calcite.spm.Constraint;
import io.mycat.calcite.spm.Plan;
import lombok.Data;
import lombok.ToString;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;


@Data
@ToString
public class DrdsSql {
    private String parameterizedSql;
    private Plan plan;
    private final List<SqlTypeName> typeNames;
    private final boolean complex;
    private List<MycatHint> hints;

    public DrdsSql(String parameterizedSqlStatement, boolean complex, List<SqlTypeName> typeNames, List<MycatHint> hints) {
        this.parameterizedSql = parameterizedSqlStatement;
        this.typeNames = typeNames;
        this.complex = complex;
        this.hints = hints;
    }

    public Constraint constraint(){
        ArrayList<String> list = new ArrayList<>(hints.size());
        for (MycatHint hint : hints) {
            String text = hint.getText();
            list.add(text);
        }
        return new Constraint(parameterizedSql,typeNames,list);
    }

    public static boolean isForUpdate(String sqlStatement) {
        return isForUpdate(SQLUtils.parseSingleMysqlStatement(sqlStatement));
    }

    public static boolean isForUpdate(SQLStatement sqlStatement) {
        final boolean forUpdate;
        if (sqlStatement instanceof SQLSelectStatement) {
            forUpdate = ((SQLSelectStatement) sqlStatement).getSelect().getFirstQueryBlock().isForUpdate();
        } else {
            forUpdate = false;
        }
        return forUpdate;
    }

    public <T extends SQLStatement> T getParameterizedStatement() {
        return (T) SQLUtils.parseSingleMysqlStatement(parameterizedSql);
    }
}