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
package io.mycat.util;


import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.MycatRowMetaDataImpl;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class SQL2ResultSetUtil {

    public static MycatRowMetaData getMycatRowMetaData(MySqlCreateTableStatement mySqlCreateTableStatement) {
        return new MycatRowMetaDataImpl(mySqlCreateTableStatement);
    }
    @SneakyThrows
    public static MycatRowMetaData getMycatRowMetaData(JdbcConnectionManager jdbcConnectionManager,
                                                       String prototypeServer,
                                                       String schema,String table) {
        try(DefaultConnection connection = jdbcConnectionManager.getConnection(prototypeServer)){
            Connection rawConnection = connection.getRawConnection();
            try(Statement statement = rawConnection.createStatement()){
                statement.setMaxRows(0);
                ResultSet resultSet = statement.executeQuery("select * from "+schema+"."+table+" where 0");
                resultSet.next();
                return new CopyMycatRowMetaData(new JdbcRowMetaData(resultSet.getMetaData()));
            }
        }
    }
}