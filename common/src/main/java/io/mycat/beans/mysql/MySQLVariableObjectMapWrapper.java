/**
 * Copyright (C) <2020>  <chenjunwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.mycat.util.JsonUtil;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Data
@NoArgsConstructor
public class MySQLVariableObjectMapWrapper {
    final Map<String, MySQLVariableObject> map = new HashMap<>();

    @SneakyThrows
    public static void main(String[] args) {
        String username = "root";
        String password = "123456";

        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", password);
        properties.put("useBatchMultiSend", "false");
        properties.put("usePipelineAuth", "false");

        String url = "jdbc:mysql://0.0.0.0:3306/db1?useServerPrepStmts=false&useCursorFetch=true&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";

        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);

        MySQLVariableObjectMapWrapper mapWrapper = new MySQLVariableObjectMapWrapper();
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            try (Statement statement = connection.createStatement()) {
                for (MySQLVariablesEnum value : MySQLVariablesEnum.values()) {
                    String[] columnNames = value.getColumnNames();

                    try {
                        for (String columnName : columnNames) {
                            ResultSet resultSet = statement.executeQuery("select " + columnName);
                            while (resultSet.next()) {
                                ResultSetMetaData metaData = resultSet.getMetaData();
                                JDBCType jdbcType = JDBCType.valueOf(metaData.getColumnType(1));
                                Object object = resultSet.getObject(1);
                                Class<?> aClass = null;
                                if (object != null) {
                                    aClass = object.getClass();
                                }
                                MySQLVariableObject mySQLVariableObject = new MySQLVariableObject(value, columnName, jdbcType, aClass, object);
                                mapWrapper.map.put(mySQLVariableObject.getColumnName(), mySQLVariableObject);
                                System.out.println(mySQLVariableObject);
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("error:" + columnNames);
                    }
                }


            }
        }
        String s = JsonUtil.toJson(mapWrapper);
        MySQLVariableObjectMapWrapper from = JsonUtil.from(s, MySQLVariableObjectMapWrapper.class);
        System.out.println(s);
    }
}