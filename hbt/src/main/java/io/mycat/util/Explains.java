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

import io.mycat.ResultSetProvider;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.beans.resultset.MycatResultSetResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Getter
public class Explains {
    final String sql;
    final String prepareCompute;
    final String resultSetRowType;
    final String hbt;
    final String rel;

    @AllArgsConstructor
    @ToString
    public static class PrepareCompute {
        String targetName;
        String sql;
        List<Object> params;

        @Override
        public String toString() {
            return
                    "targetName='" + targetName + "\'\n" +
                            ", sql='" + sql + "\'\n" +
                            ", params=" + params + "\n";
        }
    }

    public Explains(String sql, String prepareCompute, String resultSetRowType, String hbt, String rel) {
        this.sql = sql;
        this.prepareCompute = prepareCompute;
        this.resultSetRowType = resultSetRowType;
        this.hbt = hbt;
        this.rel = rel;
    }

    public static List<String> explain(String sql, String prepareCompute, String resultSetRowType, String hbt, String rel) {
        return new Explains(sql, prepareCompute, resultSetRowType, hbt, rel).explain();
    }

    List<String> explain() {
        ArrayList<String> list = new ArrayList<>();
        if (!StringUtil.isEmpty(sql)) {
            list.add("sql:");
            list.addAll(Arrays.asList(sql.split("\n")));
        }
        if (!StringUtil.isEmpty(prepareCompute)) {
            list.add("");
            list.add("prepareCompute:");
            list.addAll(Arrays.asList(prepareCompute.split("\n")));
        }
        if (!StringUtil.isEmpty(resultSetRowType)) {
            list.add("");
            list.add("resultSetRowType:");
            list.addAll(Arrays.asList(resultSetRowType.split("\n")));
        }
        if (!StringUtil.isEmpty(hbt)) {
            list.add("");
            list.add("hbt:");
            list.addAll(Arrays.asList(hbt.split("\n")));
        }
        if (!StringUtil.isEmpty(rel)) {
            list.add("");
            list.add("rel:");
            list.addAll(Arrays.asList(rel.split("\n")));
        }
        return list;
    }
}