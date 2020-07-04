/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat;

import com.alibaba.fastsql.sql.ast.SQLReplaceable;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLExprUtils;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat, CrazyPig
 */
public class PreparedStatement {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatement.class);
    private long id;
    private SQLStatement statement;//不可修改
    private int parametersNumber;
    private int[] parametersType;
    /**
     * 存放COM_STMT_SEND_LONG_DATA命令发送过来的字节数据
     * <pre>
     * key : param_id
     * value : byte data
     * </pre>
     */
    private Map<Long, ByteArrayOutputStream> longDataMap;

    public PreparedStatement(long id, SQLStatement statement, int parametersNumber) {
        this.id = id;
        this.statement = statement;
        this.parametersNumber = parametersNumber;
        this.parametersType = new int[parametersNumber];
        this.longDataMap = new HashMap<>();
    }

    public long getId() {
        return id;
    }

    public SQLStatement getStatement() {
        return statement;
    }

    public int getParametersNumber() {
        return parametersNumber;
    }

    public int[] getParametersType() {
        return parametersType;
    }

    public boolean hasLongData(long paramId) {
        return longDataMap.containsKey(paramId);
    }

    public ByteArrayOutputStream getLongData(long paramId) {
        return longDataMap.get(paramId);
    }

    /**
     * COM_STMT_RESET命令将调用该方法进行数据重置
     */
    public void resetLongData() {
        for (ByteArrayOutputStream value : longDataMap.values()) {
            if (value != null) {
                value.reset();
            }
        }
    }

    /**
     * 追加数据到指定的预处理参数
     *
     * @param paramId
     * @param data
     * @throws IOException
     */
    @SneakyThrows
    public void appendLongData(long paramId, byte[] data) {
        ByteArrayOutputStream outputStream = longDataMap.computeIfAbsent(paramId, aLong -> new ByteArrayOutputStream());
        outputStream.write(data);
    }

    /**
     * 组装sql语句,替换动态参数为实际参数值
     */
    public String getSqlByBindValue(io.mycat.BindValue[] values) {
        SQLStatement sqlStatement = statement.clone();
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            int index;
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    Object o = null;
                    if (index < values.length) {
                        io.mycat.BindValue value = values[index++];
                        if (!value.isNull) {
                            o = value.getJavaObject();
                        }
                    }
                    SQLReplaceable parent = (SQLReplaceable) x.getParent();
                    parent.replace(x, SQLExprUtils.fromJavaObject(o));
                }
                super.endVisit(x);
            }
        });
        return sqlStatement.toString();
    }

}