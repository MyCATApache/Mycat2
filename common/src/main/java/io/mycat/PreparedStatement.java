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

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.parser.ParserException;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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
    private BindValue[] bindValues;

    public PreparedStatement(long id, SQLStatement statement, int parametersNumber) {
        this.id = id;
        this.statement = statement;
        this.parametersNumber = parametersNumber;
        this.parametersType = new int[parametersNumber];
        this.longDataMap = new HashMap<>();
    }

    public static SQLExpr fromJavaObject(Object o, TimeZone timeZone) {
        if (o == null) {
            return new SQLNullExpr();
        }

        if (o instanceof String) {
            return new SQLCharExpr((String) o);
        }

        if (o instanceof BigDecimal) {
            return new SQLDecimalExpr((BigDecimal) o);
        }

        if (o instanceof Byte || o instanceof Short || o instanceof Integer || o instanceof Long || o instanceof BigInteger) {
            return new SQLIntegerExpr((Number) o);
        }

        if (o instanceof Number) {
            return new SQLNumberExpr((Number) o);
        }

        if (o instanceof Date) {
            return new SQLTimestampExpr((Date) o, timeZone);
        }

        throw new ParserException("not support class : " + o.getClass());
    }

    public static SQLExpr fromJavaObject(Object o) {
        return fromJavaObject(o, null);
    }

    public long getId() {
        return id;
    }

    public SQLStatement getStatement() {
        return statement;
    }

    public boolean isQuery() {
        return statement instanceof SQLSelectStatement;
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
     *
     * @param values
     */
    public String getSqlByBindValue(BindValue[] values) {
        SQLStatement sqlStatement = getSQLStatementByBindValue(values);
        return sqlStatement.toString();
    }

    public SQLStatement getSQLStatementByBindValue(BindValue[] values) {
        if (this.bindValues != values) {
            throw new AssertionError();
        }
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(this.statement.toString());

        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            int index;

            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    Object o = null;
                    if (index < bindValues.length) {
                        io.mycat.BindValue value = bindValues[index++];
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
        return sqlStatement;
    }

    public BindValue[] getBindValues() {
        return bindValues;
    }

    public void setBindValues(BindValue[] bindValues) {
        this.bindValues = bindValues;
    }
}