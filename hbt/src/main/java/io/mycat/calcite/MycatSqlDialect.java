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
package io.mycat.calcite;

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class MycatSqlDialect extends MysqlSqlDialect {
    /**
     * Creates a MysqlSqlDialect.
     *
     * @param context
     */
    public MycatSqlDialect(Context context) {
        super(context);
    }
    public static final SqlDialect DEFAULT = new MycatSqlDialect(DEFAULT_CONTEXT);

    @Override
    public SqlNode getCastSpec(RelDataType type) {
        if (type.getSqlTypeName() == SqlTypeName.BOOLEAN){
            return new SqlDataTypeSpec(
                    new SqlAlienSystemTypeNameSpec(
                            "SIGNED",
                            SqlTypeName.INTEGER,
                            SqlParserPos.ZERO),
                    SqlParserPos.ZERO);
        }
        return super.getCastSpec(type);
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, String charsetName, String val) {
        buf.append(literalQuoteString);
        buf.append(val);
        buf.append(literalEndQuoteString);
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlOperator operator = call.getOperator();
       if (operator instanceof MycatSqlDefinedFunction){
           operator.unparse(writer, call, leftPrec, rightPrec);
           return;
       }
//        if (operator instanceof SqlFunction){
//            operator.unparse(writer,call,leftPrec,rightPrec);
//            List<SqlNode> operandList = call.getOperandList();// should not with `` in fun name
//            if ("|".equalsIgnoreCase(operator.getName())){
//                SqlUtil.unparseBinarySyntax(operator, call, writer, leftPrec, rightPrec);
//                return;
//            }
//            if ("trim_leading".equalsIgnoreCase(operator.getName())){
//                writer.print("trim(leading ");
//                operandList.get(0).unparse(writer,0,0);
//                writer.print(" from ");
//                operandList.get(1).unparse(writer,0,0);
//                writer.print(")");
//                return;
//            }
//            if ("trim_trailing".equalsIgnoreCase(operator.getName())){
//                writer.print("trim(trailing ");
//                operandList.get(0).unparse(writer,0,0);
//                writer.print("from");
//                operandList.get(1).unparse(writer,0,0);
//                writer.print(")");
//                return;
//            }
//            if ("trim_both".equalsIgnoreCase(operator.getName())){
//                writer.print("trim(both ");
//                operandList.get(0).unparse(writer,0,0);
//                writer.print("from");
//                operandList.get(1).unparse(writer,0,0);
//                writer.print(")");
//                return;
//            }
//            writer.print(operator.getName());
//            SqlWriter.Frame frame = writer.startList("(", ")");
//            for (SqlNode sqlNode : operandList) {
//                writer.sep(",");
//                sqlNode.unparse(writer, 0, 0);
//            }
//            writer.endFunCall(frame);
//        }else {
//
//        }
        super.unparseCall(writer, call, leftPrec, rightPrec);
    }
}