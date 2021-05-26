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
package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;

public  class DateAddFunction extends SqlFunction {
    public static  final DateAddFunction INSTANCE =new DateAddFunction();
    public static final SqlReturnTypeInference SCOPE = opBinding -> {
      SqlCallBinding callBinding = (SqlCallBinding) opBinding;
      return callBinding.getValidator().getNamespace(
              callBinding.getCall()).getRowType();
    };
    public DateAddFunction() {
      super("DATE_ADD", SqlKind.OTHER_FUNCTION,
              ReturnTypes.VARCHAR_2000_NULLABLE
              , InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, SqlFunctionCategory.STRING);
    }
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      return super.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        super.unparse(writer, call, leftPrec, rightPrec);
    }

    public RexImpTable.RexCallImplementor getRexCallImplementor(){
      return  new RexImpTable.MycatAbstractRexCallImplementor(NullPolicy.ANY, false) {

          @Override
          public String getVariableName() {
            return "DATE_ADD";
          }

          @Override
          public Expression implementSafe(RexToLixTranslator translator, RexCall call, List<Expression> argValueList) {
            Expression one = argValueList.get(0);
            Expression second = argValueList.get(1);

            if (one.getType() ==String.class&&second.getType() == Duration.class&&
                    SqlTypeName.STRING_TYPES.contains(call.getType().getSqlTypeName())){
              Method dateAdd = Types.lookupMethod(MycatBuiltInMethodImpl.class, "dateAddString", String.class, Duration.class);
              return Expressions.call(dateAdd,one,second);
            }     if (one.getType() ==Duration.class&&second.getType() == String.class&&
                    SqlTypeName.STRING_TYPES.contains(call.getType().getSqlTypeName())){
              Method dateAdd = Types.lookupMethod(MycatBuiltInMethodImpl.class, "dateAddString", String.class, Duration.class);
              return Expressions.call(dateAdd,second,one);
            }
            return null;
          }
        };
      }
    }