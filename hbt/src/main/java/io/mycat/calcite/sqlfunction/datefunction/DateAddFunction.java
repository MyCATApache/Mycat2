package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
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
   public RexImpTable.RexCallImplementor getRexCallImplementor(){
      return  new RexImpTable.MycatAbstractRexCallImplementor(NullPolicy.ANY, true) {

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