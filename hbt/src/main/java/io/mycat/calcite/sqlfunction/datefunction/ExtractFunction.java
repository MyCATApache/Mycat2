package io.mycat.calcite.sqlfunction.datefunction;

import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;

import java.lang.reflect.Method;
import java.util.List;

public  class ExtractFunction extends SqlFunction {
    public static  final ExtractFunction INSTANCE =new ExtractFunction();
    public static final SqlReturnTypeInference SCOPE = opBinding -> {
      SqlCallBinding callBinding = (SqlCallBinding) opBinding;
      return callBinding.getValidator().getNamespace(
              callBinding.getCall()).getRowType();
    };
    public ExtractFunction() {
      super("EXTRACT", SqlKind.OTHER_FUNCTION,
              ReturnTypes.INTEGER
              , InferTypes.ANY_NULLABLE, OperandTypes.VARIADIC, SqlFunctionCategory.NUMERIC);
    }
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      return super.checkOperandTypes(callBinding, throwOnFailure);
    }
    public RexImpTable.RexCallImplementor getRexCallImplementor(){
      return  new RexImpTable.MycatAbstractRexCallImplementor(NullPolicy.STRICT, false) {

        @Override
       public String getVariableName() {
          return "EXTRACT";
        }

        @Override
        public Expression implementSafe(RexToLixTranslator translator, RexCall call, List<Expression> argValueList) {
          final TimeUnitRange timeUnitRange =
                  (TimeUnitRange) translator.getLiteralValue(argValueList.get(0));
          final TimeUnit unit = timeUnitRange.startUnit;
          Expression operand = argValueList.get(1);
          final SqlTypeName sqlTypeName =
                  call.operands.get(1).getType().getSqlTypeName();
          Method extract = Types.lookupMethod(MycatBuiltInMethodImpl.class, "extract", TimeUnitRange.class, String.class);
          return Expressions.call(extract,argValueList.get(0),argValueList.get(1));

        }
      };

    };

    @Override
    public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
      final SqlWriter.Frame frame = writer.startFunCall(getName());
      //@todo
      call.operand(0).unparse(writer, 0, 0);
      writer.sep("FROM");
      call.operand(1).unparse(writer, 0, 0);
      writer.endFunCall(frame);
    }
  }