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

import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.*;

public class MycatDateFunction extends MycatSqlDefinedFunction {
//    public MycatDateFunction(String name,
//                             SqlReturnTypeInference returnTypeInference,
//                             SqlOperandTypeInference operandTypeInference,
//                             SqlOperandTypeChecker operandTypeChecker,
//                             ScalarFunction function) {
//        super(name, returnTypeInference, operandTypeInference, operandTypeChecker,function, SqlFunctionCategory.TIMEDATE);
//    }
//
//    public MycatDateFunction(String name, ScalarFunction function) {
//        this(name,new GetOperandTypes(function).invoke().getSqlReturnTypeInference(), InferTypes.VARCHAR_1024, OperandTypes .VARIADIC,function);
//    }
public MycatDateFunction(String name, ScalarFunction function) {
    super(name, function, SqlFunctionCategory.TIMEDATE);
}

}
