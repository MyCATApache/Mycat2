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
package io.mycat.calcite.sqlfunction.datefunction;


import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.time.LocalDateTime;


public class SysDateFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(SysDateFunction.class,
            "sysdate");
    public static SysDateFunction INSTANCE = new SysDateFunction();


    public SysDateFunction() {
        super("SYSDATE", scalarFunction);
    }


    public static final LocalDateTime sysdate(@Parameter(name = "precision",optional = true) Integer precision) {
      if (precision == null){
          return NowFunction.now(null);
      }else {
          return NowFunction.now(precision);
      }
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return super.deriveType(validator, scope, call);
    }
}

