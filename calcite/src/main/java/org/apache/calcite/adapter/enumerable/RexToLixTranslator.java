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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.CatchBlock;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.mycat.MycatBuiltInMethod;
import org.apache.calcite.mycat.MycatSessionValueFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.GeoFunctions;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHARACTER_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PREV;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;

/**
 * Translates {@link org.apache.calcite.rex.RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 */
public class RexToLixTranslator implements RexVisitor<RexToLixTranslator.Result> {
  public static final Map<Method, SqlOperator> JAVA_TO_SQL_METHOD_MAP =
      Util.mapOf(
          findMethod(String.class, "toUpperCase"), UPPER,
          findMethod(
              SqlFunctions.class, "substring", String.class, Integer.TYPE,
              Integer.TYPE), SUBSTRING,
          findMethod(SqlFunctions.class, "charLength", String.class),
          CHARACTER_LENGTH,
          findMethod(SqlFunctions.class, "charLength", String.class),
          CHAR_LENGTH,
          findMethod(SqlFunctions.class, "translate3", String.class, String.class,
              String.class), TRANSLATE3);

  final JavaTypeFactory typeFactory;
  final RexBuilder builder;
  private final RexProgram program;
  final SqlConformance conformance;
  private final Expression root;
  final RexToLixTranslator.InputGetter inputGetter;
  private final BlockBuilder list;
  private final Function1<String, InputGetter> correlates;

  /**
   * Map from RexLiteral's variable name to its literal, which is often a
   * ({@link org.apache.calcite.linq4j.tree.ConstantExpression}))
   * It is used in the some {@code RexCall}'s implementors, such as
   * {@code ExtractImplementor}.
   *
   * @see #getLiteral
   * @see #getLiteralValue
   */
  private final Map<Expression, Expression> literalMap = new HashMap<>();

  /** For {@code RexCall}, keep the list of its operand's {@code Result}.
   * It is useful when creating a {@code CallImplementor}. */
  private final Map<RexCall, List<Result>> callOperandResultMap =
      new HashMap<>();

  /** Map from RexNode under specific storage type to its Result, to avoid
   * generating duplicate code. For {@code RexInputRef}, {@code RexDynamicParam}
   * and {@code RexFieldAccess}. */
  private final Map<Pair<RexNode, Type>, Result> rexWithStorageTypeResultMap =
      new HashMap<>();

  /** Map from RexNode to its Result, to avoid generating duplicate code.
   * For {@code RexLiteral} and {@code RexCall}. */
  private final Map<RexNode, Result> rexResultMap = new HashMap<>();

  private Type currentStorageType;

  private static Method findMethod(
      Class<?> clazz, String name, Class... parameterTypes) {
    try {
      return clazz.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private RexToLixTranslator(RexProgram program,
      JavaTypeFactory typeFactory,
      Expression root,
      InputGetter inputGetter,
      BlockBuilder list,
      RexBuilder builder,
      SqlConformance conformance,
      Function1<String, InputGetter> correlates) {
    this.program = program; // may be null
    this.typeFactory = Objects.requireNonNull(typeFactory);
    this.conformance = Objects.requireNonNull(conformance);
    this.root = Objects.requireNonNull(root);
    this.inputGetter = inputGetter;
    this.list = Objects.requireNonNull(list);
    this.builder = Objects.requireNonNull(builder);
    this.correlates = correlates; // may be null
  }

  /**
   * Translates a {@link RexProgram} to a sequence of expressions and
   * declarations.
   *
   * @param program Program to be translated
   * @param typeFactory Type factory
   * @param conformance SQL conformance
   * @param list List of statements, populated with declarations
   * @param outputPhysType Output type, or null
   * @param root Root expression
   * @param inputGetter Generates expressions for inputs
   * @param correlates Provider of references to the values of correlated
   *                   variables
   * @return Sequence of expressions, optional condition
   */
  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, PhysType outputPhysType, Expression root,
      InputGetter inputGetter, Function1<String, InputGetter> correlates) {
    List<Type> storageTypes = null;
    if (outputPhysType != null) {
      final RelDataType rowType = outputPhysType.getRowType();
      storageTypes = new ArrayList<>(rowType.getFieldCount());
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        storageTypes.add(outputPhysType.getJavaFieldType(i));
      }
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        list, new RexBuilder(typeFactory), conformance, null)
        .setCorrelates(correlates)
        .translateList(program.getProjectList(), storageTypes);
  }

  public static Expression translateTableFunction(JavaTypeFactory typeFactory,
      SqlConformance conformance, BlockBuilder blockBuilder,
      Expression root, RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    return new RexToLixTranslator(null, typeFactory, root, null,
        blockBuilder, new RexBuilder(typeFactory), conformance, null)
        .translateTableFunction(rexCall, inputEnumerable, inputPhysType, outputPhysType);
  }

  /** Creates a translator for translating aggregate functions. */
  public static RexToLixTranslator forAggregation(JavaTypeFactory typeFactory,
      BlockBuilder list, InputGetter inputGetter, SqlConformance conformance) {
    final ParameterExpression root = DataContext.ROOT;
    return new RexToLixTranslator(null, typeFactory, root, inputGetter, list,
        new RexBuilder(typeFactory), conformance, null);
  }

  Expression translate(RexNode expr) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
    return translate(expr, nullAs, null);
  }

  Expression translate(RexNode expr, Type storageType) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs, storageType);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
      Type storageType) {
    currentStorageType = storageType;
    final Result result = expr.accept(this);
    final Expression translated =
        EnumUtils.toInternal(result.valueVariable, storageType);
    assert translated != null;
    // When we asked for not null input that would be stored as box, avoid unboxing
    if (RexImpTable.NullAs.NOT_POSSIBLE == nullAs
        && translated.type.equals(storageType)) {
      return translated;
    }
    return nullAs.handle(translated);
  }

  Expression translateCast(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    Expression convert = null;
    switch (targetType.getSqlTypeName()) {
    case ANY:
      convert = operand;
      break;
      case TINYINT:
        switch (sourceType.getSqlTypeName()){
          case BOOLEAN: {
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_TINYINT.method, operand);
            break;
          }
          case TINYINT:
            convert = operand;
            break;
          case SMALLINT:
            convert = Expressions.call(MycatBuiltInMethod.SMALLINT_TO_TINYINT.method, operand);
            break;
          case INTEGER:
            convert = Expressions.call(MycatBuiltInMethod.INTEGER_TO_TINYINT.method, operand);
            break;
          case BIGINT:
            convert = Expressions.call(MycatBuiltInMethod.BIGINT_TO_TINYINT.method, operand);
            break;
          case DECIMAL:
            convert = Expressions.call(MycatBuiltInMethod.DECIMAL_TO_TINYINT.method, operand);
            break;
          case FLOAT:
            convert = Expressions.call(MycatBuiltInMethod.FLOAT_TO_TINYINT.method, operand);
            break;
          case REAL:
            convert = Expressions.call(MycatBuiltInMethod.REAL_TO_TINYINT.method, operand);
            break;
          case DOUBLE:
            convert = Expressions.call(MycatBuiltInMethod.DOUBLE_TO_TINYINT.method, operand);
            break;
          case DATE:
            convert = Expressions.call(MycatBuiltInMethod.DATE_TO_TINYINT.method, operand);
            break;
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE:
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_TINYINT.method, operand);
            break;
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            convert = Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_TINYINT.method, operand);
            break;
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
            convert = Expressions.call(MycatBuiltInMethod.PERIOD_TO_TINYINT.method, operand);
            break;
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
            convert = Expressions.call(MycatBuiltInMethod.DURATION_TO_TINYINT.method, operand);
            break;
          case CHAR:
          case VARCHAR:
            convert = Expressions.call(MycatBuiltInMethod.STRING_TO_TINYINT.method, operand);
            break;
          case BINARY:
          case VARBINARY:
            convert = Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_TINYINT.method, operand);
            break;
          case NULL:
            convert = Expressions.constant(null);
            break;
          case ANY:
            convert = operand;
            break;
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
      case SMALLINT:{
        switch (sourceType.getSqlTypeName()){
          case BOOLEAN:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_SMALLINT.method, operand);
            break;
          case TINYINT:
            convert = Expressions.call(MycatBuiltInMethod.TINYINT_TO_SMALLINT.method, operand);
            break;
          case SMALLINT:
            convert = operand;
            break;
          case INTEGER:
            convert = Expressions.call(MycatBuiltInMethod.INTEGER_TO_SMALLINT.method, operand);
            break;
          case BIGINT:
            convert = Expressions.call(MycatBuiltInMethod.BIGINT_TO_SMALLINT.method, operand);
            break;
          case DECIMAL:
            convert = Expressions.call(MycatBuiltInMethod.DECIMAL_TO_SMALLINT.method, operand);
            break;
          case FLOAT:
            convert = Expressions.call(MycatBuiltInMethod.FLOAT_TO_SMALLINT.method, operand);
            break;
          case REAL:
            convert = Expressions.call(MycatBuiltInMethod.REAL_TO_SMALLINT.method, operand);
            break;
          case DOUBLE:
            convert = Expressions.call(MycatBuiltInMethod.DOUBLE_TO_SMALLINT.method, operand);
            break;
          case DATE:
            convert = Expressions.call(MycatBuiltInMethod.DATE_TO_SMALLINT.method, operand);
            break;
          case TIME_WITH_LOCAL_TIME_ZONE:
          case TIME:
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_SMALLINT.method, operand);
            break;
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            convert = Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_SMALLINT.method, operand);
            break;
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
            convert = Expressions.call(MycatBuiltInMethod.PERIOD_TO_SMALLINT.method, operand);
            break;
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
            convert = Expressions.call(MycatBuiltInMethod.DURATION_TO_SMALLINT.method, operand);
            break;
          case CHAR:
          case VARCHAR:
            convert = Expressions.call(MycatBuiltInMethod.STRING_TO_SMALLINT.method, operand);
            break;
          case BINARY:
          case VARBINARY:
            convert = Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_SMALLINT.method, operand);
            break;
          case NULL:
            convert = Expressions.constant(null);
            break;
          case ANY:
            convert = operand;
            break;
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());

        }
      }
      case INTEGER:
      case BIGINT:
        switch (sourceType.getSqlTypeName()){
          case BOOLEAN:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_BIGINT.method, operand);
            break;
          case TINYINT:
            convert = Expressions.call(MycatBuiltInMethod.TINYINT_TO_BIGINT.method, operand);
            break;
          case SMALLINT:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_BIGINT.method, operand);
            break;
          case INTEGER:
            convert = Expressions.call(MycatBuiltInMethod.INTEGER_TO_BIGINT.method, operand);
            break;
          case BIGINT:
            convert = Expressions.call(MycatBuiltInMethod.BIGINT_TO_BIGINT.method, operand);
            break;
          case DECIMAL:
            convert = Expressions.call(MycatBuiltInMethod.DECIMAL_TO_BIGINT.method, operand);
            break;
          case FLOAT:
            convert = Expressions.call(MycatBuiltInMethod.FLOAT_TO_BIGINT.method, operand);
            break;
          case REAL:
            convert = Expressions.call(MycatBuiltInMethod.REAL_TO_BIGINT.method, operand);
            break;
          case DOUBLE:
            convert = Expressions.call(MycatBuiltInMethod.DOUBLE_TO_BIGINT.method, operand);
            break;
          case DATE:{
            convert = Expressions.call(MycatBuiltInMethod.DATE_TO_BIGINT.method, operand);
            break;
          }
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_BIGINT.method, operand);
            break;
          }
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_BIGINT.method, operand);
            break;
          }
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          {
            convert = Expressions.call(MycatBuiltInMethod.PERIOD_TO_BIGINT.method, operand);
            break;
          }
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
          {
            convert = Expressions.call(MycatBuiltInMethod.DURATION_TO_BIGINT.method, operand);
            break;
          }
          case CHAR:
          case VARCHAR:
          {
            convert = Expressions.call(MycatBuiltInMethod.STRING_TO_BIGINT.method, operand);
            break;
          }
          case BINARY:
          case VARBINARY:
          {
            convert = Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_BIGINT.method, operand);
            break;
          }
          case NULL:
          {
            convert = Expressions.constant(null);
            break;
          }
          case ANY:
          {
            convert = operand;
            break;
          }
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
        break;
      case DECIMAL:
        switch (sourceType.getSqlTypeName()){
          case BOOLEAN:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_DECIMAL.method, operand);
            break;
          case TINYINT:
            convert = Expressions.call(MycatBuiltInMethod.TINYINT_TO_DECIMAL.method, operand);
            break;
          case SMALLINT:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_DECIMAL.method, operand);
            break;
          case INTEGER:
            convert = Expressions.call(MycatBuiltInMethod.INTEGER_TO_DECIMAL.method, operand);
            break;
          case BIGINT:
            convert = Expressions.call(MycatBuiltInMethod.BIGINT_TO_DECIMAL.method, operand);
            break;
          case DECIMAL:
            convert = Expressions.call(MycatBuiltInMethod.DECIMAL_TO_DECIMAL.method, operand);
            break;
          case FLOAT:
            convert = Expressions.call(MycatBuiltInMethod.FLOAT_TO_DECIMAL.method, operand);
            break;
          case REAL:
            convert = Expressions.call(MycatBuiltInMethod.REAL_TO_DECIMAL.method, operand);
            break;
          case DOUBLE:
            convert = Expressions.call(MycatBuiltInMethod.DOUBLE_TO_DECIMAL.method, operand);
            break;
          case DATE:{
            convert = Expressions.call(MycatBuiltInMethod.DATE_TO_BIGINT.method, operand);
            break;
          }
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_DECIMAL.method, operand);
            break;
          }
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_DECIMAL.method, operand);
            break;
          }
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          {
            convert = Expressions.call(MycatBuiltInMethod.PERIOD_TO_DECIMAL.method, operand);
            break;
          }
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
          {
            convert = Expressions.call(MycatBuiltInMethod.DURATION_TO_DECIMAL.method, operand);
            break;
          }
          case CHAR:
          case VARCHAR:
          {
            convert = Expressions.call(MycatBuiltInMethod.STRING_TO_DECIMAL.method, operand);
            break;
          }
          case BINARY:
          case VARBINARY:
          {
            convert = Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_DECIMAL.method, operand);
            break;
          }
          case NULL:
          {
            convert = Expressions.constant(null);
            break;
          }
          case ANY:
          {
            convert = operand;
            break;
          }
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
        break;
      case FLOAT:
        switch (sourceType.getSqlTypeName()){
          case BOOLEAN:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_FLOAT.method, operand);
            break;
          case TINYINT:
            convert = Expressions.call(MycatBuiltInMethod.TINYINT_TO_FLOAT.method, operand);
            break;
          case SMALLINT:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_FLOAT.method, operand);
            break;
          case INTEGER:
            convert = Expressions.call(MycatBuiltInMethod.INTEGER_TO_FLOAT.method, operand);
            break;
          case BIGINT:
            convert = Expressions.call(MycatBuiltInMethod.BIGINT_TO_FLOAT.method, operand);
            break;
          case DECIMAL:
            convert = Expressions.call(MycatBuiltInMethod.DECIMAL_TO_FLOAT.method, operand);
            break;
          case FLOAT:
            convert = Expressions.call(MycatBuiltInMethod.FLOAT_TO_FLOAT.method, operand);
            break;
          case REAL:
            convert = Expressions.call(MycatBuiltInMethod.REAL_TO_FLOAT.method, operand);
            break;
          case DOUBLE:
            convert = Expressions.call(MycatBuiltInMethod.DOUBLE_TO_FLOAT.method, operand);
            break;
          case DATE:{
            convert = Expressions.call(MycatBuiltInMethod.DATE_TO_FLOAT.method, operand);
            break;
          }
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_FLOAT.method, operand);
            break;
          }
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_FLOAT.method, operand);
            break;
          }
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          {
            convert = Expressions.call(MycatBuiltInMethod.PERIOD_TO_FLOAT.method, operand);
            break;
          }
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
          {
            convert = Expressions.call(MycatBuiltInMethod.DURATION_TO_FLOAT.method, operand);
            break;
          }
          case CHAR:
          case VARCHAR:
          {
            convert = Expressions.call(MycatBuiltInMethod.STRING_TO_FLOAT.method, operand);
            break;
          }
          case BINARY:
          case VARBINARY:
          {
            convert = Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_FLOAT.method, operand);
            break;
          }
          case NULL:
          {
            convert = Expressions.constant(null);
            break;
          }
          case ANY:
          {
            convert = operand;
            break;
          }
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
        break;
      case REAL:
        switch (sourceType.getSqlTypeName()){
          case BOOLEAN:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_REAL.method, operand);
            break;
          case TINYINT:
            convert = Expressions.call(MycatBuiltInMethod.TINYINT_TO_REAL.method, operand);
            break;
          case SMALLINT:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_REAL.method, operand);
            break;
          case INTEGER:
            convert = Expressions.call(MycatBuiltInMethod.INTEGER_TO_REAL.method, operand);
            break;
          case BIGINT:
            convert = Expressions.call(MycatBuiltInMethod.BIGINT_TO_REAL.method, operand);
            break;
          case DECIMAL:
            convert = Expressions.call(MycatBuiltInMethod.DECIMAL_TO_REAL.method, operand);
            break;
          case FLOAT:
            convert = Expressions.call(MycatBuiltInMethod.FLOAT_TO_REAL.method, operand);
            break;
          case REAL:
            convert = Expressions.call(MycatBuiltInMethod.REAL_TO_REAL.method, operand);
            break;
          case DOUBLE:
            convert = Expressions.call(MycatBuiltInMethod.DOUBLE_TO_REAL.method, operand);
            break;
          case DATE:{
            convert = Expressions.call(MycatBuiltInMethod.DATE_TO_REAL.method, operand);
            break;
          }
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_REAL.method, operand);
            break;
          }
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_REAL.method, operand);
            break;
          }
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          {
            convert = Expressions.call(MycatBuiltInMethod.PERIOD_TO_REAL.method, operand);
            break;
          }
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
          {
            convert = Expressions.call(MycatBuiltInMethod.DURATION_TO_REAL.method, operand);
            break;
          }
          case CHAR:
          case VARCHAR:
          {
            convert = Expressions.call(MycatBuiltInMethod.STRING_TO_REAL.method, operand);
            break;
          }
          case BINARY:
          case VARBINARY:
          {
            convert = Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_REAL.method, operand);
            break;
          }
          case NULL:
          {
            convert = Expressions.constant(null);
            break;
          }
          case ANY:
          {
            convert = operand;
            break;
          }
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
        break;
      case DOUBLE: {
        switch (sourceType.getSqlTypeName()){
          case BOOLEAN:
            convert = Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_DOUBLE.method, operand);
            break;
          case TINYINT:
            convert = Expressions.call(MycatBuiltInMethod.TINYINT_TO_DOUBLE.method, operand);
            break;
          case SMALLINT:
            convert = Expressions.call(MycatBuiltInMethod.SMALLINT_TO_DOUBLE.method, operand);
            break;
          case INTEGER:
            convert = Expressions.call(MycatBuiltInMethod.INTEGER_TO_DOUBLE.method, operand);
            break;
          case BIGINT:
            convert = Expressions.call(MycatBuiltInMethod.BIGINT_TO_DOUBLE.method, operand);
            break;
          case DECIMAL:
            convert = Expressions.call(MycatBuiltInMethod.DECIMAL_TO_DOUBLE.method, operand);
            break;
          case FLOAT:
            convert = Expressions.call(MycatBuiltInMethod.FLOAT_TO_DOUBLE.method, operand);
            break;
          case REAL:
            convert = Expressions.call(MycatBuiltInMethod.REAL_TO_DOUBLE.method, operand);
            break;
          case DOUBLE:
            convert = Expressions.call(MycatBuiltInMethod.DOUBLE_TO_DOUBLE.method, operand);
            break;
          case DATE:{
            convert = Expressions.call(MycatBuiltInMethod.DATE_TO_DOUBLE.method, operand);
            break;
          }
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_DOUBLE.method, operand);
            break;
          }
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
            convert = Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_DOUBLE.method, operand);
            break;
          }
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          {
            convert = Expressions.call(MycatBuiltInMethod.PERIOD_TO_DOUBLE.method, operand);
            break;
          }
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
          {
            convert = Expressions.call(MycatBuiltInMethod.DURATION_TO_DOUBLE.method, operand);
            break;
          }
          case CHAR:
          case VARCHAR:
          {
            convert = Expressions.call(MycatBuiltInMethod.STRING_TO_DOUBLE.method, operand);
            break;
          }
          case BINARY:
          case VARBINARY:
          {
            convert = Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_DOUBLE.method, operand);
            break;
          }
          case NULL:
          {
            convert = Expressions.constant(null);
            break;
          }
          case ANY:
          {
            convert = operand;
            break;
          }
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
        break;
      }
      case DATE:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(MycatBuiltInMethod .STRING_TO_DATE.method, operand);
        break;
        case BOOLEAN:
          convert =
                  Expressions.call(MycatBuiltInMethod .BOOLEAN_TO_DATE.method, operand);
          break;
        case TINYINT:
          convert =
                  Expressions.call(MycatBuiltInMethod .TINYINT_TO_DATE.method, operand);
          break;
        case SMALLINT:
          convert =
                  Expressions.call(MycatBuiltInMethod .SMALLINT_TO_DATE.method, operand);
          break;
        case INTEGER:
          convert =
                  Expressions.call(MycatBuiltInMethod .INTEGER_TO_DATE.method, operand);
          break;
        case BIGINT:
          convert =
                  Expressions.call(MycatBuiltInMethod .BIGINT_TO_DATE.method, operand);
          break;
        case DECIMAL:
          convert =
                  Expressions.call(MycatBuiltInMethod .DECIMAL_TO_DATE.method, operand);
          break;
        case FLOAT:
          convert =
                  Expressions.call(MycatBuiltInMethod .FLOAT_TO_DATE.method, operand);
          break;
        case REAL:
          convert =
                  Expressions.call(MycatBuiltInMethod .REAL_TO_DATE.method, operand);
          break;
        case DOUBLE:
          convert =
                  Expressions.call(MycatBuiltInMethod .DOUBLE_TO_DATE.method, operand);
          break;
        case DATE:
          convert = operand;
          break;
        case TIME_WITH_LOCAL_TIME_ZONE:
        case TIME:
          convert =
                  Expressions.call(MycatBuiltInMethod .TIME_TO_DATE.method, operand);
          break;
        case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert = Expressions.call(MycatBuiltInMethod .TIMESTAMP_TO_DATE.method, operand);
        break;
        case INTERVAL_WEEK:
        case INTERVAL_QUARTER:
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
          convert = Expressions.call(MycatBuiltInMethod .PERIOD_TO_DATE.method, operand);
          break;
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_DAY_MICROSECOND:
          convert = Expressions.call(MycatBuiltInMethod .DURATION_TO_DATE.method, operand);
          break;
        case BINARY:
        case VARBINARY:
          convert = Expressions.call(MycatBuiltInMethod .BYTESTRING_TO_DATE.method, operand);
          break;
        case NULL:
          convert = Expressions.constant(null);
          break;
        case ANY:
          convert = operand;
          break;
        case SYMBOL:
        case MULTISET:
        case ARRAY:
        case MAP:
        case DISTINCT:
        case STRUCTURED:
        case ROW:
        case OTHER:
        case CURSOR:
        case COLUMN_LIST:
        case DYNAMIC_STAR:
        case GEOMETRY:
        case SARG:
        default:
          throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
      }
      break;
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIME:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        convert =
            Expressions.call(MycatBuiltInMethod.STRING_TO_TIME.method, operand);
        break;
        case BOOLEAN:
          convert =
                  Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_TIME.method, operand);
          break;
        case TINYINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.TINYINT_TO_TIME.method, operand);
          break;
        case SMALLINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.SMALLINT_TO_TIME.method, operand);
          break;
        case INTEGER:
          convert =
                  Expressions.call(MycatBuiltInMethod.INTEGER_TO_TIME.method, operand);
          break;
        case BIGINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.BIGINT_TO_TIME.method, operand);
          break;
        case DECIMAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.DECIMAL_TO_TIME.method, operand);
          break;
        case FLOAT:
          convert =
                  Expressions.call(MycatBuiltInMethod.FLOAT_TO_TIME.method, operand);
          break;
        case REAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.REAL_TO_TIME.method, operand);
          break;
        case DOUBLE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DOUBLE_TO_TIME.method, operand);
          break;
        case DATE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DATE_TO_TIME.method, operand);
          break;
        case TIME:
          convert = operand;
          break;
        case TIME_WITH_LOCAL_TIME_ZONE:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIME_TO_TIME.method, operand);
        break;
      case TIMESTAMP:
        convert =
                Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_TIME.method, operand);
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        convert =
                Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_TIME.method, operand);
        break;
        case INTERVAL_WEEK:
        case INTERVAL_QUARTER:
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
          convert =
                  Expressions.call(MycatBuiltInMethod.PERIOD_TO_TIME.method, operand);
          break;
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_DAY_MICROSECOND:
          convert = operand;
          break;
        case BINARY:
        case VARBINARY:
          convert =
                  Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_TIME.method, operand);
          break;
        case NULL:
          convert = Expressions.constant(null);
          break;
        case ANY:
          convert = operand;
          break;
        case SYMBOL:
        case MULTISET:
        case ARRAY:
        case MAP:
        case DISTINCT:
        case STRUCTURED:
        case ROW:
        case OTHER:
        case CURSOR:
        case COLUMN_LIST:
        case DYNAMIC_STAR:
        case GEOMETRY:
        case SARG:
        default:
          throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
      }
      break;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP:
      switch (sourceType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          convert =
                  Expressions.call(MycatBuiltInMethod.STRING_TO_TIMESTAMP.method, operand);
          break;
        case BOOLEAN:
          convert =
                  Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_TIMESTAMP.method, operand);
          break;
        case TINYINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.TINYINT_TO_TIMESTAMP.method, operand);
          break;
        case SMALLINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.SMALLINT_TO_TIMESTAMP.method, operand);
          break;
        case INTEGER:
          convert =
                  Expressions.call(MycatBuiltInMethod.INTEGER_TO_TIMESTAMP.method, operand);
          break;
        case BIGINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.BIGINT_TO_TIMESTAMP.method, operand);
          break;
        case DECIMAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.DECIMAL_TO_TIMESTAMP.method, operand);
          break;
        case FLOAT:
          convert =
                  Expressions.call(MycatBuiltInMethod.FLOAT_TO_TIMESTAMP.method, operand);
          break;
        case REAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.REAL_TO_TIMESTAMP.method, operand);
          break;
        case DOUBLE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DOUBLE_TO_TIMESTAMP.method, operand);
          break;
        case DATE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DATE_TO_TIMESTAMP.method, operand);
          break;
        case TIME:
          convert = operand;
          break;
        case TIME_WITH_LOCAL_TIME_ZONE:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIME_TO_TIMESTAMP.method, operand);
          break;
        case TIMESTAMP:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_TIMESTAMP.method, operand);
          break;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_TIMESTAMP.method, operand);
          break;
        case INTERVAL_WEEK:
        case INTERVAL_QUARTER:
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
          convert =
                  Expressions.call(MycatBuiltInMethod.PERIOD_TO_TIMESTAMP.method, operand);
          break;
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_DAY_MICROSECOND:
          convert = operand;
          break;
        case BINARY:
        case VARBINARY:
          convert =
                  Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_TIMESTAMP.method, operand);
          break;
        case NULL:
          convert = Expressions.constant(null);
          break;
        case ANY:
          convert = operand;
          break;
        case SYMBOL:
        case MULTISET:
        case ARRAY:
        case MAP:
        case DISTINCT:
        case STRUCTURED:
        case ROW:
        case OTHER:
        case CURSOR:
        case COLUMN_LIST:
        case DYNAMIC_STAR:
        case GEOMETRY:
        case SARG:
        default:
          throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
      }
      break;
    case BOOLEAN:
      switch (sourceType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          convert =
                  Expressions.call(MycatBuiltInMethod.STRING_TO_BOOLEAN.method, operand);
          break;
        case BOOLEAN:
          convert =
                  Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_BOOLEAN.method, operand);
          break;
        case TINYINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.TINYINT_TO_BOOLEAN.method, operand);
          break;
        case SMALLINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.SMALLINT_TO_BOOLEAN.method, operand);
          break;
        case INTEGER:
          convert =
                  Expressions.call(MycatBuiltInMethod.INTEGER_TO_BOOLEAN.method, operand);
          break;
        case BIGINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.BIGINT_TO_BOOLEAN.method, operand);
          break;
        case DECIMAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.DECIMAL_TO_BOOLEAN.method, operand);
          break;
        case FLOAT:
          convert =
                  Expressions.call(MycatBuiltInMethod.FLOAT_TO_BOOLEAN.method, operand);
          break;
        case REAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.REAL_TO_BOOLEAN.method, operand);
          break;
        case DOUBLE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DOUBLE_TO_BOOLEAN.method, operand);
          break;
        case DATE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DATE_TO_BOOLEAN.method, operand);
          break;
        case TIME:
          convert = Expressions.call(MycatBuiltInMethod.TIME_TO_BOOLEAN.method, operand);
          break;
        case TIME_WITH_LOCAL_TIME_ZONE:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIME_TO_BOOLEAN.method, operand);
          break;
        case TIMESTAMP:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_BOOLEAN.method, operand);
          break;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_BOOLEAN.method, operand);
          break;
        case INTERVAL_WEEK:
        case INTERVAL_QUARTER:
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
          convert =
                  Expressions.call(MycatBuiltInMethod.PERIOD_TO_BOOLEAN.method, operand);
          break;
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_DAY_MICROSECOND:
          convert = operand;
          break;
        case BINARY:
        case VARBINARY:
          convert =
                  Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_BOOLEAN.method, operand);
          break;
        case NULL:
          convert = Expressions.constant(null);
          break;
        case ANY:
          convert = operand;
          break;
        case SYMBOL:
        case MULTISET:
        case ARRAY:
        case MAP:
        case DISTINCT:
        case STRUCTURED:
        case ROW:
        case OTHER:
        case CURSOR:
        case COLUMN_LIST:
        case DYNAMIC_STAR:
        case GEOMETRY:
        case SARG:
        default:
          throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
      }
      break;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      {
        switch (sourceType.getSqlTypeName()) {
          case CHAR:
          case VARCHAR:
            convert =
                    Expressions.call(MycatBuiltInMethod.STRING_TO_PERIOD.method, operand);
            break;
          case BOOLEAN:
            convert =
                    Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_PERIOD.method, operand);
            break;
          case TINYINT:
            convert =
                    Expressions.call(MycatBuiltInMethod.TINYINT_TO_PERIOD.method, operand);
            break;
          case SMALLINT:
            convert =
                    Expressions.call(MycatBuiltInMethod.SMALLINT_TO_PERIOD.method, operand);
            break;
          case INTEGER:
            convert =
                    Expressions.call(MycatBuiltInMethod.INTEGER_TO_PERIOD.method, operand);
            break;
          case BIGINT:
            convert =
                    Expressions.call(MycatBuiltInMethod.BIGINT_TO_PERIOD.method, operand);
            break;
          case DECIMAL:
            convert =
                    Expressions.call(MycatBuiltInMethod.DECIMAL_TO_PERIOD.method, operand);
            break;
          case FLOAT:
            convert =
                    Expressions.call(MycatBuiltInMethod.FLOAT_TO_PERIOD.method, operand);
            break;
          case REAL:
            convert =
                    Expressions.call(MycatBuiltInMethod.REAL_TO_PERIOD.method, operand);
            break;
          case DOUBLE:
            convert =
                    Expressions.call(MycatBuiltInMethod.DOUBLE_TO_PERIOD.method, operand);
            break;
          case DATE:
            convert =
                    Expressions.call(MycatBuiltInMethod.DATE_TO_PERIOD.method, operand);
            break;
          case TIME:
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_PERIOD.method, operand);
            break;
          case TIME_WITH_LOCAL_TIME_ZONE:
            convert =
                    Expressions.call(MycatBuiltInMethod.TIME_TO_PERIOD.method, operand);
            break;
          case TIMESTAMP:
            convert =
                    Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_PERIOD.method, operand);
            break;
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            convert =
                    Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_PERIOD.method, operand);
            break;
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
            convert =
                    Expressions.call(MycatBuiltInMethod.PERIOD_TO_PERIOD.method, operand);
            break;
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
            convert = operand;
            break;
          case BINARY:
          case VARBINARY:
            convert =
                    Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_PERIOD.method, operand);
            break;
          case NULL:
            convert = Expressions.constant(null);
            break;
          case ANY:
            convert = operand;
            break;
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
      }
      break;
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case INTERVAL_MICROSECOND:
      case INTERVAL_WEEK:
      case INTERVAL_QUARTER:
      case INTERVAL_SECOND_MICROSECOND:
      case INTERVAL_MINUTE_MICROSECOND:
      case INTERVAL_HOUR_MICROSECOND:
      case INTERVAL_DAY_MICROSECOND:
      {
        switch (sourceType.getSqlTypeName()) {
          case CHAR:
          case VARCHAR:
            convert =
                    Expressions.call(MycatBuiltInMethod.STRING_TO_DURATION.method, operand);
            break;
          case BOOLEAN:
            convert =
                    Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_DURATION.method, operand);
            break;
          case TINYINT:
            convert =
                    Expressions.call(MycatBuiltInMethod.TINYINT_TO_DURATION.method, operand);
            break;
          case SMALLINT:
            convert =
                    Expressions.call(MycatBuiltInMethod.SMALLINT_TO_DURATION.method, operand);
            break;
          case INTEGER:
            convert =
                    Expressions.call(MycatBuiltInMethod.INTEGER_TO_DURATION.method, operand);
            break;
          case BIGINT:
            convert =
                    Expressions.call(MycatBuiltInMethod.BIGINT_TO_DURATION.method, operand);
            break;
          case DECIMAL:
            convert =
                    Expressions.call(MycatBuiltInMethod.DECIMAL_TO_DURATION.method, operand);
            break;
          case FLOAT:
            convert =
                    Expressions.call(MycatBuiltInMethod.FLOAT_TO_DURATION.method, operand);
            break;
          case REAL:
            convert =
                    Expressions.call(MycatBuiltInMethod.REAL_TO_DURATION.method, operand);
            break;
          case DOUBLE:
            convert =
                    Expressions.call(MycatBuiltInMethod.DOUBLE_TO_DURATION.method, operand);
            break;
          case DATE:
            convert =
                    Expressions.call(MycatBuiltInMethod.DATE_TO_DURATION.method, operand);
            break;
          case TIME:
            convert = Expressions.call(MycatBuiltInMethod.TIME_TO_DURATION.method, operand);
            break;
          case TIME_WITH_LOCAL_TIME_ZONE:
            convert =
                    Expressions.call(MycatBuiltInMethod.TIME_TO_DURATION.method, operand);
            break;
          case TIMESTAMP:
            convert =
                    Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_DURATION.method, operand);
            break;
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            convert =
                    Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_DURATION.method, operand);
            break;
          case INTERVAL_WEEK:
          case INTERVAL_QUARTER:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
            convert =
                    Expressions.call(MycatBuiltInMethod.PERIOD_TO_DURATION.method, operand);
            break;
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
          case INTERVAL_MICROSECOND:
          case INTERVAL_SECOND_MICROSECOND:
          case INTERVAL_MINUTE_MICROSECOND:
          case INTERVAL_HOUR_MICROSECOND:
          case INTERVAL_DAY_MICROSECOND:
            convert = operand;
            break;
          case BINARY:
          case VARBINARY:
            convert =
                    Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_DURATION.method, operand);
            break;
          case NULL:
            convert = Expressions.constant(null);
            break;
          case ANY:
            convert = operand;
            break;
          case SYMBOL:
          case MULTISET:
          case ARRAY:
          case MAP:
          case DISTINCT:
          case STRUCTURED:
          case ROW:
          case OTHER:
          case CURSOR:
          case COLUMN_LIST:
          case DYNAMIC_STAR:
          case GEOMETRY:
          case SARG:
          default:
            throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
        }
      }
        break;
      case CHAR:
    case VARCHAR:
      final SqlIntervalQualifier interval =
          sourceType.getIntervalQualifier();
    {
      switch (sourceType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          convert = operand;
          break;
        case BOOLEAN:
          convert =
                  Expressions.call(MycatBuiltInMethod.BOOLEAN_TO_STRING.method, operand);
          break;
        case TINYINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.TINYINT_TO_STRING.method, operand);
          break;
        case SMALLINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.SMALLINT_TO_STRING.method, operand);
          break;
        case INTEGER:
          convert =
                  Expressions.call(MycatBuiltInMethod.INTEGER_TO_STRING.method, operand);
          break;
        case BIGINT:
          convert =
                  Expressions.call(MycatBuiltInMethod.BIGINT_TO_STRING.method, operand);
          break;
        case DECIMAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.DECIMAL_TO_STRING.method, operand);
          break;
        case FLOAT:
          convert =
                  Expressions.call(MycatBuiltInMethod.FLOAT_TO_STRING.method, operand);
          break;
        case REAL:
          convert =
                  Expressions.call(MycatBuiltInMethod.REAL_TO_STRING.method, operand);
          break;
        case DOUBLE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DOUBLE_TO_STRING.method, operand);
          break;
        case DATE:
          convert =
                  Expressions.call(MycatBuiltInMethod.DATE_TO_STRING.method, operand);
          break;
        case TIME:
          convert = Expressions.call(MycatBuiltInMethod.TIME_TO_STRING.method, operand);
          break;
        case TIME_WITH_LOCAL_TIME_ZONE:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIME_TO_STRING.method, operand);
          break;
        case TIMESTAMP:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_STRING.method, operand);
          break;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          convert =
                  Expressions.call(MycatBuiltInMethod.TIMESTAMP_TO_STRING.method, operand);
          break;
        case INTERVAL_WEEK:
        case INTERVAL_QUARTER:
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
          convert =
                  Expressions.call(MycatBuiltInMethod.PERIOD_TO_STRING.method, operand);
          break;
        case INTERVAL_DAY:
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_DAY_MICROSECOND:
          convert = operand;
          break;
        case BINARY:
        case VARBINARY:
          convert =
                  Expressions.call(MycatBuiltInMethod.BYTESTRING_TO_STRING.method, operand);
          break;
        case NULL:
          convert = Expressions.constant(null);
          break;
        case ANY:
          convert = operand;
          break;
        case SYMBOL:
        case MULTISET:
        case ARRAY:
        case MAP:
        case DISTINCT:
        case STRUCTURED:
        case ROW:
        case OTHER:
        case CURSOR:
        case COLUMN_LIST:
        case DYNAMIC_STAR:
        case GEOMETRY:
        case SARG:
        default:
          throw new UnsupportedOperationException("can not convert "+sourceType.getSqlTypeName()+" to "+targetType.getSqlTypeName());
      }
    }
    break;
      case BINARY:
      case VARBINARY:
        break;
      case NULL:
        convert = Expressions.constant(null);
        break;
      case SYMBOL:
      case MULTISET:
      case ARRAY:
      case MAP:
      case DISTINCT:
      case STRUCTURED:
      case ROW:
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case SARG:
      default:
        throw new IllegalStateException("Unexpected value: " + targetType.getSqlTypeName());
    }
    if (convert == null) {
      convert = EnumUtils.convert(operand, typeFactory.getJavaClass(targetType));
    }
    // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
    // longer than n.
    boolean pad = false;
    boolean truncate = true;
    switch (targetType.getSqlTypeName()) {
    case CHAR:
    case BINARY:
      pad = true;
      // fall through
    case VARCHAR:
    case VARBINARY:
      final int targetPrecision = targetType.getPrecision();
      if (targetPrecision >= 0) {
        switch (sourceType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
          // If this is a widening cast, no need to truncate.
          final int sourcePrecision = sourceType.getPrecision();
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              <= 0) {
            truncate = false;
          }
          // If this is a widening cast, no need to pad.
          if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
              >= 0) {
            pad = false;
          }
          // fall through
        default:
          if (truncate || pad) {
            convert =
                Expressions.call(
                    pad
                        ? BuiltInMethod.TRUNCATE_OR_PAD.method
                        : BuiltInMethod.TRUNCATE.method,
                    convert,
                    Expressions.constant(targetPrecision));
          }
        }
      }
      break;
    case TIMESTAMP:
      int targetScale = targetType.getScale();
      if (targetScale == RelDataType.SCALE_NOT_SPECIFIED) {
        targetScale = 0;
      }
      if (targetScale < sourceType.getScale()) {
        convert =
            Expressions.call(
                BuiltInMethod.ROUND_LONG.method,
                convert,
                Expressions.constant(
                    (long) Math.pow(10, 3 - targetScale)));
      }
      break;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      switch (sourceType.getSqlTypeName().getFamily()) {
      case NUMERIC:
        final BigDecimal multiplier = targetType.getSqlTypeName().getEndUnit().multiplier;
        final BigDecimal divider = BigDecimal.ONE;
        convert = RexImpTable.multiplyDivide(convert, multiplier, divider);
      }
    }
    return scaleIntervalToNumber(sourceType, targetType, convert);
  }

  private Expression timestamp(RelDataType sourceType, Expression operand, Expression convert) {
    switch (sourceType.getSqlTypeName()){
      case BOOLEAN:
        break;
      case TINYINT:
        break;
      case SMALLINT:
        break;
      case INTEGER:
          convert = Expressions.call(MycatBuiltInMethod.LONG_TO_TIMESTAMP.method, operand);
        break;
      case BIGINT:
        break;
      case DECIMAL:
        break;
      case FLOAT:
        break;
      case REAL:
        break;
      case DOUBLE:
        break;
      case DATE:
        break;
      case TIME:
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        break;
      case TIMESTAMP:
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        break;
      case INTERVAL_YEAR:
        break;
      case INTERVAL_YEAR_MONTH:
        break;
      case INTERVAL_MONTH:
        break;
      case INTERVAL_DAY:
        break;
      case INTERVAL_DAY_HOUR:
        break;
      case INTERVAL_DAY_MINUTE:
        break;
      case INTERVAL_DAY_SECOND:
        break;
      case INTERVAL_HOUR:
        break;
      case INTERVAL_HOUR_MINUTE:
        break;
      case INTERVAL_HOUR_SECOND:
        break;
      case INTERVAL_MINUTE:
        break;
      case INTERVAL_MINUTE_SECOND:
        break;
      case INTERVAL_SECOND:
        break;
      case INTERVAL_MICROSECOND:
        break;
      case INTERVAL_WEEK:
        break;
      case INTERVAL_QUARTER:
        break;
      case INTERVAL_SECOND_MICROSECOND:
        break;
      case INTERVAL_MINUTE_MICROSECOND:
        break;
      case INTERVAL_HOUR_MICROSECOND:
        break;
      case INTERVAL_DAY_MICROSECOND:
        break;
      case CHAR:
        break;
      case VARCHAR:
        break;
      case BINARY:
        break;
      case VARBINARY:
        break;
      case NULL:
        break;
      case ANY:
        break;
      case SYMBOL:
        break;
      case MULTISET:
        break;
      case ARRAY:
        break;
      case MAP:
        break;
      case DISTINCT:
        break;
      case STRUCTURED:
        break;
      case ROW:
        break;
      case OTHER:
        break;
      case CURSOR:
        break;
      case COLUMN_LIST:
        break;
      case DYNAMIC_STAR:
        break;
      case GEOMETRY:
        break;
      case SARG:
        break;
    }
    //        convert = Expressions.call(MycatBuiltInMethod .DATE_TO_TIMESTAMP.method, operand),
    return convert;
  }

  /**
   * Handle checked Exceptions declared in Method. In such case,
   * method call should be wrapped in a try...catch block.
   * "
   *      final Type method_call;
   *      try {
   *        method_call = callExpr
   *      } catch (Exception e) {
   *        throw new RuntimeException(e);
   *      }
   * "
   */
  Expression handleMethodCheckedExceptions(Expression callExpr) {
    // Try statement
    ParameterExpression methodCall = Expressions.parameter(
        callExpr.getType(), list.newName("method_call"));
    list.add(Expressions.declare(Modifier.FINAL, methodCall, null));
    Statement st = Expressions.statement(Expressions.assign(methodCall, callExpr));
    // Catch Block, wrap checked exception in unchecked exception
    ParameterExpression e = Expressions.parameter(0, Exception.class, "e");
    Expression uncheckedException = Expressions.new_(RuntimeException.class, e);
    CatchBlock cb = Expressions.catch_(e, Expressions.throw_(uncheckedException));
    list.add(Expressions.tryCatch(st, cb));
    return methodCall;
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  public RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = program.getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  /** Translates a literal.
   *
   * @throws ControlFlowException if literal is null but {@code nullAs} is
   * {@link org.apache.calcite.adapter.enumerable.RexImpTable.NullAs#NOT_POSSIBLE}.
   */
  public static Expression translateLiteral(
      RexLiteral literal,
      RelDataType type,
      JavaTypeFactory typeFactory,
      RexImpTable.NullAs nullAs) {
    if (literal.isNull()) {
      switch (nullAs) {
      case TRUE:
      case IS_NULL:
        return RexImpTable.TRUE_EXPR;
      case FALSE:
      case IS_NOT_NULL:
        return RexImpTable.FALSE_EXPR;
      case NOT_POSSIBLE:
        throw new ControlFlowException();
      case NULL:
      default:
        return RexImpTable.NULL_EXPR;
      }
    } else {
      switch (nullAs) {
      case IS_NOT_NULL:
        return RexImpTable.TRUE_EXPR;
      case IS_NULL:
        return RexImpTable.FALSE_EXPR;
      }
    }
    Type javaClass = typeFactory.getJavaClass(type);
     Object value2 = null;
    switch (literal.getType().getSqlTypeName()) {
      case DECIMAL:
      final BigDecimal bd = literal.getValueAs(BigDecimal.class);
      if (javaClass == float.class) {
        return Expressions.constant(bd, javaClass);
      } else if (javaClass == double.class) {
        return Expressions.constant(bd, javaClass);
      }
      assert javaClass == BigDecimal.class;
      return Expressions.new_(BigDecimal.class,
          Expressions.constant(bd.toString()));
      case DATE:
        value2 = literal.getValueAs(LocalDate.class);
        javaClass = LocalDate.class;
        break;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
      value2 = literal.getValueAs(Duration.class);
      javaClass = Duration.class;
      break;
      case INTERVAL_WEEK:
      case INTERVAL_QUARTER:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      value2 = literal.getValueAs(Period.class);
      javaClass = Period.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      value2 = literal.getValueAs(LocalDateTime.class);
      javaClass = LocalDateTime.class;
      break;
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      case INTERVAL_MICROSECOND:

      case INTERVAL_SECOND_MICROSECOND:
      case INTERVAL_MINUTE_MICROSECOND:
      case INTERVAL_HOUR_MICROSECOND:
      case INTERVAL_DAY_MICROSECOND:
      value2 = literal.getValueAs(Duration.class);
      javaClass = Duration.class;
      break;

      case CHAR:
    case VARCHAR:
      value2 = literal.getValueAs(String.class);
      break;
    case BINARY:
    case VARBINARY:
      return Expressions.new_(
          ByteString.class,
          Expressions.constant(
              literal.getValueAs(byte[].class),
              byte[].class));
    case GEOMETRY:
//      final Geometries.Geom geom = literal.getValueAs(Geometries.Geom.class);
//      final String wkt = GeoFunctions.ST_AsWKT(geom);
//      return Expressions.call(null, BuiltInMethod.ST_GEOM_FROM_TEXT.method,
//          Expressions.constant(wkt));
      throw new UnsupportedOperationException("ST_GEOM_FROM_TEXT");
      case NULL:
        break;
      case ANY:
        break;
      case SYMBOL:
      value2 = literal.getValueAs(Enum.class);
      javaClass = value2.getClass();
      break;
      case MULTISET:
        break;
      case ARRAY:
        break;
      case MAP:
        break;
      case DISTINCT:
        break;
      case STRUCTURED:
        break;
      case ROW:
        break;
      case OTHER:
        break;
      case CURSOR:
        break;
      case COLUMN_LIST:
        break;
      case DYNAMIC_STAR:
        break;
      case SARG:
        break;
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      default:
      final Primitive primitive = Primitive.ofBoxOr(javaClass);
      final Comparable value = literal.getValueAs(Comparable.class);
      if (primitive != null && value instanceof Number) {
        value2 = primitive.number((Number) value);
      } else {
        value2 = value;
      }
    }

    return Expressions.constant(value2, javaClass);
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs) {
    return translateList(operandList, nullAs,
        EnumUtils.internalTypes(operandList));
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>();
    for (Pair<RexNode, ? extends Type> e : Pair.zip(operandList, storageTypes)) {
      list.add(translate(e.left, nullAs, e.right));
    }
    return list;
  }

  /**
   * Translates the list of {@code RexNode}, using the default output types.
   * This might be suboptimal in terms of additional box-unbox when you use
   * the translation later.
   * If you know the java class that will be used to store the results, use
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator#translateList(java.util.List, java.util.List)}
   * version.
   *
   * @param operandList list of RexNodes to translate
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList) {
    return translateList(operandList, EnumUtils.internalTypes(operandList));
  }

  /**
   * Translates the list of {@code RexNode}, while optimizing for output
   * storage.
   * For instance, if the result of translation is going to be stored in
   * {@code Object[]}, and the input is {@code Object[]} as well,
   * then translator will avoid casting, boxing, etc.
   *
   * @param operandList list of RexNodes to translate
   * @param storageTypes hints of the java classes that will be used
   *                     to store translation results. Use null to use
   *                     default storage type
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>(operandList.size());

    for (int i = 0; i < operandList.size(); i++) {
      RexNode rex = operandList.get(i);
      Type desiredType = null;
      if (storageTypes != null) {
        desiredType = storageTypes.get(i);
      }
      final Expression translate = translate(rex, desiredType);
      list.add(translate);
      // desiredType is still a hint, thus we might get any kind of output
      // (boxed or not) when hint was provided.
      // It is favourable to get the type matching desired type
      if (desiredType == null && !isNullable(rex)) {
        assert !Primitive.isBox(translate.getType())
            : "Not-null boxed primitive should come back as primitive: "
            + rex + ", " + translate.getType();
      }
    }
    return list;
  }

  private Expression translateTableFunction(RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    assert rexCall.getOperator() instanceof SqlWindowTableFunction;
    TableFunctionCallImplementor implementor =
        RexImpTable.INSTANCE.get((SqlWindowTableFunction) rexCall.getOperator());
    if (implementor == null) {
      throw Util.needToImplement("implementor of " + rexCall.getOperator().getName());
    }
    return implementor.implement(
        this, inputEnumerable, rexCall, inputPhysType, outputPhysType);
  }

  public static Expression translateCondition(RexProgram program,
      JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter,
      Function1<String, InputGetter> correlates, SqlConformance conformance) {
    if (program.getCondition() == null) {
      return RexImpTable.TRUE_EXPR;
    }
    final ParameterExpression root = DataContext.ROOT;
    RexToLixTranslator translator =
        new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
            new RexBuilder(typeFactory), conformance, null);
    translator = translator.setCorrelates(correlates);
    return translator.translate(
        program.getCondition(),
        RexImpTable.NullAs.FALSE);
  }

  /** Returns whether an expression is nullable.
   * @param e Expression
   * @return Whether expression is nullable
   */
  public boolean isNullable(RexNode e) {
    return e.getType().isNullable();
  }

  public RexToLixTranslator setBlock(BlockBuilder block) {
    if (block == list) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        block, builder, conformance, correlates);
  }

  public RexToLixTranslator setCorrelates(
      Function1<String, InputGetter> correlates) {
    if (this.correlates == correlates) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        builder, conformance, correlates);
  }

  public Expression getRoot() {
    return root;
  }

  private static Expression scaleIntervalToNumber(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    switch (targetType.getSqlTypeName().getFamily()) {
    case NUMERIC:
      switch (sourceType.getSqlTypeName()) {
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        // Scale to the given field.
        final BigDecimal multiplier = BigDecimal.ONE;
        final BigDecimal divider =
            sourceType.getSqlTypeName().getEndUnit().multiplier;
        return RexImpTable.multiplyDivide(operand, multiplier, divider);
      }
    }
    return operand;
  }

  /**
   * Visit {@code RexInputRef}. If it has never been visited
   * under current storage type before, {@code RexToLixTranslator}
   * generally produces three lines of code.
   * For example, when visiting a column (named commission) in
   * table Employee, the generated code snippet is:
   * {@code
   *   final Employee current =(Employee) inputEnumerator.current();
       final Integer input_value = current.commission;
       final boolean input_isNull = input_value == null;
   * }
   */
  @Override public Result visitInputRef(RexInputRef inputRef) {
    final Pair<RexNode, Type> key = Pair.of(inputRef, currentStorageType);
    // If the RexInputRef has been visited under current storage type already,
    // it is not necessary to visit it again, just return the result.
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    // Generate one line of code to get the input, e.g.,
    // "final Employee current =(Employee) inputEnumerator.current();"
    final Expression valueExpression = inputGetter.field(
        list, inputRef.getIndex(), currentStorageType);

    // Generate one line of code for the value of RexInputRef, e.g.,
    // "final Integer input_value = current.commission;"
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(), list.newName("input_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));

    // Generate one line of code to check whether RexInputRef is null, e.g.,
    // "final boolean input_isNull = input_value == null;"
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("input_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    final Result result = new Result(isNullVariable, valueVariable);

    // Cache <RexInputRef, currentStorageType>'s result
    // Note: EnumerableMatch's PrevInputGetter changes index each time,
    // it is not right to reuse the result under such case.
    if (!(inputGetter instanceof EnumerableMatch.PrevInputGetter)) {
      rexWithStorageTypeResultMap.put(key, result);
    }
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitLocalRef(RexLocalRef localRef) {
    return deref(localRef).accept(this);
  }

  /**
   * Visit {@code RexLiteral}. If it has never been visited before,
   * {@code RexToLixTranslator} will generate two lines of code. For example,
   * when visiting a primitive int (10), the generated code snippet is:
   * {@code
   *   final int literal_value = 10;
   *   final boolean literal_isNull = false;
   * }
   */
  @Override public Result visitLiteral(RexLiteral literal) {
    // If the RexLiteral has been visited already, just return the result
    if (rexResultMap.containsKey(literal)) {
      return rexResultMap.get(literal);
    }
    // Generate one line of code for the value of RexLiteral, e.g.,
    // "final int literal_value = 10;"
    final Expression valueExpression = literal.isNull()
        // Note: even for null literal, we can't loss its type information
        ? getTypedNullLiteral(literal)
        : translateLiteral(literal, literal.getType(),
            typeFactory, RexImpTable.NullAs.NOT_POSSIBLE);
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(),
            list.newName("literal_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));

    // Generate one line of code to check whether RexLiteral is null, e.g.,
    // "final boolean literal_isNull = false;"
    final Expression isNullExpression =
        literal.isNull() ? RexImpTable.TRUE_EXPR : RexImpTable.FALSE_EXPR;
    final ParameterExpression isNullVariable = Expressions.parameter(
        Boolean.TYPE, list.newName("literal_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    // Maintain the map from valueVariable (ParameterExpression) to real Expression
    literalMap.put(valueVariable, valueExpression);
    final Result result = new Result(isNullVariable, valueVariable);
    // Cache RexLiteral's result
    rexResultMap.put(literal, result);
    return result;
  }

  /**
   * Returns an {@code Expression} for null literal without losing its type
   * information.
   */
  private ConstantExpression getTypedNullLiteral(RexLiteral literal) {
    assert literal.isNull();
    Type javaClass = typeFactory.getJavaClass(literal.getType());
    switch (literal.getType().getSqlTypeName()) {
    case DATE:
      javaClass = LocalDate.class;
      break;
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
      javaClass = LocalTime.class;
      break;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      javaClass = Period.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      javaClass = LocalDateTime.class;
      break;
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      javaClass = Duration.class;
      break;
    }
    return javaClass == null || javaClass == Void.class
        ? RexImpTable.NULL_EXPR
        : Expressions.constant(null, javaClass);
  }

  /**
   * Visit {@code RexCall}. For most {@code SqlOperator}s, we can get the implementor
   * from {@code RexImpTable}. Several operators (e.g., CaseWhen) with special semantics
   * need to be implemented separately.
   */
  @Override public Result visitCall(RexCall call) {
    if (rexResultMap.containsKey(call)) {
      return rexResultMap.get(call);
    }
    final SqlOperator operator = call.getOperator();
    if (operator == PREV) {
      return implementPrev(call);
    }
    if (operator == CASE) {
      return implementCaseWhen(call);
    }
    if (operator == SEARCH) {
      return RexUtil.expandSearch(builder, program, call).accept(this);
    }

     RexImpTable.RexCallImplementor implementor =
        RexImpTable.INSTANCE.get(operator);

    if (implementor == null) {
      if (MycatSessionValueFunction.INSTANCE.getName().equalsIgnoreCase(operator.getName())){
        implementor = RexImpTable.MycatSessionValueImplementor.INSTANCE;
      }else {
        throw new RuntimeException("cannot translate call " + call);
      }
    }
    final List<RexNode> operandList = call.getOperands();
    final List<Type> storageTypes = EnumUtils.internalTypes(operandList);
    final List<Result> operandResults = new ArrayList<>();
    for (int i = 0; i < operandList.size(); i++) {
      final Result operandResult =
          implementCallOperand(operandList.get(i), storageTypes.get(i), this);
      operandResults.add(operandResult);
    }
    callOperandResultMap.put(call, operandResults);
    final Result result = implementor.implement(this, call, operandResults);
    rexResultMap.put(call, result);
    return result;
  }

  private static Result implementCallOperand(final RexNode operand,
      final Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    Result operandResult = operand.accept(translator);
    if (storageType != null) {
      operandResult = translator.toInnerStorageType(operandResult, storageType);
    }
    translator.currentStorageType = originalStorageType;
    return operandResult;
  }

  private static Expression implementCallOperand2(final RexNode operand,
      final Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    final Expression result =  translator.translate(operand);
    translator.currentStorageType = originalStorageType;
    return result;
  }

  /**
   * For {@code PREV} operator, the offset of {@code inputGetter}
   * should be set first.
   */
  private Result implementPrev(RexCall call) {
    final RexNode node = call.getOperands().get(0);
    final RexNode offset = call.getOperands().get(1);
    final Expression offs = Expressions.multiply(translate(offset),
            Expressions.constant(-1));
    ((EnumerableMatch.PrevInputGetter) inputGetter).setOffset(offs);
    return node.accept(this);
  }

  /**
   * The CASE operator is SQL’s way of handling if/then logic.
   * Different with other {@code RexCall}s, it is not safe to
   * implement its operands first.
   * For example: {@code
   *   select case when s=0 then false
   *          else 100/s > 0 end
   *   from (values (1),(0)) ax(s);
   * }
   */
  private Result implementCaseWhen(RexCall call) {
    final Type returnType = typeFactory.getJavaClass(call.getType());
    final ParameterExpression valueVariable =
        Expressions.parameter(returnType,
            list.newName("case_when_value"));
    list.add(Expressions.declare(0, valueVariable, null));
    final List<RexNode> operandList = call.getOperands();
    implementRecursively(this, operandList, valueVariable, 0);
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("case_when_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    final Result result = new Result(isNullVariable, valueVariable);
    rexResultMap.put(call, result);
    return result;
  }

  /**
   * Case statements of the form:
   * {@code CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END}.
   * When {@code a = true}, returns {@code b};
   * when {@code c = true}, returns {@code d};
   * else returns {@code e}.
   *
   * <p>We generate code that looks like:
   *
   * <blockquote><pre>
   *      int case_when_value;
   *      ......code for a......
   *      if (!a_isNull && a_value) {
   *          ......code for b......
   *          case_when_value = res(b_isNull, b_value);
   *      } else {
   *          ......code for c......
   *          if (!c_isNull && c_value) {
   *              ......code for d......
   *              case_when_value = res(d_isNull, d_value);
   *          } else {
   *              ......code for e......
   *              case_when_value = res(e_isNull, e_value);
   *          }
   *      }
   * </pre></blockquote>
   */
  private void implementRecursively(final RexToLixTranslator currentTranslator,
      final List<RexNode> operandList, final ParameterExpression valueVariable, int pos) {
    final BlockBuilder currentBlockBuilder = currentTranslator.getBlockBuilder();
    final List<Type> storageTypes = EnumUtils.internalTypes(operandList);
    // [ELSE] clause
    if (pos == operandList.size() - 1) {
      Expression res = implementCallOperand2(operandList.get(pos),
          storageTypes.get(pos), currentTranslator);
      currentBlockBuilder.add(
          Expressions.statement(
              Expressions.assign(valueVariable,
                  EnumUtils.convert(res, valueVariable.getType()))));
      return;
    }
    // Condition code: !a_isNull && a_value
    final RexNode testerNode = operandList.get(pos);
    final Result testerResult = implementCallOperand(testerNode,
        storageTypes.get(pos), currentTranslator);
    final Expression tester = Expressions.andAlso(
        Expressions.not(testerResult.isNullVariable),
        testerResult.valueVariable);
    // Code for {if} branch
    final RexNode ifTrueNode = operandList.get(pos + 1);
    final BlockBuilder ifTrueBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifTrueTranslator =
        currentTranslator.setBlock(ifTrueBlockBuilder);
    final Expression ifTrueRes = implementCallOperand2(ifTrueNode,
        storageTypes.get(pos + 1), ifTrueTranslator);
    // Assign the value: case_when_value = ifTrueRes
    ifTrueBlockBuilder.add(
        Expressions.statement(
            Expressions.assign(valueVariable,
                EnumUtils.convert(ifTrueRes, valueVariable.getType()))));
    final BlockStatement ifTrue = ifTrueBlockBuilder.toBlock();
    // There is no [ELSE] clause
    if (pos + 1 == operandList.size() - 1) {
      currentBlockBuilder.add(
          Expressions.ifThen(tester, ifTrue));
      return;
    }
    // Generate code for {else} branch recursively
    final BlockBuilder ifFalseBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifFalseTranslator =
        currentTranslator.setBlock(ifFalseBlockBuilder);
    implementRecursively(ifFalseTranslator, operandList, valueVariable, pos + 2);
    final BlockStatement ifFalse = ifFalseBlockBuilder.toBlock();
    currentBlockBuilder.add(
        Expressions.ifThenElse(tester, ifTrue, ifFalse));
  }

  private Result toInnerStorageType(final Result result, final Type storageType) {
    final Expression valueExpression =
        EnumUtils.toInternal(result.valueVariable, storageType);
    if (valueExpression.equals(result.valueVariable)) {
      return result;
    }
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(),
            list.newName(result.valueVariable.name + "_inner_type"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable = result.isNullVariable;
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitDynamicParam(RexDynamicParam dynamicParam) {
    final Pair<RexNode, Type> key = Pair.of(dynamicParam, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final Type storageType = currentStorageType != null
        ? currentStorageType : typeFactory.getJavaClass(dynamicParam.getType());
    final Expression valueExpression = EnumUtils.convert(
        Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
            Expressions.constant("?" + dynamicParam.getIndex())),
        storageType);
    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(), list.newName("value_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, checkNull(valueVariable)));
    final Result result = new Result(isNullVariable, valueVariable);
    rexWithStorageTypeResultMap.put(key, result);
    return result;
  }

  @Override public Result visitFieldAccess(RexFieldAccess fieldAccess) {
    final Pair<RexNode, Type> key = Pair.of(fieldAccess, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final RexNode target = deref(fieldAccess.getReferenceExpr());
    int fieldIndex = fieldAccess.getField().getIndex();
    String fieldName = fieldAccess.getField().getName();
    switch (target.getKind()) {
    case CORREL_VARIABLE:
      if (correlates == null) {
        throw new RuntimeException("Cannot translate " + fieldAccess
            + " since correlate variables resolver is not defined");
      }
      final RexToLixTranslator.InputGetter getter =
          correlates.apply(((RexCorrelVariable) target).getName());
      final Expression input = getter.field(
          list, fieldIndex, currentStorageType);
      final Expression condition = checkNull(input);
      final ParameterExpression valueVariable =
          Expressions.parameter(input.getType(), list.newName("corInp_value"));
      list.add(Expressions.declare(Modifier.FINAL, valueVariable, input));
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE, list.newName("corInp_isNull"));
      final Expression isNullExpression = Expressions.condition(
          condition,
          RexImpTable.TRUE_EXPR,
          checkNull(valueVariable));
      list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      final Result result1 = new Result(isNullVariable, valueVariable);
      rexWithStorageTypeResultMap.put(key, result1);
      return result1;
    default:
      RexNode rxIndex =
          builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
      RexNode rxName =
          builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
      RexCall accessCall = (RexCall) builder.makeCall(
          fieldAccess.getType(), SqlStdOperatorTable.STRUCT_ACCESS,
          ImmutableList.of(target, rxIndex, rxName));
      final Result result2 = accessCall.accept(this);
      rexWithStorageTypeResultMap.put(key, result2);
      return result2;
    }
  }

  @Override public Result visitOver(RexOver over) {
    throw new RuntimeException("cannot translate expression " + over);
  }

  @Override public Result visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Cannot translate " + correlVariable
        + ". Correlated variables should always be referenced by field access");
  }

  @Override public Result visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("cannot translate expression " + rangeRef);
  }

  @Override public Result visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("cannot translate expression " + subQuery);
  }

  @Override public Result visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  @Override public Result visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return visitInputRef(fieldRef);
  }

  Expression checkNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.FALSE_EXPR;
    }
    return Expressions.equal(expr, RexImpTable.NULL_EXPR);
  }

  Expression checkNotNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.TRUE_EXPR;
    }
    return Expressions.notEqual(expr, RexImpTable.NULL_EXPR);
  }

  BlockBuilder getBlockBuilder() {
    return list;
  }

  Expression getLiteral(Expression literalVariable) {
    return literalMap.get(literalVariable);
  }

  /** Returns the value of a literal. */
  Object getLiteralValue(Expression expr) {
    if (expr instanceof ParameterExpression) {
      final Expression constantExpr = literalMap.get(expr);
      return getLiteralValue(constantExpr);
    }
    if (expr instanceof ConstantExpression) {
      return ((ConstantExpression) expr).value;
    }
    return null;
  }

  List<Result> getCallOperandResult(RexCall call) {
    return callOperandResultMap.get(call);
  }

  /** Translates a field of an input to an expression. */
  public interface InputGetter {
    Expression field(BlockBuilder list, int index, Type storageType);
  }

  /** Implementation of {@link InputGetter} that calls
   * {@link PhysType#fieldReference}. */
  public static class InputGetterImpl implements InputGetter {
    private List<Pair<Expression, PhysType>> inputs;

    public InputGetterImpl(List<Pair<Expression, PhysType>> inputs) {
      this.inputs = inputs;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      int offset = 0;
      for (Pair<Expression, PhysType> input : inputs) {
        final PhysType physType = input.right;
        int fieldCount = physType.getRowType().getFieldCount();
        if (index >= offset + fieldCount) {
          offset += fieldCount;
          continue;
        }
        final Expression left = list.append("current", input.left);
        return physType.fieldReference(left, index - offset, storageType);
      }
      throw new IllegalArgumentException("Unable to find field #" + index);
    }
  }

  /** Result of translating a {@code RexNode}. */
  public static class Result {
    final ParameterExpression isNullVariable;
    final ParameterExpression valueVariable;

    public Result(ParameterExpression isNullVariable,
        ParameterExpression valueVariable) {
      this.isNullVariable = isNullVariable;
      this.valueVariable = valueVariable;
    }
  }
}
