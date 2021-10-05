/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.record.impl;

import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.constant.IntConstant;
import io.ordinate.engine.function.constant.LongConstant;
import io.ordinate.engine.record.*;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.schema.IntInnerType;
import lombok.SneakyThrows;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.linq4j.tree.*;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class CodeGenRecordSinkFactoryImpl implements RecordSinkFactory {
    public static final Map<String, Method> GET_METHODS = Arrays.stream(Record.class.getDeclaredMethods()).collect(Collectors.toMap(k -> k.getName(), v -> v));
    public static final Map<String, Method> SET_METHODS = Arrays.stream(RecordSetter.class.getDeclaredMethods()).collect(Collectors.toMap(k -> k.getName(), v -> v));

    public static final ParameterExpression from = Expressions.parameter(Record.class, "from");
    public static final ParameterExpression to = Expressions.parameter(RecordSetter.class, "to");
    public static final ParameterExpression rowId = Expressions.parameter(int.class, "rowId");
    public static final ParameterExpression input = Expressions.parameter(VectorSchemaRoot.class, "input");
    public static final ParameterExpression functionsExpression = Expressions.parameter(Function[].class, "functions");
    public final ICompilerFactory compilerFactory;
    public CodeGenRecordSinkFactoryImpl() {
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
        }
    }

    @Override
    @SneakyThrows
    public RecordSink buildRecordSink(IntInnerType[] types) {
        StringBuilder sb = new StringBuilder();
        for (IntInnerType type : types) {
            sb.append(type.type.getAlias());
        }
        sb.append(RecordSink.class.getSimpleName());
        String className = sb.toString();
        ClassDeclaration classDeclaration = Expressions.classDecl(java.lang.reflect.Modifier.PUBLIC, className, null, Arrays.asList(RecordSink.class),
                Arrays.asList(generateCopyRecordMethod(types), generateCopyRecordToVectorBatchMethod(types)));
        String s1 = Expressions.toString(classDeclaration);
        Class cookClass = CodeGenerator.cookClass(RecordSink.class.getClassLoader(), className, s1);
        return (RecordSink)cookClass.newInstance();
    }

    @Override
    @SneakyThrows
    public FunctionSink buildFunctionSink(Function[] functions) {
        StringBuilder sb = new StringBuilder();
        for (Function function : functions) {
            sb.append(function.getType().getAlias());
        }
        sb.append(FunctionSink.class.getSimpleName());
        String className = sb.toString();
        MethodDeclaration methodDeclaration = generateFunctionToVectorBatchMethod(functions);
        ClassDeclaration classDeclaration = Expressions.classDecl(java.lang.reflect.Modifier.PUBLIC, className,null, Collections.singletonList(FunctionSink.class),
                Arrays.asList(methodDeclaration));
        String s1 = Expressions.toString(classDeclaration);
        Class cookClass = CodeGenerator.cookClass(RecordSink.class.getClassLoader(), className, s1);
        return (FunctionSink)cookClass.newInstance();
    }

    @Override
    public RecordSetter getRecordSinkSPI(Object mapKey) {
        return RecordSinkFactoryImpl.INSTANCE.getRecordSinkSPI(mapKey);
    }

    @Override
    public RecordComparator buildEqualComparator(InnerType[] types) {
        return RecordSinkFactoryImpl.INSTANCE.buildEqualComparator(types);
    }

    @SneakyThrows
    public static void main(String[] args) {
/**
 *
 *     void copy(Record r, RecordSetter w);
 *
 *     void copy(Record record, int rowId, VectorSchemaRoot input);
 *
 *     void copy(Function[] functions, Record inputRecord, int rowId, VectorSchemaRoot input);
 *
 */
        IntInnerType[] types = new IntInnerType[]{IntInnerType.of(0, InnerType.INT32_TYPE), IntInnerType.of(1, InnerType.INT64_TYPE)};
        Function[] functions = new Function[]{IntConstant.newInstance(1), LongConstant.newInstance(1)};
        //generateCopyRecordMethod


        MethodDeclaration copyToSetter = generateCopyRecordMethod(types);

        System.out.println(copyToSetter);

        MethodDeclaration copyToVector = generateCopyRecordToVectorBatchMethod(types);
        System.out.println(copyToVector);
        //    void copy(Function[] functions, Record inputRecord, int rowId, VectorSchemaRoot input);

        MethodDeclaration funcToVector = generateFunctionToVectorBatchMethod(functions);

        CodeGenRecordSinkFactoryImpl byteBuddyRecordSinkFactory = new CodeGenRecordSinkFactoryImpl();
        RecordSink recordSink = byteBuddyRecordSinkFactory.buildRecordSink(types);
        FunctionSink functionSink = byteBuddyRecordSinkFactory.buildFunctionSink(new Function[]{IntConstant.newInstance(1), LongConstant.newInstance(2)});
        System.out.println(funcToVector);
    }

    public static MethodDeclaration generateFunctionToVectorBatchMethod(Function[] functions) {
        // public void copy(Function[] functions, Record inputRecord, int rowId, VectorSchemaRoot output)
        return Expressions.methodDecl(java.lang.reflect.Modifier.PUBLIC, void.class, "copy",
                Arrays.asList(functionsExpression, from, rowId, input), generateFunctionToVectorBatchBody(functions));
    }

    public static BlockStatement generateFunctionToVectorBatchBody(Function[] functions) {
        int size = functions.length;
        BlockBuilder statements = new BlockBuilder();


        for (int varIndex = 0; varIndex < functions.length; varIndex++) {
            Function function = functions[varIndex];
            InnerType type = function.getType();
            ParameterExpression functionVariable = Expressions.parameter(Function.class);
            ConstantExpression arrayIndex = Expressions.constant(varIndex);
            statements.add(Expressions.declare(0, functionVariable,
                    Expressions.convert_(Expressions.arrayIndex(functionsExpression, arrayIndex), Function.class)
            ));

            ParameterExpression vectorVariable = Expressions.parameter(type.getFieldVector());
            ParameterExpression returnValue = Expressions.parameter(function.getType().getJavaClass());
            statements.add(Expressions.declare(0, returnValue, Expressions.call(functionVariable, "get" + type.name(), from)));
            ParameterExpression isNull = Expressions.parameter(boolean.class);
            statements.add(Expressions.declare(0, isNull, Expressions.call(functionVariable, "isNull", from)));
            statements.add(Expressions.declare(0, vectorVariable, Expressions.convert_(
                    Expressions.call(input, "getVector", arrayIndex), type.getFieldVector())));
            MethodCallExpression ifTrue = Expressions.call(RecordSinkFactoryImpl.class, "set" + type.getFieldVector().getSimpleName() + "Null", vectorVariable, rowId);
            MethodCallExpression ifFalse = Expressions.call(RecordSinkFactoryImpl.class, "set" + type.getFieldVector().getSimpleName(), vectorVariable, rowId,
                    returnValue);
            statements.add(Expressions.ifThenElse(isNull,
                    Expressions.statement(ifTrue)
                    ,
                    Expressions.statement(ifFalse)
            ));

        }
        return statements.toBlock();
    }

    private static MethodDeclaration generateCopyRecordToVectorBatchMethod(IntInnerType[] types) {
        return Expressions.methodDecl(java.lang.reflect.Modifier.PUBLIC, void.class, "copy",
                Arrays.asList(from, rowId, input), generateCopyRecordToVectorBatchBody(types));
    }

    private static BlockStatement generateCopyRecordToVectorBatchBody(IntInnerType[] types) {
        int size = types.length;
        ArrayList<Statement> statements = new ArrayList<>(size);
        for (int varIndex = 0; varIndex < size; varIndex++) {
            IntInnerType intPair = types[varIndex];
            int columnIndex = intPair.index;
            InnerType type = intPair.type;
            ConstantExpression index = Expressions.constant(columnIndex);
            ParameterExpression vectorVariable = Expressions.parameter(type.getFieldVector());

            statements.add(Expressions.declare(0, vectorVariable, Expressions.convert_(
                    Expressions.call(input, "getVector", index), type.getFieldVector())));
            ParameterExpression returnVariable = Expressions.parameter(type.getJavaClass());
            statements.add(Expressions.declare(0, returnVariable, Expressions.call(from, "get" + type.name(), index)));
            MethodCallExpression isNullCondition = Expressions.call(from, "isNull", index);
            statements.add(Expressions.ifThenElse(isNullCondition,
                    Expressions.statement(Expressions.call(RecordSinkFactoryImpl.class, "set" + type.getFieldVector().getSimpleName() + "Null", vectorVariable, index)),
                    Expressions.statement(Expressions.call(RecordSinkFactoryImpl.class, "set" + type.getFieldVector().getSimpleName(), vectorVariable, rowId, returnVariable
                            )
                    )));

        }
        return Expressions.block(statements);
    }

    public static MethodDeclaration generateCopyRecordMethod(IntInnerType[] types) {
        // void copy(Record r, RecordSetter w);
        return Expressions.methodDecl(java.lang.reflect.Modifier.PUBLIC, void.class, "copy", Arrays.asList(from, to), generateCopyRecordBody(types));
    }

    public static BlockStatement generateCopyRecordBody(IntInnerType[] types) {
        ArrayList<Statement> setterStatements = new ArrayList<>(types.length);
        for (int varIndex = 0; varIndex < types.length; varIndex++) {
            IntInnerType intPair = types[varIndex];
            int columnIndex = intPair.index;
            InnerType type = intPair.type;
            ConstantExpression index = Expressions.constant(columnIndex);
            MethodCallExpression isNullCondition = Expressions.call(from, "isNull", index);

            setterStatements.add(Expressions.ifThenElse(isNullCondition,
                    Expressions.statement(Expressions.call(RecordSinkFactoryImpl.class, "copyNullType", from, to, index)),
                    Expressions.statement(Expressions.call(RecordSinkFactoryImpl.class, "copy" + type.name(), from, to, index))));

        }
        return Expressions.block(setterStatements);
    }
}
