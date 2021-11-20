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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class MycatRelDataTypeFactory implements JavaTypeFactory {
    final RelDataTypeSystem relDataTypeSystem;
    final JavaTypeFactoryImpl javaTypeFactory;

    public MycatRelDataTypeFactory(RelDataTypeSystem relDataTypeSystem) {
        this.relDataTypeSystem = relDataTypeSystem;
        this.javaTypeFactory = new JavaTypeFactoryImpl(relDataTypeSystem) {
            @Override
            public Charset getDefaultCharset() {
                return StandardCharsets.UTF_8;
            }

            @Override
            public Type getJavaClass(RelDataType type) {
                if (type.getSqlTypeName()== SqlTypeName.OTHER){
                 return String.class;
                }
                return super.getJavaClass(type);
            }
        };
    }


    @Override
    public RelDataTypeSystem getTypeSystem() {
        return this.javaTypeFactory.getTypeSystem();
    }

    @Override
    public RelDataType createJavaType(Class aClass) {
        return this.javaTypeFactory.createJavaType(aClass);
    }

    @Override
    public RelDataType createJoinType(RelDataType... relDataTypes) {
        return this.javaTypeFactory.createJoinType(relDataTypes);
    }

    @Override
    public RelDataType createStructType(StructKind structKind, List<RelDataType> list, List<String> list1) {
        return this.javaTypeFactory.createStructType(structKind, list, list1);
    }

    @Override
    public RelDataType createStructType(List<RelDataType> list, List<String> list1) {
        return this.javaTypeFactory.createStructType(list, list1);
    }

    @Override
    public RelDataType createStructType(FieldInfo fieldInfo) {
        return this.javaTypeFactory.createStructType(fieldInfo);
    }

    @Override
    public RelDataType createStructType(List<? extends Map.Entry<String, RelDataType>> list) {
        return this.javaTypeFactory.createStructType(list);
    }

    @Override
    public RelDataType createArrayType(RelDataType relDataType, long l) {
        return this.javaTypeFactory.createArrayType(relDataType,l);
    }

    @Override
    public RelDataType createMapType(RelDataType relDataType, RelDataType relDataType1) {
        return this.javaTypeFactory.createMapType(relDataType,relDataType1);
    }

    @Override
    public RelDataType createMultisetType(RelDataType relDataType, long l) {
        return this.javaTypeFactory.createMultisetType(relDataType,l);
    }

    @Override
    public RelDataType copyType(RelDataType relDataType) {
        return this.javaTypeFactory.copyType(relDataType);
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType relDataType, boolean b) {
        return this.javaTypeFactory.createTypeWithNullability(relDataType,b);
    }

    @Override
    public RelDataType createTypeWithCharsetAndCollation(RelDataType relDataType, Charset charset, SqlCollation sqlCollation) {
        return this.javaTypeFactory.createTypeWithCharsetAndCollation(relDataType,charset,sqlCollation);
    }

    @Override
    public Charset getDefaultCharset() {
        return this.javaTypeFactory.getDefaultCharset();
    }

    @Override
    public RelDataType leastRestrictive(List<RelDataType> list) {

        return this.javaTypeFactory.leastRestrictive(list);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName sqlTypeName) {
        return this.javaTypeFactory.createSqlType(sqlTypeName);
    }

    @Override
    public RelDataType createUnknownType() {
        return this.javaTypeFactory.createUnknownType();
    }

    @Override
    public RelDataType createSqlType(SqlTypeName sqlTypeName, int i) {
        return this.javaTypeFactory.createSqlType(sqlTypeName,i);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName sqlTypeName, int i, int i1) {
        return this.javaTypeFactory.createSqlType(sqlTypeName,i,i1);
    }

    @Override
    public RelDataType createSqlIntervalType(SqlIntervalQualifier sqlIntervalQualifier) {
        return this.javaTypeFactory.createSqlIntervalType(sqlIntervalQualifier);
    }

    @Override
    public RelDataType createDecimalProduct(RelDataType relDataType, RelDataType relDataType1) {
        return this.javaTypeFactory.createDecimalProduct(relDataType,relDataType1);
    }

    @Override
    public boolean useDoubleMultiplication(RelDataType relDataType, RelDataType relDataType1) {
        return this.javaTypeFactory.useDoubleMultiplication(relDataType,relDataType1);
    }

    @Override
    public RelDataType createDecimalQuotient(RelDataType relDataType, RelDataType relDataType1) {
        return this.javaTypeFactory.createDecimalQuotient(relDataType,relDataType1);
    }

    @Override
    public RelDataType decimalOf(RelDataType relDataType) {
        return this.javaTypeFactory.decimalOf(relDataType);
    }

    @Override
    public FieldInfoBuilder builder() {
        return  this.javaTypeFactory.builder();
    }

    @Override
    public RelDataType createStructType(Class clazz) {
        return javaTypeFactory.createStructType(clazz);
    }

    @Override
    public RelDataType createType(Type type) {
        return javaTypeFactory.createType(type);
    }

    @Override
    public Type getJavaClass(RelDataType type) {
        return javaTypeFactory.getJavaClass(type);
    }

    @Override
    public Type createSyntheticType(List<Type> types) {
        return javaTypeFactory.createSyntheticType(types);
    }

    @Override
    public RelDataType toSql(RelDataType type) {
        return javaTypeFactory.toSql(type);
    }
}
