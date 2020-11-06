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
package io.mycat.calcite.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Implementation of {@link org.apache.calcite.schema.Schema} that exposes the
 * public fields and methods in a Java object.
 *
 * @author chenjunwen
 * 2020-5-7 support dynamic update data
 */
public class MycatReflectiveSchema
    extends AbstractSchema {
  private final Class clazz;
  private Object target;
  private Map<String, Table> tableMap;

  /**
   * Creates a ReflectiveSchema.
   *
   * @param target Object whose fields will be sub-objects of the schema
   */
  public MycatReflectiveSchema(Object target) {
    super();
    this.clazz = target.getClass();
    this.target = target;
  }

  @Override public String toString() {
    return "ReflectiveSchema(target=" + target + ")";
  }

  /** Returns the wrapped object.
   *
   * <p>May not appear to be used, but is used in generated code via
   * {@link org.apache.calcite.util.BuiltInMethod#REFLECTIVE_SCHEMA_GET_TARGET}.
   */
  public Object getTarget() {
    return target;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (Field field : clazz.getFields()) {
      final String fieldName = field.getName();
      final Table table = fieldRelation(field);
      if (table == null) {
        continue;
      }
      builder.put(fieldName, table);
    }
    Map<String, Table> tableMap = builder.build();
    // Unique-Key - Foreign-Key
    for (Field field : clazz.getFields()) {
      if (RelReferentialConstraint.class.isAssignableFrom(field.getType())) {
        RelReferentialConstraint rc;
        try {
          rc = (RelReferentialConstraint) field.get(target);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(
              "Error while accessing field " + field, e);
        }
        FieldTable table =
            (FieldTable) tableMap.get(Util.last(rc.getSourceQualifiedName()));
        assert table != null;
        table.statistic = Statistics.of(
            ImmutableList.copyOf(
                Iterables.concat(
                    table.getStatistic().getReferentialConstraints(),
                    Collections.singleton(rc))));
      }
    }
    return tableMap;
  }


  /** Returns an expression for the object wrapped by this schema (not the
   * schema itself). */
  Expression getTargetExpression(SchemaPlus parentSchema, String name) {
    return EnumUtils.convert(
        Expressions.call(
            Schemas.unwrap(
                getExpression(parentSchema, name),
                MycatReflectiveSchema.class),
            BuiltInMethod.REFLECTIVE_SCHEMA_GET_TARGET.method),
        target.getClass());
  }

  /** Returns a table based on a particular field of this schema. If the
   * field is not of the right type to be a relation, returns null. */
  private <T> Table fieldRelation(final Field field) {
    final Type elementType = getElementType(field.getType());
    if (elementType == null) {
      return null;
    }

    return new FieldTable(field, elementType, (Supplier<Enumerable<? extends Object>>) () -> {
        Object o;
        try {
            o = field.get(target);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(
                    "Error while accessing field " + field, e);
        }
        @SuppressWarnings("unchecked")
        final Enumerable<T> enumerable = toEnumerable(o);
        return enumerable;
    });
  }

  /** Deduces the element type of a collection;
   * same logic as {@link #toEnumerable} */
  private static Type getElementType(Class clazz) {
    if (clazz.isArray()) {
      return clazz.getComponentType();
    }
    if (Iterable.class.isAssignableFrom(clazz)) {
      return Object.class;
    }
    return null; // not a collection/array/iterable
  }

  private static Enumerable toEnumerable(final Object o) {
    if (o.getClass().isArray()) {
      if (o instanceof Object[]) {
        return Linq4j.asEnumerable((Object[]) o);
      } else {
        return Linq4j.asEnumerable(Primitive.asList(o));
      }
    }
    if (o instanceof Iterable) {
      return Linq4j.asEnumerable((Iterable) o);
    }
    throw new RuntimeException(
        "Cannot convert " + o.getClass() + " into a Enumerable");
  }

  /** Table that is implemented by reading from a Java object. */
  private static class ReflectiveTable
      extends AbstractQueryableTable
      implements Table, ScannableTable {
    private final Type elementType;
    private final Supplier< Enumerable>  enumerable;

    ReflectiveTable(Type elementType,Supplier< Enumerable>  enumerable) {
      super(elementType);
      this.elementType = elementType;
      this.enumerable = enumerable;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return ((JavaTypeFactory) typeFactory).createType(elementType);
    }

    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    public Enumerable<Object[]> scan(DataContext root) {
        Enumerable enumerable = this.enumerable.get();
        if (elementType == Object[].class) {
        //noinspection unchecked
        return enumerable;
      } else {
        //noinspection unchecked
        return enumerable.select(new FieldSelector((Class) elementType));
      }
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        @SuppressWarnings("unchecked")
        public Enumerator<T> enumerator() {
          return (Enumerator<T>) enumerable.get();
        }
      };
    }
  }


  /** Table based on a Java field.
   *
   * @param <T> element type */
  private static class FieldTable<T> extends ReflectiveTable {
    private final Field field;
    private Statistic statistic;

    FieldTable(Field field, Type elementType, Supplier< Enumerable> enumerable) {
      this(field, elementType, enumerable, Statistics.UNKNOWN);
    }

    FieldTable(Field field, Type elementType, Supplier< Enumerable>  enumerable,
        Statistic statistic) {
      super(elementType, enumerable);
      this.field = field;
      this.statistic = statistic;
    }

    public String toString() {
      return "Relation {field=" + field.getName() + "}";
    }

    @Override public Statistic getStatistic() {
      return statistic;
    }

    @Override public Expression getExpression(SchemaPlus schema,
        String tableName, Class clazz) {
      return Expressions.field(
          schema.unwrap(MycatReflectiveSchema.class).getTargetExpression(
              schema.getParentSchema(), schema.getName()), field);
    }
  }

  /** Function that returns an array of a given object's field values. */
  private static class FieldSelector implements Function1<Object, Object[]> {
    private final Field[] fields;

    FieldSelector(Class elementType) {
      this.fields = elementType.getFields();
    }

    public Object[] apply(Object o) {
      try {
        final Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
          objects[i] = fields[i].get(o);
        }
        return objects;
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
