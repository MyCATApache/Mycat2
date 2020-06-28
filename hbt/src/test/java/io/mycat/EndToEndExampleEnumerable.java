  
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
package io.mycat;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * An end to end example from an SQL query to a plan in Enumerable convention.
 */
public class EndToEndExampleEnumerable {

  /***/
  public static class Author {
    public final int id;
    public final String fname;
    public final String lname;

    public Author(final int id, final String firstname, final String lastname) {
      this.id = id;
      this.fname = firstname;
      this.lname = lastname;
    }
  }

  /***/
  public static class Book {
    public final int id;
    public final String title;
    public final int year;
    public final Author author;

    public Book(final int id, final String title, final int year, final Author author) {
      this.id = id;
      this.title = title;
      this.year = year;
      this.author = author;
    }
  }

  /***/
  public static class BookStore {
    public final Author[] author = new Author[]{
        new Author(1, "Victor", "Hugo"),
        new Author(2, "Alexandre", "Dumas")
    };
    public final Book[] book = new Book[]{
        new Book(1, "Les Miserables", 1862, author[0]),
        new Book(2, "The Hunchback of Notre-Dame", 1829, author[0]),
        new Book(3, "The Last Day of a Condemned Man", 1829, author[0]),
        new Book(4, "The three Musketeers", 1844, author[1]),
        new Book(5, "The Count of Monte Cristo", 1884, author[1])
    };
  }

  @Test
  public void example() throws Exception {
    CalciteSchema schema = CalciteSchema.createRootSchema(true);
    schema.add("bs", new ReflectiveSchema(new BookStore()));

    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Create an SQL parser
    SqlParser parser = SqlParser.create(
        "SELECT b.id, b.title, b.\"year\", a.fname || ' ' || a.lname \n"
            + "FROM Book b\n"
            + "LEFT OUTER JOIN Author a ON b.author.id=a.id\n"
            + "ORDER BY b.id\n"
            + "LIMIT 5");
    // Parse the query into an AST
    SqlNode sqlNode = parser.parseQuery();

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList("bs"),
        typeFactory, config);

    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory);

    // Validate the initial AST
    SqlNode validNode = validator.validate(sqlNode);

    // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.Config.DEFAULT);

    // Convert the valid AST into a logical plan
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

    SchemaOnlyDataContext schemaOnlyDataContext = new SchemaOnlyDataContext(schema);
    Interpreter interpreter = new Interpreter(schemaOnlyDataContext, logPlan);
;
    for (Object[] row :     interpreter) {
      System.out.println(Arrays.toString(row));
    }
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = new RelOptTable.ViewExpander() {
    @Override public RelRoot expandView(final RelDataType rowType, final String queryString,
        final List<String> schemaPath,
        final List<String> viewPath) {
      return null;
    }
  };

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return  new JavaTypeFactoryImpl(){
        @Override
        public Charset getDefaultCharset() {
          return StandardCharsets.UTF_8;
        }
      };
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}

// End EndToEndExampleEnumerable.java