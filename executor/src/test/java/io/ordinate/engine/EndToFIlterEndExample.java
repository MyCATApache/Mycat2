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
package io.ordinate.engine;

import io.ordinate.engine.builder.CalciteCompiler;
import io.ordinate.engine.physicalplan.*;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.core.Observable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.junit.Test;

import java.util.*;

/**
 * An end to end example from an SQL query to a plan in Enumerable convention.
 */
public class EndToFIlterEndExample {

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
                "SELECT b.id, b.title, b.\"year\" "
                        + "FROM Book b\n"
                        + " where b.id = 1"
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
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

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
                SqlToRelConverter.config());

        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;
        CalciteSchema.TableEntry table = schema.getSubSchema("bs", false).getTable("book", false);
        CalciteCompiler calciteCompiler = new CalciteCompiler();


        List<Object[]> objects = Arrays.asList(new Object[]{1, "Les Miserables", 1862, "1"}, new Object[]{2, "The Hunchback of Notre-Dame", 1829, "1"});
        calciteCompiler.registerTable("bs.book",objects);
        CalciteCompiler convert = calciteCompiler.convert(logPlan);
        PhysicalPlan build = calciteCompiler.build();
        OutputLinq4jPhysicalPlan outputLinq4jPhysicalPlan = new OutputLinq4jPhysicalPlan(build);
 outputLinq4jPhysicalPlan.executeToObject(new RootContext()).blockingIterable().forEach(r-> {
     System.out.println(Arrays.toString(r));
 });
        // Display the logical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));

        // Initialize optimizer/planner with the necessary rules
        RelOptPlanner planner = cluster.getPlanner();

        Bindables.RULES.forEach(r -> planner.addRule(r));
        // Define the type of the output plan (in this case we want a physical plan in
        // EnumerableContention)
        logPlan = planner.changeTraits(logPlan,
                cluster.traitSet().replace(BindableConvention.INSTANCE));
        planner.setRoot(logPlan);
        // Start the optimization process to obtain the most efficient physical plan based on the
        // provided rule set.
        RelNode phyPlan = planner.findBestExp();


        //Predicates
        System.out.println("pulledUpPredicates");
        System.out.println(phyPlan.getCluster().getMetadataQuery().getPulledUpPredicates(phyPlan));


        System.out.println("allPredicates");
        System.out.println(phyPlan.getCluster().getMetadataQuery().getAllPredicates(phyPlan));

        // Display the physical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));

    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = new RelOptTable.ViewExpander() {
        @Override
        public RelRoot expandView(final RelDataType rowType, final String queryString,
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

        @Override
        public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return null;
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(final String name) {
            return null;
        }
    }
}

// End EndToEndExampleEnumerable.java