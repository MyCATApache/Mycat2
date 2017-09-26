package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationManagerImpl;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotationKeyRoute;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotationRuntime;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotationUtil;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.SQLType;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.java2d.pipe.BufferedContext;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jamie on 2017/9/19.
 */
public class DynamicAnnotationManagerTest extends TestCase {


    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicAnnotationManagerTest.class);

    /**
     * 不匹配的例子,没有运行action
     * @throws Exception
     */
    @Test
    public void test1() throws Exception {
        test("schemA", SQLType.INSERT, new String[]{"x1"},"SELECT * FROM Person BETWEEN 1 AND 100");
        //没有运行action
    }
    /**
     * 匹配的例子,运行action,检查and
     * @throws Exception
     */
    @Test
    public void test2() throws Exception {
        test("schemA", SQLType.INSERT, new String[]{"x1"},"SELECT * FROM Person WHERE id BETWEEN 1 AND 100 and name = \"haha\" and a=1 ");
        //运行action
    }
    /**
     * 匹配的例子,运行action,检查or
     * @throws Exception
     */
    @Test
    public void test3() throws Exception {
        test("schemA", SQLType.INSERT, new String[]{"x1"},"SELECT * FROM Person WHERE name2 = \"ha\" ");
        //运行action
    }
    /**
     * 匹配的例子,运行action,检查多个tables
     * @throws Exception
     */
    @Test
    public void test4() throws Exception {
        test("schemA", SQLType.INSERT, new String[]{"x2"},"SELECT * FROM Person WHERE name2 = \"ha\" ");
        //运行action
    }
    /**
     * 匹配的例子,运行action,检查多个tables,这里只匹配第二个
     * @throws Exception
     */
    @Test
    public void test5() throws Exception {
        test("schemA", SQLType.INSERT, new String[]{"x3"},"SELECT * FROM Person WHERE name2 = \"ha\" ");
        //运行action
    }
    /**
     * 测试不写where的情况
     * @throws Exception
     */
    @Test
    public void test6() throws Exception {
        test("schemA", SQLType.INSERT, new String[]{"x3"},"SELECT * FROM Person WHERE name2 = \"ha\" ");
        //运行action
    }


    private void test(String schema, SQLType type, String[] tables,String sql) throws Exception {
        sqlParser.parse(sql.getBytes(),context);
        int[] intsTables = DynamicAnnotationKeyRoute.stringArray2HashArray(tables);
        int schemaHash = schema.hashCode();
        int sqlType = type.ordinal();
        manager.process(schemaHash, sqlType, intsTables, context).run();
    }

    DynamicAnnotationManagerImpl manager;
    BufferSQLContext context;
    BufferSQLParser sqlParser;


    @Before
    protected void setUp() throws Exception {
        manager = new DynamicAnnotationManagerImpl("actions_bak.yaml", "annotations_bak.yaml");
        context = new BufferSQLContext();
        sqlParser = new BufferSQLParser();
    }
}