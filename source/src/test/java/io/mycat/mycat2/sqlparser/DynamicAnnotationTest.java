package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationRuntime;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.DynamicAnnotationUtil;
import junit.framework.TestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jamie on 2017/9/5.
 */
public class DynamicAnnotationTest extends TestCase {


    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicAnnotationTest.class);

    /**
     * 测试 基本的?匹配
     *
     * @throws Exception
     */
    @Test
    public void test1() throws Exception {
        test(listOf("b = ? and c = ? and d = ? . ? and c = ?"),
                "b = 1 and c = 1 and d = a.b and c = 1");
    }

    /**
     * 包含的关系匹配
     * @throws Exception
     */
    @Test
    public void test2() throws Exception {
        test(listOf("b = ? and c = ? and d = ? . ? and c = ?","c = ?","d = ? . ?","b = 1"),
                "b = 1 and c = 1 and d = a.b and c = 1");
    }

    /**
     * 含有前后有公共子序列匹配
     * @throws Exception
     */
    @Test
    public void test3() throws Exception {
        test(listOf("b = ? and c = ? and d = ? . ? and e = ?",
                "d = ? . ? and e = ? and f = 1"),
                "b = 1 and c = 1 and d = a.b and e = 1 and f = 1");
    }
    /**
     * 含有前后有公共子序列匹配,前后互补型
     * @throws Exception
     */
    @Test
    public void test4() throws Exception {
        test(listOf("b = ? and c = ? and d = 4 and e = ?",
                "d = ? and e = 1 and f = 1"),
                "b = 1 and c = 1 and d = 4 and e = 1 and f = 1");
    }
    /**
     * 含有前后有公共子序列匹配,前后互补型
     * @throws Exception
     */
    @Test
    public void test5() throws Exception {
        test(listOf("b = ? and c = ? and d = ? and e = 2",
                "d = 4 and e = ? and f = 1"),
                "b = 1 and c = 1 and d = 4 and e = 2 and f = 1");
    }

    /**
     *    *.*匹配 todo 条件解析没做分词,a.b 匹配写成 ?空格.空格 ?
     * @throws Exception
     */
    @Test
    public void test6() throws Exception {
        test(listOf("b = ? and c = ? and d = 4 and e = a . ?",
                "d = ? and e = ? . ccc and f = 1"),
                "b = 1 and c = 1 and d = 4 and e = a.ccc and f = 1");
    }
    /**
     * 含有前后有公共子序列匹配,前后互补型,两个d干扰
     * @throws Exception
     */
    @Test
    public void test7() throws Exception {
        test(listOf("b = ? and c = ? and d = s and d = x",
                "d = s and d = x and f = 1"),
                "b = 1 and c = 1 and d = s and d = x and f = 1");
    }
    @Test
    public void test8() throws Exception {
        test(listOf("b = ? and c = ? and d = s and d = x",
                "d = s and d = x and f = 1","?  = ?"),
                "b = 1 and c = 1 and d = s and d = x and f = 1");
    }

    /**
     * 字符串测试
     * @throws Exception
     */
    @Test
    public void test9() throws Exception {
        test(listOf("b = ? and c = \"haha\" and d = ? and d = x",
                "d = \"ahah\" and d = x and f = 1","?  = ?"),
                "b = 1 and c = \"haha\" and d = \"ahah\" and d = x and f = 1");
    }
    private void test(List<String> conList, String target) throws Exception {
        conList.stream().forEach(LOGGER::info);
        byte[] bytes = target.getBytes(StandardCharsets.UTF_8);
        DynamicAnnotationRuntime runtime;
        BufferSQLParser parser;
        BufferSQLContext context;
        parser = new BufferSQLParser();
        context = new BufferSQLContext();
        parser.parse(bytes, context);
        runtime = DynamicAnnotationUtil.compile(conList);
        runtime.getMatch().pick(0, context.getHashArray().getCount(), context, context.getHashArray(), context.getBuffer());
        runtime.testCallBackInfo(context);
    }

    static List<String> listOf(String... c) {
        return Arrays.asList(c);
    }

}
