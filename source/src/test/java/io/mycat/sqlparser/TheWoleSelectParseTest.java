package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.SelectStatementParser;
import io.mycat.mycat2.sqlparser.byteArrayInterface.expr.ExprSQLParser;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jamie on 2017/9/8.
 */
public class TheWoleSelectParseTest  extends TestCase {

    @Test
    public void testExpr() throws Exception {
       String t = "select a.id,a.price,b.username where id in(111,222,333,444) and id2=id3;";
        test(t);
    }
    //    SQLParser parser;
    BufferSQLParser parser;
    BufferSQLContext context;
    private static final Logger LOGGER = LoggerFactory.getLogger(io.mycat.mycat2.sqlparser.TCLSQLParserTest.class);

    private void test(String t) {
        LOGGER.info(t);
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
        SelectStatementParser.pickSelectStatement(0,context.getHashArray().getCount(),context,context.getHashArray(),context.getBuffer());
    }

    @Before
    protected void setUp() throws Exception {
        parser = new BufferSQLParser();
        context = new BufferSQLContext();
        //parser.init();
        MatchMethodGenerator.initShrinkCharTbl();
    }
}
