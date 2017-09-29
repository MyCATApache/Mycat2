package io.mycat.mycat2.sqlparser;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.TokenizerUtil;

/**
 * Created by yanjunli on 2017/9/26.
 */
public class MYCATSQLParseTest extends TestCase {

    //    SQLParser parser;
    BufferSQLParser parser;
    BufferSQLContext context;
    private static final Logger LOGGER = LoggerFactory.getLogger(MYCATSQLParseTest.class);
    
    @Before
    protected void setUp() throws Exception {
        parser = new BufferSQLParser();
        context = new BufferSQLContext();
        //parser.init();
        MatchMethodGenerator.initShrinkCharTbl();
    }

    /**
     * mycat switch repl xxx xxx;
     */
    @Test
    public void testGrantPrivType2() throws Exception {
    	MatchMethodGenerator.initShrinkCharTbl();
        String t = " mycat switch repl test1 1;";
        test(t);
    }

    
    private void test(String t) {
        LOGGER.info(t);
        byte[] bytes = t.getBytes();
        parser.parse(bytes, context);
    }


}
