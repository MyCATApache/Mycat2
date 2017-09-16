package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;

/**
 * Created by jamie on 2017/9/13.
 */
public interface DynamicAnnotationMatch {
    public void pick(int i, final int arrayCount, BufferSQLContext context, HashArray array, ByteArrayInterface sql);

    boolean isComplete();
    boolean[] getCompleteTags();
    String getName();
}
