package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;

import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;

/**
 * Created by jamie on 2017/9/13.
 */
public interface DynamicAnnotationMatch extends Cloneable {
    public void pick(int i, final int arrayCount, BufferSQLContext context,HashArray array);

    //boolean isComplete();
    int[] getCompleteTags();
    String getName();
    boolean isComplete();
    default void pick(int i, BufferSQLContext context) {
        HashArray array = context.getHashArray();
        pick(i, array.getCount(), context, array);
    }

    DynamicAnnotationMatch newInstance() throws Exception;
}
