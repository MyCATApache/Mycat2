package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlannotations.SQLAnnotationList;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotationKeyRoute;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.SQLType;

import java.util.List;

/**
 * Created by jamie on 2017/9/19.
 */
public interface DynamicAnnotationManager {


    public Runnable process(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception;

    /**
     * 动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
     *
     * @param
     * @return
     */
    //public void processNow(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception ;
    public default Runnable process(String schema, SQLType sqltype, int[] tables, BufferSQLContext context) throws Exception {

        return process(schema.hashCode(), sqltype.getValue(), tables, context);
    }

    public default Runnable process(String schema, SQLType sqltype, String[] tables, BufferSQLContext context) throws Exception {
        return process(schema.hashCode(), sqltype.getValue(), DynamicAnnotationKeyRoute.stringArray2HashArray(tables), context);
    }

    public void collectInSQLAnnotationList(int schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotationList> collect)throws Exception;
    public default void collectInSQLAnnotationList(String schema, SQLType sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotationList> collect) throws Exception {
         collectInSQLAnnotationList(schema.hashCode(), sqltype.getValue(), tables, context,collect);
    }
    public void collect(int schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotation> collect)throws Exception;
    public default void collect(String schema, SQLType sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotation> collect) throws Exception {
        collect(schema.hashCode(), sqltype.getValue(), tables, context,collect);
    }


}