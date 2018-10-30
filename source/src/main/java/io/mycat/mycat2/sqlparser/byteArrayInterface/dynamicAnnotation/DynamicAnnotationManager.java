package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlannotations.SQLAnnotationList;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotationKeyRoute;

import java.util.List;

/**
 * Created by jamie on 2017/9/19.
 */
public interface DynamicAnnotationManager {


    Runnable process(int schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception;

    /**
     * 动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
     *
     * @param
     * @return
     */
    //public void processNow(int mycatSchema, int sqltype, int[] tables, BufferSQLContext context) throws Exception ;
    default Runnable process(String schema, int sqltype, int[] tables, BufferSQLContext context) throws Exception {

        return process(schema.hashCode(), sqltype, tables, context);
    }

    default Runnable process(String schema, int sqltype, String[] tables, BufferSQLContext context) throws Exception {
        return process(schema.hashCode(), sqltype, DynamicAnnotationKeyRoute.stringArray2HashArray(tables), context);
    }

    void collectInSQLAnnotationList(int schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotationList> collect) throws Exception;

    default void collectInSQLAnnotationList(String schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotationList> collect) throws Exception {
         collectInSQLAnnotationList(schema.hashCode(), sqltype, tables, context,collect);
    }

    void collect(int schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotation> collect) throws Exception;

    default void collect(String schema, int sqltype, int[] tables, BufferSQLContext context, List<SQLAnnotation> collect) throws Exception {
        collect(schema.hashCode(), sqltype, tables, context,collect);
    }


}